package redis

import (
	"fmt"
	"math/rand"
	"regexp"
	"strconv"

	"github.com/dvirsky/go-pylog/logging"
	"github.com/garyburd/redigo/redis"
	"github.com/EverythingMe/meduza/driver"
	"github.com/EverythingMe/meduza/errors"
	"github.com/EverythingMe/meduza/query"
	"github.com/EverythingMe/meduza/schema"
)

// table represents a single table containing entities, with its schema description and indexing
type table struct {
	desc    schema.Table
	indexes []index
	primary primaryIndex
}

func (t *table) String() string {
	return t.desc.Name
}

// addIndex adds a new index to the table based on a schema index descriptor.
// Since in redis secondary indexes are implemented by the driver, this does nothing but mark new entities for
// indexing using this index
func (t *table) AddIndex(desc *schema.Index) error {

	var idx index
	switch desc.Type {
	case schema.SimpleIndex:
		if len(desc.Columns) != 1 {
			return logging.Errorf("Cannot create simple index %s with more than one property", desc.Name)
		}
		idx = NewCompoundIndex(*desc, t.desc.Name)

	case schema.CompoundIndex:
		idx = NewCompoundIndex(*desc, t.desc.Name)
	default:
		return errors.Context(logging.Errorf("Unsupported index type %s", desc.Type))
	}

	t.indexes = append(t.indexes, idx)
	logging.Debug("Added idnex %s (%s) to table %s, it now has %s indexes", desc.Name, desc.Type, t, len(t.indexes))
	return nil
}

// idKey takes the raw object id, and generates the actual redis key used to store the HASH of this object
func (t table) idKey(id schema.Key) string {
	return fmt.Sprintf("%s:%s", t.desc.Name, id)
}

// reindex takes a set of entities, breaks them into indexing commands and makes sure they are written into
// the proper secondary indexes
func (t *table) reindex(entities ...schema.Entity) error {

	cs := newChangeSet(t, len(entities))
	for _, ent := range entities {

		if ent.Id.IsNull() {
			return errors.NewError("Cannot reindex an entity with an empty id")
		}

		// we transform the entity into an entityChange set of changes (sets in this case)
		changes := make([]query.Change, 0, len(ent.Properties))
		for k, p := range ent.Properties {
			changes = append(changes, query.Change{k, p, query.Noop})
		}

		// now we add this change into the change set for the entire batch
		cs.Add(newEntityChange(t, ent.Id, changeReindex, changes...))
	}

	// execute the entire changeset at once
	_, err := cs.Execute()
	if err != nil {
		logging.Error("Could not execute changeset: %s", err)
		return err
	}
	logging.Debug("Redindexed %d entities in table %s", len(entities), t)

	return nil

}

// Put writes an object to the data store. It is called by Add and Set, and the isUpdate
// flag indicates if this is a newly created object or an updated one, that should be re-indexed
// and removed from existing indexes
func (t *table) Put(entities ...schema.Entity) ([]schema.Key, error) {

	ret := make([]schema.Key, len(entities))

	cs := newChangeSet(t, len(entities))
	for i, ent := range entities {

		changeType := changeUpdate

		// We always try to generate the id for the entity, even if it is given.
		// This is because a compound primary key's value might change
		if id, err := t.primary.GenerateId(ent); err != nil {
			return nil, err
		} else if id != ent.Id {
			ent.SetId(id)
			changeType = changeInsert
		}

		ret[i] = ent.Id
		// we transform the entity into an entityChange set of changes (sets in this case)
		changes := make([]query.Change, 0, len(ent.Properties))
		for k, p := range ent.Properties {
			changes = append(changes, query.Set(k, p))
		}

		// the expiry must be the last change
		if ent.TTL > 0 {
			changes = append(changes, query.Expire(ent.TTL))
		}

		// now we add this change into the change set for the entire batch
		cs.Add(newEntityChange(t, ent.Id, changeType, changes...))
	}

	// execute the entire changeset at once
	_, err := cs.Execute()
	if err != nil {
		logging.Error("Could not execute changeset: %s", err)
		return nil, err
	}
	logging.Debug("Put %d entities, ids: %s", len(entities), ret)

	return ret, nil
}

// getIds returns a list of ids for a specific set of query filters. It also returns the total
// number of entities for this selection, or an error if couldn't find the ids by any index.
// limit of -1 means all ids
func (t *table) getIds(filters query.Filters, offset, limit int, order query.Ordering) (ids []schema.Key, total int, err error) {

	if m, _ := t.primary.Matches(filters, order); m {

		if ids, total, err = t.primary.Find(filters, offset, limit, order); err != nil {
			return
		}

		err = nil

		// trim ids if we got a paging request over primary values (this shouldn't happen but still...)
		if len(ids) > 0 {
			if limit > 0 && offset >= 0 && len(ids) > offset+limit {
				ids = ids[offset : offset+limit]
			}
		}
		return
	}

	// for secondary indexes, let's just find the relevant index
	idx := t.selectIndex(filters, order)
	if idx == nil {
		err = errors.NoIndexError
		ids = nil
		return
	}

	// use the index to find the ids we want for this query
	ids, total, err = idx.Find(filters, offset, limit, order)

	return

}

// Update updates an existing object with new or existing properties
func (t *table) Update(q query.UpdateQuery) (int, error) {

	ids, total, err := t.getIds(q.Filters, 0, -1, query.NoOrder)
	if err != nil {
		return 0, err
	} else if len(ids) == 0 {
		return 0, nil
	}
	logging.Debug("Ids for update: %s", ids)

	// convert the query to a change set
	cs := newChangeSet(t, len(ids))
	for _, id := range ids {
		cs.Add(newEntityChange(t, id, changeUpdate, q.Changes...))
	}

	// execute the change set, indexing everything we've updated
	num, err := cs.Execute()
	if err != nil {
		return 0, err
	}
	logging.Info("Performed %d changes in changeset for query %s", num, q)

	return total, err

}

// selectIndex chooses the right index for the query, or returns nil if no index
// was found for the query
func (t table) selectIndex(filters query.Filters, order query.Ordering) index {

	var bestIdx index = nil
	var bestScore float32 = 0

	for _, idx := range t.indexes {
		logging.Debug("Matching filters %s order %s against index %s", filters, order, idx)
		if match, score := idx.Matches(filters, order); match {
			logging.Debug("Match score for %s: %f", idx, score)
			if bestIdx == nil || score > bestScore {
				bestIdx = idx
				bestScore = score
			}
		}
	}
	logging.Debug("Best index match for %s: %s (%f)", filters, bestIdx, bestScore)
	return bestIdx

	return nil

}

// redisError generates an internal Error object for redis problems
func redisError(err error) error {
	switch err.(type) {
	case *errors.Error:
		return err
	}
	return errors.NewError("Redis error: %s", err)
}

// readEntity takes a raw slice of redis HGETALL return values and loads them into an internal entity
func (t *table) readEntity(id schema.Key, vals []interface{}) *schema.Entity {

	ret := schema.NewEntity(id)
	for i := 0; i < len(vals); i += 2 {

		//logging.Info("Reading property for entity %s: Key: '%v', value: '%v'", id, vals[i], vals[i+1])

		propName := string(vals[i].([]byte))
		if propName == schema.IdKey {
			continue
		}
		//no such property
		if vals[i+1] == nil {
			continue
		}
		value, err := decoder.Decode(vals[i+1].([]byte), schema.UnknownType)
		if err != nil {
			logging.Error("Error loading entity: %s", err)
			continue
		}

		ret.Properties[propName] = value

	}

	return ret

}

// load reads objects from redis using a list of keys, and returns a list
// of entities stored at these ids, or an error if we failed loading.
// missing objects will be returned as nil.
// an optional list of properties to load can be provided. If nil or empty, all properties will be loaded
func (t *table) load(ids []schema.Key, properties ...string) (ents []schema.Entity, err error) {

	conn := pool.Get()
	defer conn.Close()

	batch := NewBatch(conn)
	ents = make([]schema.Entity, 0, len(ids))

	// If the query includes specific properties, we only fetch them
	var props []interface{}
	if len(properties) > 0 {

		// to avoid redundant allocations, we keep one slot in the props slice for the key name.
		// this will later be sent to HMGET
		props = make([]interface{}, len(properties)+1)
		for i, p := range properties {
			props[i+1] = p
		}
	}

	// TODO: Received everything over this connection on failure

	for _, id := range ids {

		var e error
		if len(properties) == 0 {
			_, e = batch.Send("HGETALL", t.idKey(id))
		} else {

			// we put the key id as the first argument in props
			props[0] = t.idKey(id)
			_, e = batch.Send("HMGET", props...)
		}

		if e != nil {
			return nil, redisError(e)
		}
	}

	rets, e := batch.Execute()
	if e != nil {
		return nil, redisError(e)
	}

	for i := 0; i < len(ids); i++ {
		vals, _ := redis.Values(rets[i].Reply())

		// if we only had a partial-property query, we need to "zip" the requested properties and values
		if len(properties) > 0 && len(properties) == len(vals) {

			zipped := make([]interface{}, len(vals)*2)
			for i, v := range vals {
				zipped[i*2] = []byte(properties[i])
				zipped[i*2+1] = v
			}

			vals = zipped
		}
		if len(vals) > 1 {
			ents = append(ents, *t.readEntity(ids[i], vals))
		}

	}

	return

}

func (t *table) Get(q query.GetQuery, res *query.GetResponse) {

	ids, total, err := t.getIds(q.Filters, q.Paging.Offset, q.Paging.Limit, q.Order)
	if err != nil {
		res.Error = errors.Wrap(err)
		return
	}

	ents, err := t.load(ids, q.Properties...)
	if err != nil {
		res.Error = errors.Wrap(err)
		return
	}
	res.Total = total
	res.Entities = ents

	return

}

func (t *table) Delete(filters query.Filters) (int, error) {

	chunk := DefaultConfig.DeleteChunkSize

	//offset := 0
	total := 0

	for {
		ids, _, err := t.getIds(filters, 0, chunk, query.NoOrder)

		if err != nil {
			return 0, err
		} else if len(ids) == 0 {
			break
		}

		cs := newChangeSet(t, len(ids))
		for _, id := range ids {

			cs.Add(newEntityChange(t, id, changeDelete, query.Change{Op: query.OpDel}))

		}

		num, err := cs.Execute()
		if err != nil {
			return 0, err
		}
		total += num
		// if we've deleted all object, offset remains 0. if there were holes, they are now at the start
		// and we need to skip them

		logging.Debug("Performed %d changes in changeset for delete, deleted %d objects", num, len(ids))

	}

	logging.Info("Total deleted rows: %d", total)
	return total, nil
}

var sizeRE = regexp.MustCompile("serializedlength:([0-9]+)")

func (t *table) Stats(numSamples int) (*driver.TableStats, error) {

	conn := pool.Get()
	if conn == nil {
		return nil, redisError(errors.NewError("Could not get connection"))
	}

	k := t.primary.RedisKey()

	sz, err := redis.Int(conn.Do("ZCARD", k))
	if err != nil {
		return nil, redisError(errors.NewError("Error sampling %s: %s", t.primary, err))
	}

	if sz < numSamples {
		numSamples = sz
	}

	totalSize := 0
	keysSize := 0
	for i := 0; i < numSamples; i++ {
		offset := rand.Intn(sz)

		entries, err := redis.Strings(conn.Do("ZRANGE", k, offset, offset))

		if err == nil && len(entries) == 1 {

			info, err := redis.String(conn.Do("DEBUG", "OBJECT", t.idKey(schema.Key(entries[0]))))
			if err != nil {
				logging.Error("Error sampling key '%s': %s", entries[0], err)
				continue
			}

			if matches := sizeRE.FindStringSubmatch(info); len(matches) == 2 {

				sz, _ := strconv.ParseInt(matches[1], 10, 32)
				totalSize += int(sz)
				keysSize += len(entries[0])

			}

		} else {
			return nil, redisError(errors.NewError("Error sampling key data: %s", err))
		}
	}

	if sz > numSamples {

		sampleRatio := float32(numSamples) / float32(sz)
		totalSize = int(float32(totalSize) / sampleRatio)
		keysSize = int(float32(keysSize) / sampleRatio)

	}

	return &driver.TableStats{
		NumRows:           driver.Counter(sz),
		EstimatedDataSize: driver.ByteCounter(totalSize),
		EstimatedKeysSize: driver.ByteCounter(keysSize),
	}, nil

}
