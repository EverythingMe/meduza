package redis

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"hash/fnv"
	"io"
	"reflect"
	"strings"

	"github.com/dvirsky/go-pylog/logging"
	"github.com/garyburd/redigo/redis"
	"github.com/EverythingMe/meduza/errors"
	"github.com/EverythingMe/meduza/query"
	"github.com/EverythingMe/meduza/schema"
)

// primaryIndex describes all primary indexes. The default one just generates random ids
type primaryIndex interface {
	index

	// GenerateIds gets a list of entities and generates the proper id for every entity given.
	// We don't change the entity's id, but return a list of ids matching the entity list's length and order,
	// so the caller can assign the id to the entity
	GenerateId(entity schema.Entity) (schema.Key, error)

	Unindex(ids ...schema.Key) error
}

// randomPrimaryIndex is a primary index that generates random ids for entities that don't have pre-set ids,
// or blindly accepts any id given to an entity from the client
type basePrimaryIndex struct {
	desc  schema.Index
	table *table
	idkey string
}

func (idx basePrimaryIndex) find(ids []interface{}) ([]schema.Key, int, error) {

	keys := make([]schema.Key, len(ids))

	b := NewBatch(pool.Get())
	defer b.Abort()
	for i, id := range ids {
		switch tid := id.(type) {
		case schema.Key:
			keys[i] = tid
		case string:
			keys[i] = schema.Key(tid)

		case []byte:
			keys[i] = schema.Key(tid)
		default:
			logging.Error("Non string Id given querying %s: %v (%s)", idx.table, id, reflect.TypeOf(id))
			continue
		}

		if _, err := b.Send("EXISTS", idx.table.idKey(keys[i])); err != nil {
			return nil, 0, redisError(err)
		}

	}
	b.Send("ZCARD", idx.RedisKey())

	// non existing keys will be deleted from the index

	rets, err := b.Execute()
	if err != nil {
		return nil, 0, redisError(err)
	}
	ret := make([]schema.Key, 0, len(keys))

	// We perform "read repair" on the primary index - non existing entries get deleted from it
	repairs := make([]schema.Key, 0, len(keys))

	var total int
	for i, p := range rets {
		if i < len(keys) {
			if exists, _ := redis.Bool(p.Reply()); exists {
				ret = append(ret, keys[i])
			} else {
				repairs = append(repairs, keys[i])
			}
		} else {
			total, _ = redis.Int(p.Reply())
		}

	}

	if len(repairs) > 0 {
		logging.Info("Repairing %d dangling entries in primary key", len(repairs))
		idx.removeEntries(repairs...)
	}

	return ret, total, nil
}

func (i basePrimaryIndex) MatchesProperties(properties ...string) bool {
	logging.Warning("primary.MatchesProperties should never be called!")
	return false
}

func (i basePrimaryIndex) Properties() []string {
	return []string{schema.IdKey}
}

// scan returns a partial scan of the keys in the primary index, based on limit and order
func (i basePrimaryIndex) scan(offset, limit int, order query.Ordering) ([]schema.Key, int, error) {

	cmd := newRedisCommand("ZRANGE", i.RedisKey())
	if !order.Ascending {
		cmd.command = "ZREVRANGE"
	}

	if limit > 0 {
		cmd.add(offset, offset+limit-1)
	} else {
		cmd.add(0, -1)
	}

	tx := NewTransaction(pool.Get())
	defer tx.Abort()

	idsP, err := tx.Send(cmd.command, cmd.args...)
	if err != nil {
		return nil, 0, redisError(err)
	}

	totalP, err := tx.Send("ZCARD", i.RedisKey())
	if err != nil {
		return nil, 0, redisError(err)
	}

	if _, err = tx.Execute(); err != nil {
		return nil, 0, redisError(err)
	}

	ids, _ := redis.Strings(idsP.Reply())
	total, _ := redis.Int(totalP.Reply())

	ret := make([]schema.Key, len(ids))
	for i := range ids {
		ret[i] = schema.Key(ids[i])
	}
	return ret, total, nil

}

func (i basePrimaryIndex) RawEntries(chunk int) (<-chan string, chan<- bool) {
	return nil, nil
}

func (i basePrimaryIndex) RemoveEntry(entry string) error {

	conn := pool.Get()
	_, err := conn.Do("ZREM", i.RedisKey(), entry)
	return err
}

func (i basePrimaryIndex) removeEntries(entries ...schema.Key) error {

	conn := pool.Get()

	args := redis.Args{i.RedisKey()}
	args = args.AddFlat(entries)

	_, err := conn.Do("ZREM", args...)
	return err
}

func (i basePrimaryIndex) Scan(chunk int) (<-chan schema.Key, chan<- bool) {

	idch := make(chan schema.Key)
	stopch := make(chan bool)
	go func() {
		offset := 0
		defer close(idch)
		for {
			ids, total, err := i.scan(offset, chunk, query.Ordering{By: schema.IdKey, Ascending: true})
			if err != nil {
				logging.Error("Error scanning %s: %s", i, err)

				return
			}

			for _, id := range ids {
				select {
				case idch <- id:
					//logging.Debug("Scann pushed id %s", id)
				case <-stopch:
					logging.Info("Stopping scan loop")
					return
				}
			}

			if len(ids) == 0 || total < offset+chunk {
				logging.Info("iteration stopped. aborting")
				return
			}
			offset += chunk
		}
	}()

	return idch, stopch

}

func (i basePrimaryIndex) RedisKey() string {

	if i.idkey == "" {
		i.idkey = fmt.Sprintf("%s::PRIMARY", i.table.desc.Name)
	}
	return i.idkey

}

func (i basePrimaryIndex) Unindex(ids ...schema.Key) error {
	logging.Info("Unindexing ids %s in primary %s", ids, i.desc.Name)
	delCmd := newUnindexCommand(i.RedisKey())
	for _, id := range ids {
		delCmd.addEntry(string(id))
	}

	conn := pool.Get()
	if conn != nil {
		_, err := conn.Do(delCmd.command, delCmd.args...)
		return err
	}
	return redisError(errors.NewError("Could not get connection from pool"))
}

func (i basePrimaryIndex) UnindexEntities(entities ...schema.Entity) error {

	ids := make([]schema.Key, len(entities))
	for i, e := range entities {
		ids[i] = e.Id
	}

	return i.Unindex(ids...)
}

// Pipeline is the main indexing utility, that allows concurrent and bulk indexing of entities on a single transaction.
//
// It returns a channel the caller sends entity diffs down, and a channel that eventually sends errors in indexing back.
// The caller needs to close the entity diff channel, and then wait for an error on the error channel, before executing the
// transaction.
func (i basePrimaryIndex) Pipeline(tx *Transaction) (chan<- *entityDiff, <-chan error) {

	ch := make(chan *entityDiff)
	ech := make(chan error)

	// enqueuing commands for indexing
	addCmd := newIndexCommand(i.RedisKey())
	delCmd := newUnindexCommand(i.RedisKey())

	go func() {
		for eDiff := range ch {
			// sending nil down the channel aborts it without closing
			if eDiff == nil {
				break
			}

			if eDiff.changeType == changeDelete {
				delCmd.addEntry(string(eDiff.id))
			} else {
				addCmd.addEntry(string(eDiff.id))
			}
		}

		// enqueue the ZREM command if we need to unindex anything
		if delCmd.valid() {
			if err := delCmd.send(tx); err != nil {
				logging.Error("Error sending command to transaction: %s", err)
				ech <- err
				return
			}
		}

		// enqueue the ZADD command if we need to index anything
		if addCmd.valid() {
			if err := addCmd.send(tx); err != nil {
				logging.Error("Error sending command to transaction: %s", err)
				ech <- err
				return
			}
		}

		// we send this to the error channel so the caller knows they can continue
		ech <- nil

	}()

	return ch, ech
}

type randomPrimaryIndex struct {
	basePrimaryIndex
}

// GenerateIds gets a list of entities and generates the proper id for every entity given.
// We don't change the entity's id, but return a list of ids matching the entity list's length and order,
// so the caller can assign the id to the entity
func (randomPrimaryIndex) GenerateId(ent schema.Entity) (schema.Key, error) {

	if ent.Id != "" {
		return ent.Id, nil
	}
	uid := make([]byte, 8)
	if _, err := io.ReadFull(rand.Reader, uid); err != nil {
		return "", errors.NewError("Could not generate random id: %s", err) // rand should never fail
	}

	return schema.Key(strings.TrimRight(base64.URLEncoding.EncodeToString(uid), "=")), nil

}

// Find gets the ids matching the filters. If this is a normal id search, we just match against the existence
// of these ids in redis. This index supports only this method
func (r randomPrimaryIndex) Matches(filters query.Filters, order query.Ordering) (bool, float32) {
	// see if this is a query that can be queried by a primary key
	if flt, single := filters.One(); single && flt.Property == schema.IdKey {
		return true, 1
	}
	return false, 0
}

func (r randomPrimaryIndex) Find(filters query.Filters, offset int, limit int, order query.Ordering) ([]schema.Key, int, error) {
	flt, single := filters.One()
	if !single || flt.Property != schema.IdKey {
		return nil, 0, errors.NewError("Filters do not match primary key")
	}

	if flt.Operator == query.All {
		ids, _, err := r.basePrimaryIndex.scan(offset, limit, order)
		if err != nil {
			return nil, 0, err
		}

		args := make([]interface{}, len(ids))
		for i := range ids {
			args[i] = ids[i]
		}

		return r.basePrimaryIndex.find(args)

	}

	if flt.Operator != query.Eq && flt.Operator != query.In {
		return nil, 0, errors.NewError("Unsupported operator for primary key: %s", flt.Operator)
	}

	return r.basePrimaryIndex.find(flt.Values)

}

func (idx randomPrimaryIndex) String() string {
	return "PRIMARY_RANDOM"
}

func newRandomPrimary(desc *schema.Index, tbl *table) randomPrimaryIndex {
	return randomPrimaryIndex{
		basePrimaryIndex{
			*desc,
			tbl,
			"",
		},
	}
}

type compoundPrimaryIndex struct {
	basePrimaryIndex
	properties propertyList
	hashed     bool
}

func (i compoundPrimaryIndex) Properties() []string {
	return []string(i.properties)
}

func newCompounPrimary(desc *schema.Index, tbl *table) compoundPrimaryIndex {

	hashed := desc.ExtraParams["hashed"] == true

	return compoundPrimaryIndex{
		basePrimaryIndex{
			*desc,
			tbl,
			"",
		},
		propertyList(desc.Columns),
		hashed,
	}

}

func (i compoundPrimaryIndex) processId(id []byte) schema.Key {
	if len(id) == 0 || id == nil {
		return ""
	}

	if i.hashed {
		h := fnv.New64a()
		h.Write(id)
		return schema.Key(fmt.Sprintf("%x", h.Sum64()))
	}
	return schema.Key(id)
}

// Find gets the ids matching the filters. If this is a normal id search, we just match against the existence
// of these ids in redis. Else the filter must generate them for itself
func (i compoundPrimaryIndex) Find(filters query.Filters, offset int, limit int, order query.Ordering) ([]schema.Key, int, error) {
	// if this is just a normal id search - find the usual way

	if flt, single := filters.One(); single && flt.Property == schema.IdKey {

		if flt.Operator == query.All {
			ids, _, err := i.scan(offset, limit, order)
			if err != nil {
				return nil, 0, err
			}

			args := make([]interface{}, len(ids))
			for i := range ids {
				args[i] = ids[i]
			}

			return i.find(args)

		}

		return i.find(flt.Values)
	}

	ids, err := i.filtersToIds(filters)
	if err != nil {
		return nil, 0, err
	}

	return i.find(ids)

}

func (idx compoundPrimaryIndex) String() string {
	return fmt.Sprintf("PRIMARY(%s)", idx.properties)
}

func (idx compoundPrimaryIndex) Matches(filters query.Filters, order query.Ordering) (bool, float32) {
	// see if this is a query that can be queried by a primary key
	if flt, single := filters.One(); single && flt.Property == schema.IdKey {
		return true, 1
	}

	// if the query is property based - we match if the property list is exactly our own property list
	if len(filters) != len(idx.properties) {
		return false, 0
	}

	for p := range filters {
		if !idx.properties.contains(p) {
			return false, 0
		}
	}

	return true, 1
}

func (idx compoundPrimaryIndex) filtersToIds(filters query.Filters) ([]interface{}, error) {

	idBuffers := make([]interface{}, 0, len(idx.properties))

	i := 0
	for _, p := range idx.properties {

		flt, found := filters[p]
		if !found {
			return nil, errors.NewError("Filter for %s not found in query", p)
		}

		// in the first iteration we start with a serialized version of each of filters' values
		if i == 0 {
			for _, val := range flt.Values {
				pv, err := prepareValue(val)
				if err != nil {
					return nil, err
				}

				idBuffers = append(idBuffers, formatValue(pv))
			}

		} else {
			// for the next iteration, we take every previous partial id, copy it and create N copies
			// for the N values of the current filter, appending each value to each copy
			tmpBuffs := make([]interface{}, 0, len(idBuffers)*len(flt.Values))

			for _, idBuf := range idBuffers {
				buf := idBuf.([]byte)
				for _, v := range flt.Values {

					// format the value and append it to a new copy of the current id buffer
					pv, err := prepareValue(v)
					if err != nil {
						return nil, err
					}

					s := formatValue(pv)
					//make a copy of the buf
					tmpBuf := make([]byte, len(buf), len(buf)+len(s))
					copy(tmpBuf, buf)

					// append the new value to it and add it to tmpBufs
					tmpBuffs = append(tmpBuffs, append(tmpBuf, s...))
				}
			}

			idBuffers = tmpBuffs
		}

		i++
	}

	for i := range idBuffers {
		idBuffers[i] = idx.processId(idBuffers[i].([]byte))
	}

	return idBuffers, nil

}

// GenerateIds gets a list of entities and generates the proper id for every entity given.
// We don't change the entity's id, but return a list of ids matching the entity list's length and order,
// so the caller can assign the id to the entity
func (i compoundPrimaryIndex) GenerateId(ent schema.Entity) (schema.Key, error) {

	// we estimate 10 bytes per property value to save some capacity for growing
	key := make([]byte, 0, len(i.properties)*10)

	for _, p := range i.properties {

		val, found := ent.Get(p)
		if !found || val == nil {
			return "", errors.NewError("Cannot index entity with missing/nil value for %s", p)
		}

		pv, err := prepareValue(val)
		if err != nil {
			return "", err
		}

		key = append(key, formatValue(pv)...)
	}

	return i.processId(key), nil
}
