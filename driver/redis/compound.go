package redis

import (
	"fmt"
	"strings"

	"github.com/dvirsky/go-pylog/logging"
	"github.com/garyburd/redigo/redis"
	"github.com/EverythingMe/meduza/errors"
	"github.com/EverythingMe/meduza/query"
	"github.com/EverythingMe/meduza/schema"
)

// CompoundIndex is an index that can index just one column, regardless of its type
type CompoundIndex struct {
	desc       schema.Index
	properties propertyList
	table      string
	key        string
}

// NewCompoundIndex creates a new compound index using a descriptor, for the given table name
func NewCompoundIndex(idx schema.Index, table string) *CompoundIndex {

	return &CompoundIndex{
		desc:       idx,
		properties: propertyList(idx.Columns),
		table:      table,
	}

}

func (i *CompoundIndex) String() string {
	return i.desc.Name
}

// Matches returns true if a query can be searched by this index.
// It matches if the filter set's properties are a *prefix* of this index's properties,
// AND the ordering, if there is one, is the last or sole property of the index
//
// i.e if we index P1,P2,P3 and:
//    1. the filter map is for P1 - we match
//    2. the filter map is for P1,P2 - we match
//    3. the filter map is for P2,P3 - we do not match
//    4. the filter map is for P1,P3 -  we do not match
func (i *CompoundIndex) Matches(filters query.Filters, order query.Ordering) (bool, float32) {

	expectedMatches := len(filters)
	if !order.IsNil() {
		// if we have an ordering - it must be the last property of this index or we couldn't sort by it
		if order.By != i.properties[len(i.properties)-1] {
			logging.Debug("Order by '%s' cannot be done by %s", order.By, i.desc.Name)
			return false, 0
		}

		// if the order clause is not part of the filter, we need to match it as well
		if _, found := filters[order.By]; !found {
			expectedMatches++
		}

	}

	if expectedMatches > len(i.properties) {
		return false, 0
	}

	matches := 0
	for _, p := range i.properties {
		if _, found := filters[p]; !found {

			if !order.IsNil() && p == order.By {
				matches++
				continue
			}
			logging.Debug("Filters do not match against index %s", i)
			return false, 0
		}
		matches++
		// if all properties in our list have been found in the filters - we can perform the query and we stop
		if matches == expectedMatches {
			break
		}
	}
	logging.Debug("Filters match against index %s", i)
	return true, float32(matches) / float32(len(i.properties))

}

// MatchesProperties tells us whether the properties are a subset of our own property
func (i *CompoundIndex) MatchesProperties(properties ...string) bool {

	for _, p := range properties {
		if !i.properties.contains(p) {
			return false
		}
	}
	return true
}

// Properties returns the list of properties this index indexes
func (i *CompoundIndex) Properties() []string {

	return i.properties
}

// redisKey generates the desired redis key for this index
func (i *CompoundIndex) RedisKey() string {
	if i.key == "" {
		i.key = fmt.Sprintf("k:%s/%s", i.table, strings.Join(i.properties, "_"))
	}
	return i.key

}

// entry returns the internal entry for a value inside the index
func (i *CompoundIndex) entry(id schema.Key, properties map[string]interface{}) string {

	// a valid entry is one that contains at least one non nil value
	validEntry := false
	stringVals := make([]byte, 0, len(i.properties)*10)
	for n, p := range i.properties {
		if _, found := properties[p]; !found {
			return ""
		}
		if n > 0 {
			stringVals = append(stringVals, '|')
		}

		v := properties[p]

		if v != nil {
			validEntry = true
			stringVals = append(stringVals, []byte(fmt.Sprintf("%v", v))...)
		}

	}
	if validEntry {
		stringVals = append(stringVals, '|', ':', ':')
		stringVals = append(stringVals, []byte(id)...)
		return string(stringVals)
	}

	return ""

}

// entry returns the internal entry for a value inside the index
func (i *CompoundIndex) diffEntry(eDiff *entityDiff, useNew bool) string {

	return i.entry(eDiff.id, eDiff.vals(useNew))
}

// entry returns the internal entry for a value inside the index
func (i *CompoundIndex) rangeKeys(vals query.Filters, order query.Ordering) (rStart string, rEnd string, err error) {

	startVals := []byte("[")
	endVals := []byte("(")
	var pv interface{}

	nProps := 0

	numRanges := 0

	for _, p := range i.properties {

		// append the separator if this is not the first iteration

		f, found := vals[p]

		// we break at the first property missing from the query.
		// this means we can do partial finds on at least 1 property
		if !found {
			break
		}
		nProps++

		switch f.Operator {

		case query.Eq:
			if numRanges > 0 {
				err = errors.NewError("Ranges must come after equality filters in the index's column order")
				return
			}

			if pv, err = prepareValue(f.Values[0]); err != nil {
				return
			}

			b := []byte(fmt.Sprintf("%v|", pv))
			startVals = append(startVals, b...)
			endVals = append(endVals, b...)

		case query.Between:

			if !order.IsNil() && order.By != p {
				err = errors.NewError("Range queries can only be ordered by the range property")
				return
			}
			if numRanges > 0 {
				err = errors.NewError("Only a single range per query allowed")
				return
			}
			numRanges++
			if pv, err = prepareValue(f.Values[0]); err != nil {
				return
			} else {
				startVals = append(startVals, []byte(fmt.Sprintf("%v", pv))...)
			}

			if pv, err = prepareValue(f.Values[1]); err != nil {
				return
			} else {
				endVals = append(endVals, []byte(fmt.Sprintf("%v", pv))...)
			}
		default:
			err = errors.NewError("Invalid filter type for index %s: %s", i.desc.Name, f.Operator)
			return
		}

	}

	if nProps > 0 {

		endVals = append(endVals, 0xff)
		rStart = string(startVals)
		rEnd = string(endVals)
	}
	logging.Debug("Ranges for filters: '%s' - '%s'", rStart, rEnd)
	return

}

//// UnindexEntities removes a list of entities from this index
//func (i *CompoundIndex) UnindexEntities(entities ...schema.Entity) error {

//	delCmd := newUnindexCommand(i.redisKey())
//	for _, ent := range entities {

//		entry := i.entry(ent.Id, ent.Properties)
//		if entry != "" {
//			logging.Debug("Unindexing entity %s", ent.Id)
//			delCmd.add(entry)
//		}
//	}

//	conn := pool.Get()
//	if conn != nil {
//		_, err := conn.Do(delCmd.command, delCmd.args...)
//		return err
//	}
//	return redisError(errors.NewError("Could not get connection from pool"))
//}

// Pipeline is the main indexing utility, that allows concurrent and bulk indexing of entities on a single transaction.
//
// It returns a channel the caller sends entity diffs down, and a channel that eventually sends errors in indexing back.
// The caller needs to close the entity diff channel, and then wait for an error on the error channel, before executing the
// transaction.
func (i *CompoundIndex) Pipeline(tx *Transaction) (chan<- *entityDiff, <-chan error) {

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

			addEntry := ""
			delEntry := i.diffEntry(eDiff, false)

			// for inserts/updates - we need to compare the new entry and old entry to decide what to do
			if eDiff.changeType != changeDelete {
				addEntry = i.diffEntry(eDiff, true)
				//logging.Debug("Add entry: %s, delEntry: %s", addEntry, delEntry)
				// we compare the entries to see if we need un/reindex
				if addEntry != delEntry {
					if len(addEntry) > 0 {
						addCmd.addEntry(addEntry)
					}
					if len(delEntry) > 0 {
						delCmd.addEntry(delEntry)
					}

				}
			} else if delEntry != "" { //for del queries
				delCmd.addEntry(delEntry)
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

// Find returns ids from the query's filters. The query should have exactly 1 filter.
// We assume matching of the index to the query has been checked before
func (i *CompoundIndex) Find(filters query.Filters, offset, limit int, order query.Ordering) ([]schema.Key, int, error) {

	// now we ZRANGE the selected key - either the original one or an aggregated one
	b := NewBatch(pool.Get())
	defer b.Abort()

	rangeStart, rangeEnd, err := i.rangeKeys(filters, order)
	if err != nil {
		return nil, 0, err
	}

	cmd := newRedisCommand("ZRANGEBYLEX", i.RedisKey(), rangeStart, rangeEnd)

	// If the query is ordered and descending, we do a ZREVBYLEX and reverse end and start
	if !order.IsNil() && !order.Ascending {
		cmd = newRedisCommand("ZREVRANGEBYLEX", i.RedisKey(), rangeEnd, rangeStart)
	}

	if limit > 0 {
		cmd.add("LIMIT", offset, offset+limit)
	}

	if _, err := b.Send(cmd.command, cmd.args...); err != nil {
		return nil, 0, redisError(err)
	}

	//we also want the cardinality of the key - i.e. how many results did we find
	// we reuse the args of the RANGE command but trim them if they have a LIMIT set
	if _, err := b.Send("ZLEXCOUNT", cmd.args[:3]...); err != nil {
		return nil, 0, redisError(err)
	}

	rets, err := b.Execute()
	if err != nil {
		return nil, 0, redisError(err)
	}

	ids, _ := redis.Strings(rets[0].Reply())
	card, _ := redis.Int(rets[1].Reply())
	//logging.Debug("Ids: %s, total: %s", ids, card)
	ret := make([]schema.Key, len(ids))
	for n, id := range ids {
		ret[n] = extractId(id)

	}

	return ret, card, nil

}

func extractId(s string) schema.Key {
	parts := strings.Split(s, "::")
	if len(parts) == 2 {
		return schema.Key(parts[1])
	}
	return ""
}

// scanRaw returns a partial scan of the raw keys in the primary index, based on limit and order, but does
// not convert them to ids, returning the real entries in the index
func (i CompoundIndex) scanRaw(offset, limit int, order query.Ordering) ([]string, int, error) {

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

	return ids, total, nil
}

//// scan returns a partial scan of the keys in the primary index, based on limit and order
//func (i CompoundIndex) scan(offset, limit int, order query.Ordering) ([]schema.Key, int, error) {
//	ids, total, err := i.scanRaw(offset, limit, order)
//	if err != nil {
//		return nil, 0, err
//	}

//	ret := make([]schema.Key, len(ids))
//	for n := range ids {
//		ret[n] = extractId(ids[n])

//	}
//	return ret, total, nil

//}

func (i CompoundIndex) RemoveEntry(entry string) error {

	conn := pool.Get()

	_, err := conn.Do("ZREM", i.RedisKey(), entry)
	return err
}

func (i CompoundIndex) Scan(chunk int) (<-chan schema.Key, chan<- bool) {

	idch := make(chan schema.Key)
	stopch := make(chan bool)
	go func() {
		defer close(idch)
		ch, sch := i.RawEntries(chunk)

		for id := range ch {
			k := extractId(id)
			select {
			case idch <- k:
				logging.Debug("Scann pushed id %s", id)
			case <-stopch:
				logging.Info("Stopping scan loop")
				sch <- true
				return
			}
		}

	}()

	return idch, stopch

}

func (i CompoundIndex) RawEntries(chunk int) (<-chan string, chan<- bool) {

	idch := make(chan string)
	stopch := make(chan bool)
	go func() {
		offset := 0
		defer close(idch)
		for {
			ids, total, err := i.scanRaw(offset, chunk, query.Ordering{By: schema.IdKey, Ascending: true})
			if err != nil {
				logging.Error("Error scanning %s: %s", i, err)

				return
			}

			for _, id := range ids {
				select {
				case idch <- id:
					logging.Debug("Scann pushed id %s", id)
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
