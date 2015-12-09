package redis

import (
	"fmt"
	"time"

	"hash/fnv"
	"sort"
	"sync"

	"github.com/dvirsky/go-pylog/logging"
	"github.com/garyburd/redigo/redis"
	"github.com/EverythingMe/meduza/errors"
	"github.com/EverythingMe/meduza/query"
	"github.com/EverythingMe/meduza/schema"
)

type changeType int

const (
	//change types for entityChange

	changeNop changeType = iota
	changeUpdate
	changeInsert
	changeDelete
	changeReindex
)

type entityChange struct {
	changes           []query.Change
	changedProperties propertyList
	objectId          schema.Key
	changeType        changeType
	table             *table
}

func newEntityChange(t *table, objectId schema.Key, ct changeType, changes ...query.Change) entityChange {

	changedProps := make(propertyList, len(changes))
	for i := range changes {
		changedProps[i] = changes[i].Property
	}

	return entityChange{
		changes:           changes,
		changedProperties: changedProps.sorted(),
		objectId:          objectId,
		table:             t,
		changeType:        ct,
	}
}

type redisCommand struct {
	command string
	args    redis.Args
}

func newRedisCommand(cmd string, args ...interface{}) *redisCommand {
	return &redisCommand{
		cmd,
		args,
	}
}

func (r *redisCommand) add(args ...interface{}) *redisCommand {
	r.args = append(r.args, args...)
	return r
}

func (r *redisCommand) send(tx *Transaction) error {
	//logging.Debug("Enqueuing command %s with %d args", r.command, len(r.args))
	_, err := tx.Send(r.command, r.args...)
	return errors.Context(err)

}

func (r *redisCommand) valid() bool {
	return len(r.args) > 1
}

func (rc entityChange) commands() ([]*redisCommand, error) {

	hmset := newRedisCommand("HMSET", rc.table.idKey(rc.objectId))

	ret := []*redisCommand{hmset}

	for _, ch := range rc.changes {
		switch ch.Op {

		// nop is used for reindexing entities
		case query.Noop:
			continue
		case query.OpSet:
			val, err := encoder.Encode(ch.Value)
			if err != nil {
				return nil, redisError(fmt.Errorf("Could not encode changeset: %s", err))
			}
			hmset.add(ch.Property, val)

			// for del ops, we need a single DEL query, no use in continuing
		case query.OpDel:
			return []*redisCommand{newRedisCommand("DEL", rc.table.idKey(rc.objectId))}, nil
			//TODO: Add more ops handling here
		case query.OpIncrement:
			ret = append(ret, newRedisCommand("HINCRBY", rc.table.idKey(rc.objectId), ch.Property, ch.Value))
		case query.OpPropDel:
			ret = append(ret, newRedisCommand("HDEL", rc.table.idKey(rc.objectId), ch.Property))
		case query.OpExpire:

			var ttl int
			switch v := ch.Value.(type) {
			case time.Duration:
				ttl = int(v / time.Millisecond)
			case int64:
				ttl = int(v / int64(time.Millisecond))
			case int:
				ttl = v / int(time.Millisecond)
			default:
				return nil, redisError(fmt.Errorf("Invalid value for TTL: %v", ch.Value))
			}
			ret = append(ret, newRedisCommand("PEXPIRE", rc.table.idKey(rc.objectId), ttl))

		default:
			logging.Error("Unsupported op: %s", ch.Op)
			return nil, errors.OpNotSupported
		}
	}

	if len(hmset.args) <= 1 {
		ret = ret[1:]
	}

	if len(ret) > 0 {
		return ret, nil
	}
	return nil, nil
}

type changeSet struct {
	table   *table
	changes []entityChange
}

// NewChangeSet creates a new changeset on table T. you can give it a capacity hint that is basically
// the number of rows that's going to change
func newChangeSet(t *table, capacityHint int) *changeSet {
	return &changeSet{
		table:   t,
		changes: make([]entityChange, 0, capacityHint),
	}
}

type propertyList []string

func (l propertyList) contains(p string) bool {
	for _, lp := range l {
		if lp == p {
			return true
		}
	}

	return false
}

// Len is used for sorting
func (l propertyList) Len() int { return len(l) }

// Swap is used for sorting
func (l propertyList) Swap(i, j int) { l[i], l[j] = l[j], l[i] }

// Less is used for sorting
func (l propertyList) Less(i, j int) bool { return l[i] < l[j] }

func (l propertyList) sorted() propertyList {
	sort.Sort(l)
	return l
}

func (l propertyList) hash() uint64 {

	h := fnv.New64a()

	for _, p := range l {
		h.Write([]byte(p))
	}

	return h.Sum64()
}

func (l propertyList) equals(other propertyList) bool {

	if len(l) != len(other) {
		return false
	}

	for i := range l {
		if l[i] != other[i] {
			return false
		}
	}

	return true

}

// TODO: limit memory size for this
type indexableCache struct {
	entries map[uint64][]string
	lock    sync.RWMutex
}

func (c *indexableCache) set(h uint64, p []string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.entries[h] = p
}

func (c *indexableCache) get(h uint64) ([]string, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	p, f := c.entries[h]
	return p, f

}

var propCache = &indexableCache{map[uint64][]string{}, sync.RWMutex{}}

// indexableProperties takes the properties of a rowChange and determines which properties should be loaded
// when performing the update to match diffs
func (c *changeSet) indexableProperties(rc entityChange) []string {
	//TODO: cache this - do it just once per change set or something

	h := rc.changedProperties.hash()
	if cached, found := propCache.get(h); found {
		return cached
	}

	props := make(propertyList, 0, len(rc.changes))

	// Add the primary index's properties always
	props = append(props, c.table.primary.Properties()...)

	for _, ch := range rc.changes {

		for _, idx := range c.table.indexes {

			// for DEL changes - we need all indexable properties
			if rc.changeType == changeDelete {
				for _, prop := range idx.Properties() {
					if !props.contains(prop) {
						logging.Debug("Adding property %s to indexable property list of change %s", prop, ch)
						props = append(props, prop)
					}
				}
				continue
			}

			// for other queries we need only properties that are actually updated in the query,
			// or related ones used in multi property indexing
			logging.Debug("Checking props %s against idx %s", ch.Property, idx)
			if idx.MatchesProperties(ch.Property) {

				for _, prop := range idx.Properties() {
					if !props.contains(prop) {
						logging.Debug("Adding property %s to indexable property list of change %s", prop, ch)
						props = append(props, prop)
					}
				}
			}
		}
	}

	propCache.set(h, props)
	return props

}

// Add adds a single row change to the changeSet, without executing it
func (c *changeSet) Add(rc entityChange) {

	c.changes = append(c.changes, rc)
	logging.Debug("Adding row change %s to change set. len now %d", rc, len(c.changes))
}

// changeResult holds the pre and post
type changeResult struct {
	properties propertyList
	oldPromise *Promise
	change     entityChange
}

// entityDiff represents all the (indexable) changes an entity change caused
type entityDiff struct {
	id         schema.Key
	diffs      map[string]*propertyDiff
	changeType changeType
}

func (d entityDiff) vals(newVals bool) map[string]interface{} {

	ret := make(map[string]interface{})
	for k, pd := range d.diffs {
		if newVals {
			ret[k] = pd.newVal
		} else {
			ret[k] = pd.oldVal
		}

	}
	return ret
}

// propertyDiff represents the diff a change caused in one property
type propertyDiff struct {
	newVal   interface{}
	oldVal   interface{}
	op       query.ChangeOp
	changed  bool
	loadOnly bool
}

func (p propertyDiff) String() string {
	return fmt.Sprintf("diff[%v] %v => %v", p.changed, p.oldVal, p.newVal)
}

func (cr changeResult) getDiffs() (ret *entityDiff, err error) {

	ret = &entityDiff{
		id:         cr.change.objectId,
		diffs:      make(map[string]*propertyDiff),
		changeType: cr.change.changeType,
	}

	// we keep a lits of the old values of we've fetched any.
	// It is guaranteed to be the same order and length as the indexable properties
	var olds []interface{}
	if cr.oldPromise != nil {
		olds, _ = redis.Values(cr.oldPromise.Reply())
	}

	// build the diffs
	for i, p := range cr.properties {
		pd := &propertyDiff{}
		ret.diffs[p] = pd

		// if the property of this result was not actually changed, we consider this a "load only" change
		if !cr.change.changedProperties.contains(p) {
			logging.Debug("Property %s not in changed properties", p)
			pd.loadOnly = true
		}

		// if we have old values, fill the old value in the property diff
		if olds != nil {
			//logging.Debug("Old value for %s: %v", p, olds[i])
			switch v := olds[i].(type) {
			case []byte:
				if pd.oldVal, err = decoder.Decode(v, schema.UnknownType); err != nil {
					return nil, logging.Errorf("Could not decode %v: %s", olds[i], err)
				}
				logging.Debug("Decoded old val for %s: %s", p, pd.oldVal)

			case nil: // we keep nil values as nil
			default:
				return nil, logging.Errorf("Invalid value to deocde: %v", olds[i])
			}
		}
	}

	// fill the diffs with new values
	if cr.change.changeType != changeDelete {
		for _, change := range cr.change.changes {
			if pd := ret.diffs[change.Property]; pd != nil {

				// mark the diff as a change or not - depending on whether the old and new values differ
				pd.changed = change.Value != pd.oldVal

				pd.newVal = change.Value

			}
		}
	}

	// now prepare the values for all raw diffs
	for _, pd := range ret.diffs {

		// now encode the values for indexing
		if pd.oldVal, err = prepareValue(pd.oldVal); err != nil {
			return nil, logging.Errorf("Error encoding value for indexing: %s", err)
		}

		if pd.newVal, err = prepareValue(pd.newVal); err != nil {
			return nil, logging.Errorf("Error encoding value for indexing: %s", err)
		}

		// for load only UPDATE results, we need to set the new val as the old.
		// this is used to index unchanged properties in compound indexes.
		if pd.loadOnly && cr.change.changeType != changeDelete {
			pd.newVal = pd.oldVal
		}

	}

	return ret, nil
}

func (c *changeSet) indexChanges(results []changeResult) error {

	tx := NewTransaction(pool.Get())
	defer tx.Abort()

	diffs := make([]*entityDiff, len(results))
	for i, res := range results {
		eDiff, err := res.getDiffs()
		if err != nil {
			return redisError(err)
		}

		diffs[i] = eDiff
	}

	pipe, errchan := c.table.primary.Pipeline(tx)
	for _, eDiff := range diffs {
		pipe <- eDiff
	}
	close(pipe)

	// TODO: continue writing to next index, and collect errors from all eventually
	if err := <-errchan; err != nil {
		return logging.Errorf("Error indexing entities: %s", err)
	}

	for _, idx := range c.table.indexes {
		pipe, errchan := idx.Pipeline(tx)
		for _, eDiff := range diffs {
			pipe <- eDiff
		}
		close(pipe)

		// TODO: continue writing to next index, and collect errors from all eventually
		if err := <-errchan; err != nil {
			return logging.Errorf("Error indexing entities: %s", err)
		}
	}

	_, err := tx.Execute()
	return err

}

// Execute takes the chagneset and executes it. returns the number of changes executed
func (c *changeSet) Execute() (int, error) {

	tx := NewTransaction(pool.Get())
	defer tx.Abort()

	results := make([]changeResult, 0, len(c.changes))

	for _, rc := range c.changes {

		// if we need to get the prev value of any fields prior to the change - we add an HMGET before
		if indexable := c.indexableProperties(rc); len(indexable) > 0 {

			switch rc.changeType {

			case changeUpdate, changeDelete:
				args := make(redis.Args, 0, len(indexable)).Add(c.table.idKey(rc.objectId)).AddFlat(indexable)

				promise, err := tx.Send("HMGET", args...)
				if err != nil {
					return 0, redisError(err)
				}
				results = append(results, changeResult{indexable, promise, rc})

			case changeInsert, changeReindex:
				results = append(results, changeResult{indexable, nil, rc})
			}
		}

		// now we enqueue the commands to perform the change
		if cmds, err := rc.commands(); err != nil {
			return 0, redisError(err)
		} else if cmds != nil {

			for _, cmd := range cmds {
				logging.Debug("Enqueuing command %s", *cmd)
				if _, err := tx.Send(cmd.command, cmd.args...); err != nil {
					return 0, redisError(err)
				}
			}
		}

	}

	if _, err := tx.Execute(); err != nil {
		return 0, redisError(fmt.Errorf("Failed performing changeset transaction: %s", err))
	}

	if err := c.indexChanges(results); err != nil {
		logging.Error("Could not index objects: %s", err)
		return 0, err
	}

	if _, err := tx.Execute(); err != nil {
		return 0, redisError(err)
	}
	return len(c.changes), nil

}
