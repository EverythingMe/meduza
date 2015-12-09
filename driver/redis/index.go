package redis

import (
	"fmt"
	"math"

	"encoding/binary"
	"encoding/hex"

	"github.com/EverythingMe/meduza/query"
	"github.com/EverythingMe/meduza/schema"
)

// index represents an index in the redis database
type index interface {
	// Matches tells us whether the index can find the entities in the filter set
	Matches(filters query.Filters, order query.Ordering) (bool, float32)

	// MatchesProperties tells us whether the index can find the entities based on the properties in the list
	MatchesProperties(properties ...string) bool

	// Properties returns the list of properties this index is indexing
	Properties() []string

	// Find gets a filter set and paging offsets, and returns a list of keys that match filters.
	// Pass limit -1 to get all the ids
	Find(filters query.Filters, offset, limit int, order query.Ordering) ([]schema.Key, int, error)

	// Pipeline is the main indexing utility, that allows concurrent and bulk indexing of entities on a single transaction.
	//
	// It returns a channel the caller sends entity diffs down, and a channel that eventually sends errors in indexing back.
	// The caller needs to close the entity diff channel, and then wait for an error on the error channel, before executing the
	// transaction.
	//
	// An index just reads all the entity diffs, queues them for bulk indexing/unindexing, and when the channel is closed,
	// it queues them with as little as possible redis requests on the transaction.
	// then the caller executes the transaction with the buffered indexing commandss
	Pipeline(tx *Transaction) (chan<- *entityDiff, <-chan error)

	// Scan returns a channel that scans through the index and returns all the ids of the objects indexed in it
	Scan(chunk int) (idch <-chan schema.Key, stopch chan<- bool)

	RawEntries(chunk int) (ch <-chan string, stopch chan<- bool)

	RemoveEntry(entry string) error

	RedisKey() string

	//UnindexEntities(entities ...schema.Entity) error
}

// unindexCommand wraps a redis command for unindexing keys (ZREM)
type unindexCommand struct{ *redisCommand }

// indexCommand wraps a redis command ZADD for indexing keys
type indexCommand struct{ *redisCommand }

func newUnindexCommand(key string) *unindexCommand {
	return &unindexCommand{
		newRedisCommand("ZREM", key),
	}
}
func newIndexCommand(key string) *indexCommand {
	return &indexCommand{
		newRedisCommand("ZADD", key),
	}
}

func (c *unindexCommand) addEntry(entry string) {
	c.add(entry)
}

func (c *indexCommand) addEntry(entry string) {
	c.add(0, entry)
}

// prepareValue takes a value and if needed pre-processes it for indexing. This is mainly used for
// normalizing texts
func prepareValue(val interface{}) (interface{}, error) {
	if val == nil {
		return nil, nil
	}
	nrml := getNormalizer()
	defer putNormalizer(nrml)
	switch tv := val.(type) {
	case schema.Text:
		return nrml.NormalizeString(string(tv))
	case string:
		return nrml.NormalizeString(tv)
	case []byte:
		return nrml.Normalize(tv)

	case schema.Int:
		ret := make([]byte, binary.Size(tv))
		binary.BigEndian.PutUint64(ret, uint64(tv))
		return hex.EncodeToString(ret), nil
	case schema.Uint:
		ret := make([]byte, binary.Size(tv))
		binary.BigEndian.PutUint64(ret, uint64(tv))
		return hex.EncodeToString(ret), nil
	case schema.Float:
		uintval := math.Float64bits(float64(tv))
		if uintval&0x8000000000000000 != 0 {
			uintval = ^uintval & 0xFFFFFFFFFFFFFFFF
		} else {
			uintval |= 0x8000000000000000
		}
		return prepareValue(schema.Uint(uintval))

	default:
		return encoder.Encode(val)

	}

}

// formatValue takes a prepared value and  converts it to the serialized version we use in indexing
func formatValue(val interface{}) []byte {

	switch v := val.(type) {
	case []byte:
		return append(v, '|')
	case string:
		return append([]byte(v), '|')
	case schema.Text:
		return append([]byte(v), '|')
	default:
		return []byte(fmt.Sprintf("%v|", val))
	}
}
