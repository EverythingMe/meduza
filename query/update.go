package query

import (
	"time"

	"github.com/EverythingMe/meduza/errors"
	"github.com/EverythingMe/meduza/schema"
)

// ChangeOp is a wrapper to a string, making sure you don't accidentally put random strings into changes
type ChangeOp string

// Constants for chanbge Ops
const (
	OpSet ChangeOp = "SET"
	// Delete an entire entity
	OpDel       ChangeOp = "DEL"
	OpExpire    ChangeOp = "EXP"
	OpIncrement ChangeOp = "INCR"
	OpSetAdd    ChangeOp = "SADD"
	OpSetDel    ChangeOp = "SDEL"
	OpMapSet    ChangeOp = "MSET"
	OpMapDel    ChangeOp = "MDEL"
	// Delete a single property of an entity
	OpPropDel ChangeOp = "PDEL"
	// noop is a special internal OP that is used for reindexing entities.
	// it is not valid in a client request
	Noop ChangeOp = "NOOP"
)

// Change represents a single change in an UPDATE query- what property is being updated, how and to what value
type Change struct {
	Property string      `bson:"property"`
	Value    interface{} `bson:"value"`
	Op       ChangeOp    `bson:"op"`
}

// Validate makes sure a change is sane - the op is supported , etc
func (c Change) Validate() error {

	if c.Property == "" && c.Op != OpExpire {
		return errors.NewError("No property name for change")
	}

	switch c.Op {
	case OpSet, OpDel, OpIncrement, OpExpire, OpPropDel:
	default:
		return errors.NewError("Change Op %s still not supported", c.Op)
	}
	return nil

}

// Set returns a new SetOp change for a property to given value
func Set(prop string, newVal interface{}) Change {
	return Change{prop, internal(newVal), OpSet}
}

// Incement returns a new Incr change for a property with a given integer value
func Increment(prop string, amount int64) Change {
	return Change{prop, internal(amount), OpIncrement}
}

// DelProperty is an UPDATE change that deletes a single property from an entity
func DelProperty(prop string) Change {
	return Change{Property: prop, Op: OpPropDel}
}

// IncrementFloat returns a new Incr for a property with a given float value. Should be used only on float properties
func IncrementFloat(prop string, amount float64) Change {
	return Change{prop, internal(amount), OpIncrement}
}

func Expire(ttl time.Duration) Change {
	return Change{"", ttl, OpExpire}
}

// UpdateQuery represents an UPDATE request sent to the server and processed by the relevant driver
type UpdateQuery struct {
	Table   string   `bson:"table"`
	Filters Filters  `bson:"filters"`
	Changes []Change `bson:"changes"`
}

// UpdateResponse is used to send a response back to clients to an update query.
// It includes the usual Response fields, and the number of entities affected by the update
type UpdateResponse struct {
	*Response
	Num int `bson:"num"`
}

// NewUpdateQuery initializes an update query for a given table
func NewUpdateQuery(table string) *UpdateQuery {
	return &UpdateQuery{
		Table:   table,
		Filters: make(Filters),
		Changes: make([]Change, 0, 1),
	}
}

// NewUpdateResponse creates a new response object to send back to a client,
// with a given error and the number of entities updated
func NewUpdateResponse(err error, num int) *UpdateResponse {
	return &UpdateResponse{
		NewResponse(err),
		num,
	}
}

// Where adds a selection filter indicating what entities will be updated
func (q *UpdateQuery) Where(prop string, operator string, values ...interface{}) *UpdateQuery {
	q.Filters[prop] = NewFilter(prop, operator, values...)
	return q
}

// WhereId creates a selection filter by primary ids
func (q *UpdateQuery) WhereId(ids ...interface{}) *UpdateQuery {
	return q.Where(schema.IdKey, In, ids...)
}

// WhereEquals creates an Eq filter on the query
func (q *UpdateQuery) WhereEquals(prop string, values ...interface{}) *UpdateQuery {
	return q.Where(prop, Eq, values)
}

// Set adds a SET op change to the query's change set
func (q *UpdateQuery) Set(prop string, newVal interface{}) *UpdateQuery {
	q.Changes = append(q.Changes, Set(prop, newVal))
	return q
}

// DelProperty is an UPDATE change that deletes a single property from an entity
func (q *UpdateQuery) DelProperty(prop string) *UpdateQuery {
	q.Changes = append(q.Changes, DelProperty(prop))
	return q
}

func (q *UpdateQuery) Increment(prop string, newVal int64) *UpdateQuery {
	q.Changes = append(q.Changes, Increment(prop, newVal))
	return q
}

func (q *UpdateQuery) Expire(ttl time.Duration) *UpdateQuery {
	q.Changes = append(q.Changes, Expire(ttl))
	return q
}

// Validate tests the query's parameter for validity (not against a schema - just that they are sane).
// If any problem is found it returns an error
func (q *UpdateQuery) Validate() (err error) {
	if q.Table == "" {
		return errors.NewError("No table for PUT query")
	}
	if len(q.Filters) == 0 {
		return errors.NewError("No selection filters for Update query")
	}
	if len(q.Changes) == 0 {
		return errors.NewError("No changs in update query")
	}

	for _, f := range q.Filters {
		if err = f.Validate(); err != nil {
			return
		}
	}

	for _, c := range q.Changes {
		if err = c.Validate(); err != nil {
			return
		}
	}

	return
}
