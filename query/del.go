package query

import "github.com/EverythingMe/meduza/errors"

// DelQuery represents a DEL request handled by the appropriate driver
type DelQuery struct {
	Table   string  `bson:"table"`
	Filters Filters `bson:"filters"`
}

// NewDelQuery creates a new query object for a given table
func NewDelQuery(table string) *DelQuery {
	return &DelQuery{
		Table:   table,
		Filters: make(Filters),
	}
}

// Validate performs a sanity check on a DelQuery and its filters
func (q DelQuery) Validate() error {
	if q.Table == "" {
		return errors.NewError("DelQuery has no table")
	}

	for _, f := range q.Filters {
		if err := f.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// Where adds a selection filter to the query to determine which entitie will be deleted.
// returns the query itself for use in builder-style syntax
func (q *DelQuery) Where(prop string, operator string, values ...interface{}) *DelQuery {
	q.Filters[prop] = NewFilter(prop, operator, values...)
	return q
}

// DelResponse represents a response to a DEL query over the protocol, with the usual resonse
// properties, and the number of objects deleted
type DelResponse struct {
	*Response
	Num int `bson:"num"`
}

// NewDelResponse creates a new response to be returned by the protocol with the given error and
// number of objects deleted
func NewDelResponse(err error, num int) *DelResponse {
	return &DelResponse{
		NewResponse(err),
		num,
	}
}
