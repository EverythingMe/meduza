package query

import (
	"github.com/EverythingMe/meduza/errors"
	"github.com/EverythingMe/meduza/schema"
)

type PutQuery struct {
	Table    string          `bson:"table"`
	Entities []schema.Entity `bson:"entities"`
}

func NewPutQuerySize(table string, capacity int) *PutQuery {
	return &PutQuery{
		Table:    table,
		Entities: make([]schema.Entity, 0, capacity),
	}
}

func NewPutQuery(table string) *PutQuery {
	return NewPutQuerySize(table, 1)
}

func (q PutQuery) Validate() error {
	if q.Table == "" {
		return errors.NewError("No table for PUT query")
	}

	if q.Entities == nil || len(q.Entities) == 0 {
		return errors.NewError("PUT query does not contain entities")
	}

	for _, ent := range q.Entities {
		if err := ent.Validate(); err != nil {
			return err
		}
	}

	return nil
}

func (q *PutQuery) AddEntity(e schema.Entity) *PutQuery {
	q.Entities = append(q.Entities, e)
	return q
}

type PutResponse struct {
	*Response
	Ids []schema.Key `bson:"ids"`
}

func NewPutResponse(err error, ids ...schema.Key) *PutResponse {
	return &PutResponse{
		NewResponse(err),
		ids,
	}
}
