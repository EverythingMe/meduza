package mock

import (
	"code.google.com/p/go-uuid/uuid"
	"github.com/EverythingMe/meduza/driver"
	"github.com/EverythingMe/meduza/query"
	"github.com/EverythingMe/meduza/schema"
)

type MockDriver struct{}

func (MockDriver) Init(sc schema.SchemaProvider, config interface{}) error {
	return nil
}
func (MockDriver) ReserveId() schema.Key {
	return schema.Key(uuid.New())

}

func (MockDriver) Stats() (*driver.Stats, error) {
	return &driver.Stats{}, nil

}

func (d MockDriver) Get(q query.GetQuery) *query.GetResponse {
	ret := query.NewGetResponse(nil)
	defer ret.Done()

	ret.AddEntity(*schema.NewEntity(d.ReserveId(), schema.NewText("foo", "bar")))
	return ret

}

func (d MockDriver) Put(q query.PutQuery) *query.PutResponse {
	return query.NewPutResponse(nil, d.ReserveId())
}

func (MockDriver) Delete(q query.DelQuery) *query.DelResponse {
	return query.NewDelResponse(nil, 0)
}

func (MockDriver) Update(q query.UpdateQuery) *query.UpdateResponse {
	return query.NewUpdateResponse(nil, 0)
}

func (MockDriver) Status() error {
	return nil
}

func (MockDriver) Dump(table string) (<-chan schema.Entity, <-chan error, chan<- bool, error) {
	return nil, nil, nil, nil
}
