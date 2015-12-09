package driver

import (
	"github.com/dustin/go-humanize"
	"github.com/EverythingMe/meduza/query"
	"github.com/EverythingMe/meduza/schema"
)

type Driver interface {
	Init(sp schema.SchemaProvider, config interface{}) error
	Get(q query.GetQuery) *query.GetResponse
	Update(q query.UpdateQuery) *query.UpdateResponse
	Put(q query.PutQuery) *query.PutResponse
	Delete(q query.DelQuery) *query.DelResponse

	Dump(table string) (<-chan schema.Entity, <-chan error, chan<- bool, error)

	// Status asks the driver if it is up and running
	Status() error
	Stats() (*Stats, error)
	//Transaction() Error
	//Commit() Error
	//Abort()
}

type Counter int
type ByteCounter int

func (c Counter) MarshalYAML() (interface{}, error) {
	return humanize.Comma(int64(c)), nil
}

func (c ByteCounter) MarshalYAML() (interface{}, error) {
	return humanize.Bytes(uint64(c)), nil
}

type TableStats struct {
	NumRows           Counter     `yaml:"num_rows"`
	EstimatedDataSize ByteCounter `yaml:"data_size"`
	EstimatedKeysSize ByteCounter `yaml:"keys_size"`
}

type Stats struct {
	Tables map[string]*TableStats `yaml:"tables"`
}
