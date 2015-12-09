package query

import (
	"encoding/json"
	"reflect"

	"github.com/EverythingMe/meduza/client"
	"github.com/EverythingMe/meduza/errors"
	"github.com/EverythingMe/meduza/schema"
)

// GetQuery describes and build a query to get objects from the database.
//
// It includes the selection filters, ordering, paging and fields information.
//
// It is serialized over the wire protocol and passed between the client and the server
type GetQuery struct {
	Table      string   `bson:"table"`
	Properties []string `bson:"properties"`
	Filters    Filters  `bson:"filters"`
	Order      Ordering `bson:"order"`
	Paging     Paging   `bson:"paging"`
}

// NewGetQuery creates a new GetQuery for the given table, with all the other prams.
// The query can be used in a building sequence to add more parameters to it
func NewGetQuery(table string) *GetQuery {
	return &GetQuery{
		Table:      table,
		Properties: make([]string, 0),
		Filters:    make(Filters),
		Paging:     Paging{0, DefaultPagingLimit},
		Order:      Ordering{Ascending: true},
	}
}

// String representation of a query in Json
func (q GetQuery) String() string {

	b, e := json.MarshalIndent(q, "", "  ")
	if e != nil {
		return e.Error()
	}
	return string(b)
}

// Validate makes sure the query's parameters and filters are sane
func (q GetQuery) Validate() error {
	if q.Table == "" {
		return errors.NewError("No table for GET query")
	}

	if q.Filters == nil || len(q.Filters) == 0 {
		return errors.NewError("No filters for GET query")
	}

	for _, f := range q.Filters {
		if err := f.Validate(); err != nil {
			return err
		}
	}

	if err := q.Paging.Validate(); err != nil {
		return err
	}

	if err := q.Order.Validate(); err != nil {
		return err
	}

	return nil
}

// Filter adds a WHERE filter to the query.
// returns the query itself so it can be used in a building sequence.
func (q *GetQuery) Filter(prop, op string, values ...interface{}) *GetQuery {

	q.Filters[prop] = NewFilter(prop, op, values...)

	return q
}

func (q *GetQuery) FilterEq(prop string, value interface{}) *GetQuery {

	q.Filters[prop] = NewFilter(prop, Eq, value)

	return q
}

func (q *GetQuery) FilterBetween(prop string, min, max interface{}) *GetQuery {

	q.Filters[prop] = NewFilter(prop, Between, min, max)

	return q
}

func (q *GetQuery) All() *GetQuery {

	q.Filters[schema.IdKey] = NewFilter(schema.IdKey, All)

	return q
}
func (q *GetQuery) FilterIn(prop string, values ...interface{}) *GetQuery {

	q.Filters[prop] = NewFilter(prop, In, values...)

	return q
}

// Page sets the paging parameters for the query.
// returns the query itself so it can be used in a building sequence.
func (q *GetQuery) Page(offset, limit int) *GetQuery {
	q.Paging = Paging{
		Offset: offset,
		Limit:  limit,
	}

	return q
}

// Limit the GET query to a set of specific fields
func (q *GetQuery) Fields(props ...string) *GetQuery {
	q.Properties = props
	return q
}

type SortMode string

const (
	ASC  SortMode = "ASC"
	DESC SortMode = "DESC"
)

// OrderBy adds ordering information to the query's selection. If it is missing,
// We order by the index this query is based on. Both lexical and numeric ordering are supported (floats, ints or timestamps)
//
// If an ordering directive is present, it must be either the sole property selection index, or the last property of it.
//
// i.e. if you have a in index on (name, birth_date) you can order by birth_date only. This requires careful planning
// of the indexes :)
//
// We do not allow unindexed ordering. If the order parameter is not the last or sole property of the seleciton index,
// the query fails
func (q *GetQuery) OrderBy(prop string, mode SortMode) *GetQuery {

	q.Order = Ordering{
		By:        prop,
		Ascending: mode != DESC,
	}
	return q
}

// Limit sets a limit for the number of results to get, assuming the offset is the first result.
// Use Page(offset, limit) for paging beyond the first resutls.
//
// returns the query itself so it can be used in a building sequence.
func (q *GetQuery) Limit(limit int) *GetQuery {
	q.Paging = Paging{
		Offset: 0,
		Limit:  limit,
	}

	return q
}

// Load executes the query on the client c, and maps the returning results to a model object(s).
//
// If you pass a pointer to a slice of objects, this slice will be filled with objects of this type.
//
// If you pass a pointer to a single object, the result will be mapped to this object only. Note that
// if there are more than 1 results and you pass a single object, Load will return an error.
//
// If the query hasn't succeeded we return its error.
// If there were no results but no error, the slice will be of size 0, and a given object will remain unchanged
func (q GetQuery) Load(c client.Client, dst interface{}) (int, error) {

	res, err := c.Do(q)
	if err != nil {
		return 0, err
	}

	if ret, ok := res.(GetResponse); !ok {
		return 0, errors.NewError("Could not cast %s to get response", reflect.TypeOf(res))
	} else {

		if ret.Error != nil {

			return 0, errors.NewError("Could not load results: %s", ret.Error.Error())
		}
		return ret.Total, ret.MapEntities(dst)
	}

}

type GetResponse struct {
	*Response
	Entities []schema.Entity `bson:"entities"`
	// the total number of entities matching this response, regardless of how many entities we've retrived
	Total int `bson:"total"`
}

func NewGetResponseSize(err error, sizeHint int) *GetResponse {
	return &GetResponse{
		Response: NewResponse(err),
		Entities: make([]schema.Entity, 0, sizeHint),
	}
}

func NewGetResponse(err error) *GetResponse {
	return NewGetResponseSize(err, 0)
}

func (r *GetResponse) AddEntity(e schema.Entity) {
	r.Entities = append(r.Entities, e)
}

func (r GetResponse) MapEntities(dst interface{}) error {

	if len(r.Entities) == 0 {
		return errors.EmptyResult
	}
	dv := reflect.ValueOf(dst)
	var modelType reflect.Type
	if dv.Kind() != reflect.Ptr || dv.IsNil() {
		return errors.NewError("Could not load results into a non pointer or nil pointer")
	}

	modelType = dv.Elem().Type()

	if modelType.Kind() == reflect.Slice {

		modelType = modelType.Elem()
		dv = dv.Elem()
		for _, ent := range r.Entities {

			obj := reflect.New(modelType).Interface()

			if err := schema.DecodeEntity(ent, obj); err != nil {
				return errors.NewError("Could not map entity %s: %s", ent, err)
			}

			dv.Set(reflect.Append(dv, reflect.ValueOf(obj).Elem()))

		}
	} else {
		if len(r.Entities) != 1 {
			return errors.NewError("Cannot load multiple entities into a single object, pass a slice please")
		}
		return schema.DecodeEntity(r.Entities[0], dst)
	}
	return nil
}
