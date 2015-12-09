package meduza

import (
	"fmt"
	"reflect"
	"time"

	"github.com/EverythingMe/meduza/client"
	"github.com/EverythingMe/meduza/errors"
	"github.com/EverythingMe/meduza/query"
	"github.com/EverythingMe/meduza/schema"
	"github.com/dvirsky/go-pylog/logging"
)

// Various ORM-like convenience functions

// Session represents a meduza session, connecting to a set of servers and a specific schema
type Session struct {
	pool   *client.Pool
	Schema string
}

// Get loads an object or many objects by primary ids only.
//
// dst must be a pointer to a slice of matching model objects for multiple ids,
// or a pointer to a single model object for a single id.
//
// If the ids do not exist in the database, an EmptyResult error is returned
func (s Session) Get(table string, dst interface{}, ids ...schema.Key) error {

	client, err := s.pool.Get()
	if err != nil {
		return err
	}

	qids := make([]interface{}, len(ids))
	for i := range ids {
		qids[i] = ids[i]
	}
	_, err = query.NewGetQuery(s.qualifiedName(table)).FilterIn("id", qids...).Load(client, dst)
	return err
}

// Setup initializes a default session for the default schema, and exposes the redis LB to all sessions
func Setup(defaultSchema string, dialer client.Dialer) {

	DefaultSession = NewSession(defaultSchema, dialer)
}

// NewSession creates a new Session object for a specific schema with a given dialer
func NewSession(schm string, dialer client.Dialer) *Session {

	return &Session{
		pool:   client.NewPool(dialer),
		Schema: schm,
	}

}

func (s Session) qualifiedName(table string) string {
	return fmt.Sprintf("%s.%s", s.Schema, table)
}

// Select loads objects from a table based on filtering secondary keys.
//
// dst must be a pointer to a slice of matching model objects for multiple results,
// or a pointer to a single model object for a single result.
// Since it is unknown how many results will return, it is recommended to use only slices here
//
// If no results are found, an EmptyResult error is returned.
// If the result was successful, we return the total number of records for this selection
func (s Session) Select(table string, dst interface{}, offset, limit int, filters ...query.Filter) (int, error) {

	client, err := s.pool.Get()
	if err != nil {
		return 0, err
	}

	if len(filters) == 0 {
		return 0, errors.NewError("No filters given to query.")
	}

	q := query.NewGetQuery(s.qualifiedName(table)).Page(offset, limit)
	q.Filters = query.NewFilters(filters...)

	return q.Load(client, dst)

}

// Put saves a list of objects into the database in a batch, and returns the resulting ids of the operation.
//
// If the objects' id field is empty we will allocate ids for them and return these ids.
//
// If the ids are set and valid, we will accept them as-is. meaning:
//
// 1. If no such id exists, this is considered an "insert"
// 2. If such an id exists, this is an "update" that will overwrite any existing properties of the db's
// stored entity under this id.
func (s Session) Put(table string, objects ...interface{}) ([]schema.Key, error) {
	return s.PutExpiring(table, 0, objects...)
}

// PutExpiring is identical to Put but expires all entities with a given TTL
func (s Session) PutExpiring(table string, ttl time.Duration, objects ...interface{}) ([]schema.Key, error) {

	client, err := s.pool.Get()
	if err != nil {
		return nil, err
	}

	q := query.NewPutQuerySize(s.qualifiedName(table), len(objects))

	for _, obj := range objects {

		ent, err := schema.EncodeStruct(obj)
		if err != nil {
			return nil, logging.Errorf("Could not save object %s: %s", obj, err)
		}

		// Expire the entities if an expire was set
		if ttl > 0 {
			ent.Expire(ttl)
		}
		q.AddEntity(*ent)
	}

	res, err := client.Do(q)
	if err != nil {
		return nil, errors.NewError("Could not perform  %s", err)
	}

	resp, ok := res.(query.PutResponse)
	if !ok {
		return nil, errors.NewError("Invalid response object for  %s", res)
	}

	if len(resp.Ids) != len(objects) {
		return nil, errors.NewError("Not all ids returned, expected %d but got %d", len(objects), len(resp.Ids))
	}

	for i, id := range resp.Ids {

		if reflect.ValueOf(objects[i]).Kind() == reflect.Ptr {
			err := schema.SetPrimary(id, objects[i])
			if err != nil {
				logging.Error("Error mapping if to object: %s", err)
			}
		}
	}
	return resp.Ids, nil

}

// Update performs an update on a table, making the specified changes (setting fields) to entities
// selected by the where filters.
//
// It returns the number of affected rows, or an error if something went wrong
func (s Session) Update(table string, where query.Filters, changes ...query.Change) (int, error) {

	if where == nil || len(where) == 0 {
		return 0, errors.NewError("No selection supplied for query")
	}

	client, err := s.pool.Get()
	if err != nil {
		return 0, err
	}

	q := query.NewUpdateQuery(s.qualifiedName(table))
	q.Filters = where
	q.Changes = changes

	res, err := client.Do(q)
	if err != nil {
		return 0, errors.NewError("Could not perform  %s", err)
	}

	if resp, ok := res.(query.UpdateResponse); ok {
		return resp.Num, nil
	}
	return 0, errors.NewError("Invalid response object for update  %s", res)

}

// Delete deletes entities from a table, based on the where filters.
// It returns the number of deleted rows, or an error if something went wrong
func (s Session) Delete(table string, where ...query.Filter) (int, error) {

	if where == nil || len(where) == 0 {
		return 0, errors.NewError("No selection supplied for query")
	}

	client, err := s.pool.Get()
	if err != nil {
		return 0, err
	}

	q := query.NewDelQuery(s.qualifiedName(table))
	q.Filters = query.NewFilters(where...)

	if err := q.Validate(); err != nil {
		return 0, err
	}

	res, err := client.Do(q)
	if err != nil {
		return 0, errors.NewError("Could not perform  %s", err)
	}

	if resp, ok := res.(query.DelResponse); ok {
		return resp.Num, nil
	}
	return 0, errors.NewError("Invalid response object for DEL  %s", res)

}

// DefaultSession is the sessions that all static calls operate on
var DefaultSession *Session

// Select performs a Select query on the default session. See Session.Select
func Select(table string, dst interface{}, offset, limit int, filters ...query.Filter) (int, error) {
	return DefaultSession.Select(table, dst, offset, limit, filters...)
}

// Get performs a Get query on the default session. See Session.Get
func Get(table string, dst interface{}, ids ...schema.Key) error {
	return DefaultSession.Get(table, dst, ids...)
}

// Put performs a Put query on the default session. See Session.Put
func Put(table string, objects ...interface{}) ([]schema.Key, error) {
	return DefaultSession.Put(table, objects...)
}

// Delete performs a Delete query on the default session. See Session.Delete
func Delete(table string, where ...query.Filter) (int, error) {
	return DefaultSession.Delete(table, where...)
}

// Update performs an update on the Default Session. See Session.Update
func Update(table string, where query.Filters, changes ...query.Change) (int, error) {
	return DefaultSession.Update(table, where, changes...)
}

func init() {

}
