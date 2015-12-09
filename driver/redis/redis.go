package redis

import (
	"sync"
	"time"

	"github.com/dvirsky/go-pylog/logging"
	"github.com/garyburd/redigo/redis"
	"github.com/EverythingMe/meduza/driver"
	"github.com/EverythingMe/meduza/errors"
	"github.com/EverythingMe/meduza/query"
	"github.com/EverythingMe/meduza/schema"
	"golang.org/x/text/language"
)

// connection pool for dealing with redis
var pool *redis.Pool

// data encoder for encoding data to redis
var encoder schema.Encoder

// data deocder to decode data coming from redis into primitive types
var decoder schema.Decoder

func init() {

	encoder = Encoder{}

	decoder = Decoder{}
}

var normalizerPool = sync.Pool{
	New: func() interface{} {
		return schema.NewNormalizer(language.Und, true, false)
	},
}

func getNormalizer() schema.TextNormalizer {
	return normalizerPool.Get().(schema.TextNormalizer)
}

func putNormalizer(n schema.TextNormalizer) {
	if n != nil {
		normalizerPool.Put(n)
	}
}

// Driver is the driver implementation over a redis data store
type Driver struct {
	tableLock sync.RWMutex
	tables    map[string]*table
	schemas   map[string]*schema.Schema
}

// NewDriver creates a new redis driver instance
func NewDriver() *Driver {
	return &Driver{
		tables:  make(map[string]*table),
		schemas: make(map[string]*schema.Schema),
	}
}

func initPool(config Config) {

	timeout := time.Duration(config.Timeout) * time.Millisecond
	pool = &redis.Pool{
		MaxIdle:     10,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {

			c, err := redis.DialTimeout(config.Network, config.Addr, timeout, timeout, timeout)

			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, pooledTime time.Time) error {

			// for connections that were idle for over a second, let's make sure they can still talk to redis before doing anything with them
			if time.Since(pooledTime) > time.Second {
				_, err := c.Do("PING")
				return err
			}
			return nil
		},
	}
}

func (r *Driver) getTable(name string) (*table, bool) {
	r.tableLock.RLock()
	defer r.tableLock.RUnlock()

	t, f := r.tables[name]
	if !f {
		logging.Warning("Non existing table name: %s", name)
	}
	return t, f
}

func (r *Driver) newTable(desc schema.Table) (*table, error) {
	tbl := &table{
		desc:    desc,
		indexes: make([]index, 0, len(desc.Indexes)),
	}

	for _, idx := range desc.Indexes {
		logging.Debug("Creating index %s (type %s) on table %s", idx.Name, idx.Type, desc.Name)
		tbl.AddIndex(idx)
	}

	if desc.Primary == nil {
		tbl.primary = newRandomPrimary(desc.Primary, tbl)
	} else {
		switch desc.Primary.Type {
		case schema.PrimaryCompound:
			tbl.primary = newCompounPrimary(desc.Primary, tbl)
		case schema.PrimaryRandom:
			tbl.primary = newRandomPrimary(desc.Primary, tbl)
		default:
			return nil, errors.NewError("Unknown primary type: %s", desc.Primary.Type)
		}
	}

	return tbl, nil
}

// The minimal frequency for the repair loop. We do this because we don't want to take up too much CPU
// time on repair loops
const MinRepairFrequency = 10 //ms

// Init initializes and configures the redis driver
func (r *Driver) Init(sp schema.SchemaProvider, config interface{}) error {

	conf, ok := config.(Config)
	if !ok {
		return errors.NewError("Invalid configuration provided")
	}

	initPool(conf)

	DefaultConfig = conf

	encoder = NewEncoder(conf.TextCompressThreshold)

	for _, sc := range sp.Schemas() {
		r.handleSchema(sc)
	}

	go r.monitorChanges(sp)

	if conf.Master && conf.RepairEnabled {

		if conf.RepairFrequency < MinRepairFrequency {
			conf.RepairFrequency = MinRepairFrequency
		}

		logging.Info("Running repair loop, frequency %dms", conf.RepairFrequency)

		go r.repairLoop(time.Duration(conf.RepairFrequency) * time.Millisecond)
	}

	return nil
}

// handleSchema takes a schema spec and breaks it into tables, putting them in the table scec of the driver
func (r *Driver) handleSchema(sc *schema.Schema) error {

	for _, desc := range sc.Tables {
		logging.Debug("Creating table %s on schema %s", desc.Name, sc.Name)
		tbl, err := r.newTable(*desc)
		if err != nil {
			logging.Error("Could not load schema into redis driver - bad schema: %s", err)
			return err
		}

		r.tableLock.Lock()
		r.tables[desc.Name] = tbl
		r.tableLock.Unlock()

	}

	r.schemas[sc.Name] = sc
	return nil

}

// Put executes a PUT query on the driver, inserting/updating one or more entities
func (r *Driver) Put(q query.PutQuery) *query.PutResponse {
	ret := query.NewPutResponse(nil)
	defer ret.Done()

	if tbl, found := r.getTable(q.Table); !found {
		ret.Error = errors.InvalidTableError
	} else {

		ids, err := tbl.Put(q.Entities...)
		ret.Error = errors.Wrap(err)
		ret.Ids = ids

	}

	return ret
}

// Get executes a GET query on the driver, selecting any number of entities
func (r *Driver) Get(q query.GetQuery) *query.GetResponse {

	ret := query.NewGetResponse(nil)
	defer ret.Done()
	if tbl, found := r.getTable(q.Table); !found {
		ret.Error = errors.InvalidTableError
	} else {
		tbl.Get(q, ret)
	}
	return ret

}

// Dump a specific table's records as a stream of entities.
//
// The function also returns a channel for erros that may occur, and a bool channel allowing its caller to
// stop it if an error has happened upstream
func (r *Driver) Dump(table string) (<-chan schema.Entity, <-chan error, chan<- bool, error) {
	if tbl, found := r.getTable(table); !found {
		return nil, nil, nil, errors.InvalidTableError
	} else {

		chunkSize := 50

		idch, stopch := tbl.primary.Scan(chunkSize)

		chunk := make([]schema.Key, chunkSize)

		ch := make(chan schema.Entity)
		errch := make(chan error)
		rstopch := make(chan bool)
		go func() {

			defer close(ch)
			i := 0

			for id := range idch {

				chunk[i] = id
				i++

				if i == chunkSize {

					//logging.Debug("Dumping keys %v", chunk)

					i = 0

					ents, err := tbl.load(chunk)
					if err != nil {
						logging.Error("error loading entities for dumping: %s", err)
						stopch <- true
						errch <- err
						return
					}

					for _, ent := range ents {
						select {
						case ch <- ent:
						case <-rstopch:
							logging.Info("Stopping iteration from caller")
							stopch <- true
							return
						}
					}

				}
			}

			if i > 0 {

				ents, err := tbl.load(chunk[:i])
				if err != nil {
					logging.Error("error loading entities for dumping: %s", err)
					errch <- err
				}

				for _, ent := range ents {
					ch <- ent
				}
			}

			errch <- nil
		}()

		return ch, errch, rstopch, nil

	}

}

// Delete executes a DEL query on the driver, deleting entities based on filter criteria
func (r *Driver) Delete(q query.DelQuery) *query.DelResponse {
	ret := query.NewDelResponse(nil, 0)
	defer ret.Done()

	if tbl, found := r.getTable(q.Table); !found {
		ret.Error = errors.InvalidTableError
	} else {
		num, err := tbl.Delete(q.Filters)
		ret.Error = errors.Wrap(err)
		ret.Num = num
	}
	return ret
}

// Update executes an UPDATE query on the driver, performing a series of changes on entities specified
// by a set of filters
func (r *Driver) Update(q query.UpdateQuery) *query.UpdateResponse {
	ret := query.NewUpdateResponse(nil, 0)
	defer ret.Done()
	if tbl, found := r.getTable(q.Table); !found {
		ret.Error = errors.InvalidTableError
	} else {
		num, err := tbl.Update(q)
		ret.Error = errors.Wrap(err)
		ret.Num = num
	}
	return ret

}

// Status returns an error if the driver is not properly running and has at least one schema active
func (r *Driver) Status() error {

	if len(r.schemas) == 0 {
		return errors.NewError("redis driver: no loaded schema")
	}
	if len(r.tables) == 0 {
		return errors.NewError("redis driver: no loaded table")
	}

	conn := pool.Get()
	if conn == nil {
		return errors.NewError("redis driver: no connection to server")
	}

	defer conn.Close()

	_, err := conn.Do("PING")
	if err != nil {
		return errors.NewError("redis driver: cannot ping server: %s", err)
	}

	return nil

}

func (r *Driver) monitorChanges(sp schema.SchemaProvider) {

	logging.Debug("Monitoring schema changes in redis driver")
	ch, err := sp.Updates()
	if err != nil {
		logging.Error("Cannot monitor changes in schema provider: %s", err)
		return
	}

	for sc := range ch {
		if sc != nil {
			logging.Info("Detected change in schema: %s", sc.Name)

			r.handleSchema(sc)
		}

	}
}

const SampleSize = 100

func (r *Driver) Stats() (*driver.Stats, error) {

	ret := &driver.Stats{
		Tables: make(map[string]*driver.TableStats),
	}

	var err error
	for _, tbl := range r.tables {
		if ret.Tables[tbl.desc.Name], err = tbl.Stats(SampleSize); err != nil {
			logging.Error("Error sampling data: %s", err)
		}

	}

	return ret, nil

}
