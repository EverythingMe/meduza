package redis

import (
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/dvirsky/go-pylog/logging"
	"github.com/garyburd/redigo/redis"
	"github.com/EverythingMe/meduza/schema"
)

func (r *Driver) repairLoop(freq time.Duration) {
	logging.Info("Starting redis repair loop")

	go r.repairTables(freq)

	r.repairEntities(freq)

}

// repairEntities selects random entites from redis and re-indexes them
func (r *Driver) repairEntities(freq time.Duration) {
	for _ = range time.Tick(freq) {
		conn := pool.Get()
		k, err := redis.String(conn.Do("RANDOMKEY"))

		if err != nil {
			logging.Error("Error reading random key: %s", err)
			continue
		}

		if id, table, schem, err := extractEntityId(k); err == nil && id != "" {

			// if we found entities of a deleted table
			if _, found := r.getTable(fmt.Sprintf("%s.%s", schem, table)); !found {
				logging.Warning("Deleting key from non existent table: %s", k)
				if _, err = conn.Do("DEL", k); err != nil {
					logging.Error("Could not delete dead entity from redis: %s", err)
				}
				continue
			}

			r.reindexEntity(schem, table, id)
		} else if err != errNoMatch {

			logging.Error("Error extracting entity id %s: %s", k, err)
		}

	}
}

// repairTables iterates the indexes of tables and tries to repair holes in them
func (r *Driver) repairTables(freq time.Duration) {

	for _ = range time.Tick(freq) {

		// extract current table map
		r.tableLock.RLock()
		tables := make([]*table, 0, len(r.tables))
		for _, t := range r.tables {
			tables = append(tables, t)
		}
		r.tableLock.RUnlock()

		for _, t := range tables {
			t.repair(freq)
		}

	}

}

var entityRegex = regexp.MustCompile("^([a-zA-Z][a-zA-Z0-9_]+)\\.([a-zA-Z][a-zA-Z0-9_]+)\\:([^:].+)$")

var errNoMatch = errors.New("no match")

// extractEntityId takes a raw redis key, and if it matches the above regex ^^^^ it breaks it down
// to schema/table/key, so it can be repaired if needed.
func extractEntityId(key string) (id schema.Key, table, schem string, err error) {
	m := entityRegex.FindAllStringSubmatch(key, 3)

	if len(m) != 1 || len(m[0]) != 4 {
		err = errNoMatch
		return
	}

	id = schema.Key(m[0][3])
	schem = m[0][1]
	table = m[0][2]
	logging.Debug("Key %s split into %s/%s/%s", key, schem, table, id)
	return
}

// reindexEntity loads an entity, and makes sure it is properly indexed for all its keys
func (r *Driver) reindexEntity(schem, table string, id schema.Key) {

	t, found := r.getTable(fmt.Sprintf("%s.%s", schem, table))
	if !found {
		logging.Info("Table not found %s.%s Deleting", schem, table)
	}

	ents, err := t.load([]schema.Key{id})
	if err != nil {
		logging.Error("%#v", err)
		return
	}

	if len(ents) == 0 {
		logging.Error("Entity not found for index %s: %s", t.primary, id)
	} else {
		logging.Debug("Reindexing entity %v", id)
		if err := t.reindex(ents...); err != nil {
			logging.Error("Error reindexing entity: %s", err)
		}
	}

}

// repair walks over the table's indexes and makes sure there are no dangling references to non-existent keys
func (t *table) repair(freq time.Duration) {

	idch, stopch := t.primary.Scan(10)
	_ = stopch
	logging.Info("Repairing table %s", t)
	for id := range idch {

		ents, err := t.load([]schema.Key{id})
		if err != nil {
			logging.Error("%#v", err)
			continue
		}

		if len(ents) == 0 {
			logging.Info("Entity not found for index %s: %s", t.primary, id)
			t.primary.Unindex(id)
		}
		time.Sleep(freq)

	}

	idxs := t.indexes[:]
	for _, idx := range idxs {

		idch, stopch := idx.RawEntries(10)
		_ = stopch
		for rawId := range idch {
			id := extractId(rawId)
			if !id.IsNull() {
				ents, err := t.load([]schema.Key{id})
				if err != nil {
					logging.Error("%#v", err)
					continue
				}

				if len(ents) == 0 {
					logging.Info("Entity not found for index %s: %s", idx, id)
					idx.RemoveEntry(rawId)
				}
				time.Sleep(freq)
			}

		}

	}

}
