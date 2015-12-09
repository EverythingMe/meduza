package main

import (
	"encoding/gob"
	"fmt"
	"io"

	"github.com/EverythingMe/meduza/driver"
	"github.com/EverythingMe/meduza/query"

	"github.com/dvirsky/go-pylog/logging"

	"github.com/EverythingMe/meduza/schema"
)

var registered = false

func registerGobStuff() {

	if !registered {
		registered = true
		//gob.Register(schema.Entity{})
		gob.RegisterName("x", schema.Text(""))
		gob.RegisterName("b", schema.Binary(""))
		gob.RegisterName("i", schema.Int(0))
		gob.RegisterName("u", schema.Uint(0))
		gob.RegisterName("f", schema.Float(0))
		gob.RegisterName("k", schema.Key(""))
		gob.RegisterName("s", schema.Set{})
		gob.RegisterName("l", schema.List{})
		gob.RegisterName("m", schema.Map{})
		gob.RegisterName("t", schema.Timestamp{})
	}

}

// dump a table from a driver into the output writer
func dump(drv driver.Driver, out io.Writer, schm, table string) error {
	registerGobStuff()

	enc := gob.NewEncoder(out)

	ch, errch, stopch, err := drv.Dump(fmt.Sprintf("%s.%s", schm, table))
	if err != nil {
		return err
	}

	ok := true

	i := 0
	for ok {
		select {
		case ent := <-ch:

			if err := enc.Encode(ent); err != nil {
				logging.Error("Error encoding :%s", err)
				logging.Info("Stopping")
				stopch <- true
				return err
			}
			i++

			if i%10000 == 0 {
				logging.Info("Dumped %d entities for %s.%s", i, schm, table)
			}

		case err := <-errch:
			if err != nil {
				return err
			}
			ok = false
		}
	}
	logging.Info("finished! %d", i)

	return nil
}

// load a dump of properties into the driver, for a given schema and table
func loadDump(drv driver.Driver, in io.Reader, schm, table string) error {
	registerGobStuff()

	dec := gob.NewDecoder(in)

	var err error

	chunk := 50

	pq := query.NewPutQuery(fmt.Sprintf("%s.%s", schm, table))
	num := 0

	for err == nil {
		var ent schema.Entity
		err = dec.Decode(&ent)

		if err == nil {
			pq.AddEntity(ent)
		} else if err != io.EOF {
			return logging.Errorf("Error decoding entity: %s", err)
		}

		if len(pq.Entities) == chunk || err == io.EOF {

			if len(pq.Entities) > 0 {
				pr := drv.Put(*pq)
				if pr.Err() != nil {
					logging.Error("Error loading dump: %s", pr.Err())
					return pr.Err()
				}
				num += len(pq.Entities)

				if num%10000 == 0 {
					logging.Info("Loaded %d entities for %s.%s", num, schm, table)
				}
				pq.Entities = make([]schema.Entity, 0, chunk)
			}
		}

	}
	logging.Info("Loaded %d entities to %s.%s", num, schm, table)

	return nil
}
