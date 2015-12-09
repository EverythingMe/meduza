// Package redis provides a redis based schema provider and deployer
package redis

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/dvirsky/go-pylog/logging"
	"github.com/garyburd/redigo/redis"
	"github.com/EverythingMe/meduza/errors"
	"github.com/EverythingMe/meduza/schema"
)

const redisKey = "__mdz_schemas__"
const pubsubKey = "__mdz_updates__"

// Provider implements a schema provider with updates, based on a redis HASH storing all schemas, and
// a redis PUBSUB channel for updates
type Provider struct {
	addr string
	net  string

	schemas map[string]*schema.Schema
}

// NewProvider creates a new provider for a given redis server in net/addr
func NewProvider(net, addr string) *Provider {
	return &Provider{
		addr:    addr,
		net:     net,
		schemas: make(map[string]*schema.Schema),
	}
}

// Init connects to the redis server and reads all the schemas in its schemas HASH key.
func (p *Provider) Init() error {

	conn, err := redis.Dial(p.net, p.addr)
	if err != nil {
		return errors.NewError("Could not connect to schema redis: %s", err)
	}
	defer conn.Close()

	keys, err := redis.Strings(conn.Do("HKEYS", redisKey))
	if err != nil {
		return errors.NewError("Could not get schemas from redis: %s", err)
	}

	for _, k := range keys {

		sc, err := p.load(k, conn)
		if err != nil {
			logging.Error("Could not parse schema %s: %s", k, err)
			continue
		}
		logging.Info("Successfully parsed schema %s", k)
		p.schemas[k] = sc

	}
	return nil
}

// Schemas returns a list of all the currently read schemas
func (p *Provider) Schemas() []*schema.Schema {

	ret := make([]*schema.Schema, 0, len(p.schemas))
	for _, v := range p.schemas {
		ret = append(ret, v)
	}
	return ret
}

// load loads a single schema from redis
func (p *Provider) load(name string, conn redis.Conn) (sc *schema.Schema, err error) {
	defer func() {
		e := recover()
		if e != nil {
			logging.Error("Panic recovered: %s", e)
			err = e.(error)
		}

	}()
	if conn == nil {
		conn, err = redis.Dial(p.net, p.addr)
		if err != nil {
			return nil, err
		}
		defer conn.Close()
	}

	logging.Info("Trying to load schema from redis hash %s", name)
	val, err := redis.String(conn.Do("HGET", redisKey, name))
	if err != nil {
		logging.Error("Error loading schema from redis: %s", err)
		return nil, errors.NewError("Could not load schema data from redis: %s", err)
	}

	return schema.Load(strings.NewReader(val))

}

// Updates registers to a pubsub redis channel on schema updates, and fires changed schemas into the returned channel
func (p *Provider) Updates() (<-chan *schema.Schema, error) {

	conn, err := redis.Dial(p.net, p.addr)
	if err != nil {
		return nil, errors.NewError("Could not connect to schema redis: %s", err)
	}

	ch := make(chan *schema.Schema)

	go func() {
		for {

			if conn == nil {
				if conn, err = redis.Dial(p.net, p.addr); err != nil {
					logging.Error("Error connecting to pubsub: %s", err)
				}
			} else {

				psc := redis.PubSubConn{conn}
				err := psc.Subscribe(pubsubKey)
				if err != nil {
					logging.Error("Could not subscribe: %s", err)
					time.Sleep(100 * time.Microsecond)
					conn.Close()
					conn = nil
					continue
				}

				for err == nil {
					switch v := psc.Receive().(type) {
					case redis.Message:
						logging.Info("Got an update for schema %s", v.Data)
						sc, err := p.load(string(v.Data), nil)
						logging.Info("Loading schema result: %s, %#v", err, sc)
						p.schemas[sc.Name] = sc
						if sc != nil && err == nil {
							ch <- sc
						}
					case redis.Subscription:
						logging.Debug("Subscription %s: %s %d\n", v.Channel, v.Kind, v.Count)
					case error:
						logging.Error("Error reading pubsub: %s", v)
						conn.Close()
						conn = nil
						err = v
						break
					}

				}
			}

			// sleep before retrying
			time.Sleep(100 * time.Millisecond)
		}

	}()

	return ch, nil
}

// Stop actually does nothing in this provider
func (p *Provider) Stop() {

}

// Deployer implements a schema deployer to our schema redis server
type Deployer struct {
	addr string
	net  string
}

// NewDeployer creates a new deployer for the given redis net/addr
func NewDeployer(net, addr string) Deployer {
	return Deployer{
		addr, net,
	}
}

// Deploy deploys the data in reader r under the name name - if it is a valid schema.
// If it succeeds, it publishes a message to the provider's pubsub channel
func (d Deployer) Deploy(r io.Reader) error {

	// connect to redis
	conn, err := redis.Dial(d.net, d.addr)
	if err != nil {
		return errors.NewError("Could not connect to schema redis: %s", err)
	}
	defer conn.Close()

	// read all the contents
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return errors.NewError("Could not read schema: %s", err)
	}
	var sc *schema.Schema
	// make sure the schema is fine before loading it
	if sc, err = schema.Load(bytes.NewReader(b)); err != nil {
		return errors.NewError("Could not deploy schema - parsing error: %s", err)
	}

	if _, err = conn.Do("HSET", redisKey, sc.Name, b); err != nil {
		return errors.NewError("Could not save schema in redis: %s", err)
	}

	_, err = conn.Do("PUBLISH", pubsubKey, sc.Name)
	return err
}

// DeployUri wraps Deploy with a URI of a schema file, reads it and loads it.
// It currently supports only deployment from local files
//
// TODO: Consider removing this completely
func (d Deployer) DeployUri(uri string) error {
	u, err := url.Parse(uri)
	if err != nil {
		return errors.NewError("Could not parse schema uri: %s", err)
	}

	if u.Scheme == "file" {

		fp, err := os.Open(path.Join(u.Host, u.Path))
		if err != nil {
			return errors.NewError("Could not open schema file %s: %s", u.Path, err)
		}

		defer fp.Close()
		return d.Deploy(fp)
	}

	return errors.NewError("Illegal Uri scheme: %s", uri)

}
