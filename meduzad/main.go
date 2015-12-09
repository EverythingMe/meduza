package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"

	"github.com/EverythingMe/disposable-redis"
	"github.com/EverythingMe/gofigure"
	"github.com/EverythingMe/gofigure/autoflag"
	"github.com/EverythingMe/meduza/driver"
	"github.com/EverythingMe/meduza/driver/redis"
	"github.com/EverythingMe/meduza/protocol/bson"
	"github.com/EverythingMe/meduza/schema"
	redis_schema "github.com/EverythingMe/meduza/schema/provider/redis"
	"github.com/EverythingMe/meduza/transport/resp"
	"github.com/dvirsky/go-pylog/logging"
	"github.com/dvirsky/go-pylog/logging/scribe"
)

type Meduza struct {
	drv driver.Driver
	sp  schema.SchemaProvider
	srv *resp.Server
	sd  schema.Deployer
}

func NewMeduza() *Meduza {

	proto := bson.BsonProtocol{}
	mdz := new(Meduza)
	mdz.sp = redis_schema.NewProvider(config.SchemaRedis.Network, config.SchemaRedis.Addr)
	mdz.drv = redis.NewDriver()
	mdz.srv = resp.NewServer(mdz.drv, proto)
	mdz.sd = redis_schema.NewDeployer(config.SchemaRedis.Network, config.SchemaRedis.Addr)

	return mdz
}

// Status self-checks that everything is fine with this server, and returns
// an error describing the problem if something is wrong
func (m *Meduza) Status() error {

	// TODO: Add more checks here
	return m.drv.Status()

}

func (m *Meduza) Stats() (*driver.Stats, error) {
	return m.drv.Stats()
}

func (m *Meduza) Dump(schm, table string, out io.Writer) error {

	return dump(m.drv, out, schm, table)

}

func (m *Meduza) LoadDump(schm, table string, in io.Reader) error {

	return loadDump(m.drv, in, schm, table)

}

// Start initializes all of the meduza components, and starts listening
func (m *Meduza) Start() error {

	logging.Info("Initializing schema provider")
	err := m.sp.Init()
	if err != nil {
		return err
	}

	logging.Info("Initializing redis driver")
	if err = m.drv.Init(m.sp, config.Redis); err != nil {
		return err
	}

	ListenCtl(config.Server.CtlListen)

	logging.Info("Starting Meduza server")
	if err = m.srv.Listen(config.Server.Listen); err != nil {
		return err
	}

	return nil

}

var meduzaServer *Meduza

// initStatsd configures and re-inits the statsd instrumentor
func initStatsd() {

	conf := config.Statsd
	logging.Info("Reconfiguring statsd, %#v", conf)
	if conf.Enabled {
		logging.Info("initializing statsd at %s", conf.Address)
	}
}

// initLogging initializes the logger and scribe client
func initLogging() {
	logging.Info("Reconfiguring logging, %#v", config.Server)
	logging.SetMinimalLevelByName(config.Server.LoggingLevel)
	if config.Scribe.Enabled {
		handler := scribe.NewScribeLogger(config.Scribe.Address, config.Scribe.Category,
			config.Scribe.BufferSize, os.Stderr)
		logging.SetHandler(handler)
	}
}

func configReload() {

	if err := autoflag.Load(gofigure.DefaultLoader, &config); err != nil {
		logging.Error("Error loading configs: %v", err)
	} else {
		initLogging()
		initStatsd()
	}

}
func main() {

	var testMode bool
	var port int
	var ctlPort int
	flag.BoolVar(&testMode, "test", false, "If set, we start meduza for testing with an ephemeral redis instance")
	flag.IntVar(&port, "port", 0, "If set, override the listening port in the configs. Used for testing")
	flag.IntVar(&ctlPort, "ctl_port", 0, "If set, override the CTL listening port in the configs. Used for testing")

	if err := autoflag.Load(gofigure.DefaultLoader, &config); err != nil {
		logging.Error("Error loading configs: %v", err)
	}

	if port > 0 {
		config.Server.Listen = fmt.Sprintf(":%d", port)
	}

	if ctlPort > 0 {
		config.Server.CtlListen = fmt.Sprintf(":%d", ctlPort)
	}

	if testMode {

		logging.Info("Starting in testing mode")

		srv, err := disposable_redis.NewServerRandomPort()
		if err != nil {
			panic(err)
		}

		config.Redis.Network = "tcp"
		config.Redis.Addr = srv.Addr()
		config.SchemaRedis.Network = "tcp"
		config.SchemaRedis.Addr = srv.Addr()

		defer func() {
			srv.Stop()
			fmt.Println("Stopped disposable redis")
		}()
	}

	// Init statsd
	initStatsd()

	// Init Logging
	initLogging()

	// wait for SIGHUP to reload configs
	m := gofigure.NewSignalMonitor()
	m.Monitor(gofigure.ReloadFunc(configReload))

	runtime.GOMAXPROCS(runtime.NumCPU())

	meduzaServer = NewMeduza()

	err := meduzaServer.Start()
	if err != nil {
		panic(err)
	}

}
