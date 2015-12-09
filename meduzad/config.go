package main

import "github.com/EverythingMe/meduza/driver/redis"

type serverConfig struct {
	Listen       string `yaml:"listen"`
	CtlListen    string `yaml:"ctl_listen"`
	LoggingLevel string `yaml:"logging_level"`
}

type statsdConfig struct {
	Enabled    bool    `yaml:"enabled"`
	Address    string  `yaml:"address"`
	SampleRate float32 `yaml:sample_rate`
	Prefix     string  `yaml:"prefix"`
}

type scribeConfig struct {
	Enabled    bool   `yaml:"enabled"`
	Address    string `yaml:"address"`
	Category   string `yaml:"category"`
	BufferSize int    `yaml:"buffer_size"`
}

var config = struct {
	Server      serverConfig `yaml:"server"`
	Redis       redis.Config `yaml:"redis"`
	SchemaRedis redis.Config `yaml:"schema_redis"`
	Scribe      scribeConfig `yaml:"scribe"`
	Statsd      statsdConfig `yaml:"statsd"`
}{
	Server: serverConfig{
		Listen:       ":9977",
		CtlListen:    ":9966",
		LoggingLevel: "INFO",
	},
	Redis:       redis.DefaultConfig,
	SchemaRedis: redis.DefaultConfig,
	Scribe: scribeConfig{
		Enabled:    false,
		Address:    "localhost:1463",
		Category:   "meduza",
		BufferSize: 1000,
	},
	Statsd: statsdConfig{
		Enabled:    true,
		Address:    "localhost:8125",
		SampleRate: 0.05,
		Prefix:     "meduza",
	},
}
