package redis

// Config represents the configurations for the redis driver
type Config struct {
	Network               string `yaml:"net"`
	Addr                  string `yaml:"addr"`
	Timeout               int64  `yaml:"timeout_ms"`
	Master                bool   `yaml:"master"`
	RepairEnabled         bool   `yaml:"repair_enabled"`
	RepairFrequency       int    `yaml:"repair_freq_ms"`
	TextCompressThreshold int    `yaml:"text_compress_threshold"`
	DeleteChunkSize       int    `yaml:"del_chunk_size"`
}

var DefaultConfig = Config{
	Network:               "tcp",
	Addr:                  "localhost:6379",
	Timeout:               1000,
	Master:                true,
	RepairEnabled:         false,
	RepairFrequency:       50,
	TextCompressThreshold: 2048,
	DeleteChunkSize:       100,
}
