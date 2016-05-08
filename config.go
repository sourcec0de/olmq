package lmq

// Config is used to pass multiple configuration options to lmq's constructors.
type Config struct {
	Producer  struct{}
	Consumser struct{}
	Topic     struct {
		maxNum           int
		mapSize          int64
		name             []string
		partitionSize    int64
		partitionsToKeep uint64
	}
}

// NewConfig returns a new configuration instance with sane defaults.
func NewConfig() *Config {
	c := &Config{}
	return c
}
