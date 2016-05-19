package lmq

// Config is used to pass multiple configuration options to lmq's constructors.
type Config struct {
	Producer  struct{}
	Consumser struct{}
	Topic     struct {
		maxNum           int
		mapSize          int64
		partitionSize    int64
		partitionsToKeep uint64
	}
	ChannelBufferSize int
}

// NewConfig returns a new configuration instance with sane defaults.
func NewConfig() *Config {
	c := &Config{}
	c.Topic.maxNum = 256 * 2
	c.ChannelBufferSize = 256
	c.Topic.partitionSize = 1024 * 1024 * 1024 * 2
	c.Topic.partitionsToKeep = 8
	return c
}
