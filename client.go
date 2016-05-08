package lmq

// Client is a generic mq client. It manages connections to one broker.
type Client interface {
	// Config returns the Config struct of the client. This struct should not be
	// altered after it has been created.
	Config() *Config
}

type client struct {
	conf   *Config
	broker Broker
}

// NewClient returns a Client with given path and Config
func NewClient(path string, conf *Config) Client {
	if conf == nil {
		conf = NewConfig()
	}
	client := &client{
		conf:   conf,
		broker: NewBroker(path),
	}
	return client
}

func (client *client) Config() *Config {
	return client.conf
}
