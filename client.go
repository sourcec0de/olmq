package lmq

// Client is a generic mq client. It manages connections to one broker.
type Client interface {
	// Config returns the Config struct of the client. This struct should not be
	// altered after it has been created.
	Config() *Config
	RefleshTopicMeta(name string)
}

type client struct {
	conf   *Config
	broker Broker
}

// NewClient returns a Client with given path and Config
func NewClient(path string, conf *Config) (Client, error) {
	if conf == nil {
		conf = NewConfig()
	}
	broker, err := NewBroker(path, conf)
	if err != nil {
		return nil, err
	}
	client := &client{
		conf:   conf,
		broker: broker,
	}
	return client, nil
}

func (client *client) Config() *Config {
	return client.conf
}

func (client *client) RefleshTopicMeta(name string) {
	client.broker.RefleshTopicMeta(name)
}
