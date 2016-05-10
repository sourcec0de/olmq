package lmq

// Client is a generic mq client. It manages connections to one broker.
type Client interface {
	// Config returns the Config struct of the client. This struct should not be
	// altered after it has been created.
	Config() *Config
	RefleshTopicMeta(name string)
	WritablePartition(topic string) (uint64, error)
	WriteMessages(msgs []Message, topic string)
	ReadMessages(topic string) <-chan Message
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

func (client *client) WritablePartition(topic string) (uint64, error) {
	return client.broker.WritablePartition(topic)
}

func (client *client) WriteMessages(msgs []Message, topic string) {
	client.broker.WriteMessages(msgs, topic)
}

func (client *client) ReadMessages(topic string) <-chan Message {
	return client.broker.ReadMessages(topic)
}
