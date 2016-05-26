package lmq

import "log"

// Consumer process messages from mq. You MUST call Close() on a consumer to avoid leaks,
// it will not be garbage-collected automatically when it passes out of scope.
type Consumer interface {
	ReadMessages(consumerTag string, topic string) <-chan Message
	Close() error
}

type consumer struct {
	client      Client
	conf        *Config
	ownerClient bool
}

// NewConsumer creates a new Consumer using the given mq path and configuration.
func NewConsumer(path string, config *Config) (Consumer, error) {
	client, err := NewClient(path, config)
	if err != nil {
		return nil, err
	}
	c, err := NewConsumerFromClient(client)
	if err != nil {
		return nil, err
	}
	c.(*consumer).ownerClient = true
	return c, nil
}

// NewConsumerFromClient creates a new consumer using the given client. It is still
// necessary to call Close() on the underlying client when shutting down this consumer.
func NewConsumerFromClient(client Client) (Consumer, error) {
	c := &consumer{
		client: client,
		conf:   client.Config(),
	}
	return c, nil
}

func (c *consumer) Close() error {
	return nil
}

func (c *consumer) ReadMessages(consumerTag string, topic string) <-chan Message {
	log.Println("ReadMessages, consumerTag: ", consumerTag, ", topic: ", topic)
	c.client.RefleshTopicMeta(topic)
	log.Println("ReadMessages, after c.client.RefleshTopicMeta")
	return c.client.ReadMessages(consumerTag, topic)
}
