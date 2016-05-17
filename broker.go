package lmq

import (
	"fmt"
	"sync"

	"github.com/bmatsuo/lmdb-go/lmdb"
)

const (
	envMetaName = "__meta__"
)

var brokerManager struct {
	sync.Mutex
	m map[string]Broker
}

// Broker represents a single mq broker connection.
// All operations on this object are entirely concurrency-safe
type Broker interface {
	Open(conf *Config) error
	RefleshTopicMeta(name string)
	WritablePartition(topic string) (uint64, error)
	WriteMessages(msgs []Message, topic string)
	ReadMessages(consumerTag string, topic string) <-chan Message
	Close() error
}

type lmdbBroker struct {
	path string
	conf *Config

	env *lmdb.Env
	m   map[string]Topic
	sync.Mutex
}

// NewBroker returns a Broker with given path
func NewBroker(path string, conf *Config) (Broker, error) {
	brokerManager.Lock()
	defer brokerManager.Unlock()
	if brokerManager.m == nil {
		brokerManager.m = make(map[string]Broker)
	}
	broker := brokerManager.m[path]
	if broker == nil {
		broker = &lmdbBroker{
			path: path,
			conf: conf,
			m:    make(map[string]Topic),
		}
		brokerManager.m[path] = broker
		err := broker.Open(conf)
		if err != nil {
			return nil, err
		}
	}
	return broker, nil
}

func (broker *lmdbBroker) Open(conf *Config) error {
	env, err := lmdb.NewEnv()
	if err != nil {
		return err
	}
	if err = env.SetMapSize(conf.Topic.mapSize); err != nil {
		return err
	}
	if err = env.SetMaxDBs(conf.Topic.maxNum); err != nil {
		return err
	}
	envPath := fmt.Sprintf("%s/%s", broker.path, envMetaName)
	if err := env.Open(envPath, lmdb.NoSync|lmdb.NoSubdir, 0644); err != nil {
		return err
	}
	if _, err := env.ReaderCheck(); err != nil {
		return err
	}
	broker.env = env
	return nil
}

func (broker *lmdbBroker) Close() error {
	return broker.env.Close()
}

func (broker *lmdbBroker) RefleshTopicMeta(name string) {
	broker.Lock()
	defer broker.Unlock()
	topic := broker.m[name]
	if topic == nil {
		topic = newLmdbTopic(broker.env, name, broker.conf)
		broker.m[name] = topic
	}
}

func (broker *lmdbBroker) WritablePartition(topic string) (uint64, error) {
	broker.Lock()
	defer broker.Unlock()
	t := broker.m[topic]
	if t == nil {
		t = newLmdbTopic(broker.env, topic, broker.conf)
		broker.m[topic] = t
	}
	return t.OpenPartitionForPersisted()
}

func (broker *lmdbBroker) WriteMessages(msgs []Message, topic string) {
	broker.Lock()
	defer broker.Unlock()
	t := broker.m[topic]
	if t == nil {
		t = newLmdbTopic(broker.env, topic, broker.conf)
		broker.m[topic] = t
		t.OpenPartitionForPersisted()
	}
	t.PersistedToPartition(msgs)
}

func (broker *lmdbBroker) ReadMessages(consumerTag string, topic string) <-chan Message {
	broker.Lock()
	defer broker.Unlock()
	t := broker.m[topic]
	if t == nil {
		t = newLmdbTopic(broker.env, topic, broker.conf)
		broker.m[topic] = t
	}
	t.OpenPartitionForConsuming(consumerTag)
	return t.ConsumingFromPartition()
}
