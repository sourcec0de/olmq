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
	Close() error
}

type lmdbBroker struct {
	path string
	conf *Config

	env *lmdb.Env
}

func NewBroker(path string) Broker {
	brokerManager.Lock()
	defer brokerManager.Unlock()
	broker := brokerManager.m[path]
	if broker == nil {
		broker = &lmdbBroker{
			path: path,
			conf: nil,
		}
		brokerManager.m[path] = broker
	}
	return broker
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
