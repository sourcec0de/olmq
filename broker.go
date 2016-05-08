package lmq

import (
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
