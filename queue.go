package lmq

import (
	"sync"

	"github.com/bmatsuo/lmdb-go/lmdb"
)

type Queue interface {
	Topic(name string) Topic
	SendMessage(topic Topic, msg []Message) bool
	StartConsuming(topic Topic, maxFetch uint) bool
	StopConsuming(topic Topic) bool
}

type lmdbQueue struct {
	path   string
	env    *lmdb.Env
	topics map[string]*Topic
	mu     sync.Mutex
}

func newLmdbQueue(path string) *lmdbQueue {
	queue := &lmdbQueue{
		path:   path,
		env:    lmdb.NewEnv(),
		topics: make(map[string]*Topic),
	}
	return queue
}
