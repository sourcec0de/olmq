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
		topics: make(map[string]*Topic),
	}
	env, err := lmdb.NewEnv()
	if err != nil {
		panic(err)
	}
	queue.env = env
	return queue
}
