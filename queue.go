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
	topics map[string]*lmdbTopic
	mu     sync.Mutex
}

func newLmdbQueue(path string) *lmdbQueue {
	queue := &lmdbQueue{
		path:   path,
		topics: make(map[string]*lmdbTopic),
	}
	env, err := lmdb.NewEnv()
	if err != nil {
		panic(err)
	}
	queue.env = env
	return queue
}

func (queue *lmdbQueue) Topic(name string) *lmdbTopic {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	topic := queue.topics[name]
	if topic != nil {
		return topic
	}
	topic = newLmdbTopic(queue.env, name)
	queue.topics[name] = topic
	return topic
}
