package lmq

import (
	"fmt"
	"sync"

	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/pkg/errors"
)

const (
	defaultTopicMapSize = 256 * 1024 * 1024
	defaultMaxTopicNum  = 256
	envMetaName         = "__meta__"
)

type Queue interface {
	Topic(name string) Topic
	SendMessage(topic Topic, msg []Message)
	StartConsuming(topic Topic, maxFetch uint) bool
	StopConsuming(topic Topic) bool
}

type QueueOpt struct {
	maxTopicNum  int
	topicMapSize int64
}

type lmdbQueue struct {
	path   string
	env    *lmdb.Env
	topics map[string]*lmdbTopic
	mu     sync.Mutex
}

func newLmdbQueue(path string, opt *QueueOpt) *lmdbQueue {
	queue := &lmdbQueue{
		path:   path,
		topics: make(map[string]*lmdbTopic),
	}
	env, err := lmdb.NewEnv()
	if err != nil {
		panic(err)
	}
	queue.env = env
	if err = queue.conf(opt); err != nil {
		panic(err)
	}
	envPath := fmt.Sprintf("%s/%s", path, envMetaName)
	if err := env.Open(envPath, lmdb.NoSync|lmdb.NoSubdir, 0644); err != nil {
		_ = env.Close()
		panic(err)
	}
	if _, err := env.ReaderCheck(); err != nil {
		_ = env.Close()
		panic(err)
	}
	return queue
}

func (queue *lmdbQueue) conf(opt *QueueOpt) error {
	if opt != nil {
		if err := queue.env.SetMapSize(opt.topicMapSize); err != nil {
			return errors.Wrap(err, "SetMapSize failed")
		}
		if err := queue.env.SetMaxDBs(opt.maxTopicNum); err != nil {
			return errors.Wrap(err, "SetMaxDBs failed")
		}
	} else {
		if err := queue.env.SetMapSize(defaultTopicMapSize); err != nil {
			return errors.Wrap(err, "Set default MapSize failed")
		}
		if err := queue.env.SetMaxDBs(defaultMaxTopicNum); err != nil {
			return errors.Wrap(err, "Set default MaxDBs failed")
		}
	}
	return nil
}

func (queue *lmdbQueue) Topic(name string, opt *TopicOpt) *lmdbTopic {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	topic := queue.topics[name]
	if topic != nil {
		return topic
	}
	topic = newLmdbTopic(queue.env, name, opt)
	queue.topics[name] = topic
	return topic
}

func (queue *lmdbQueue) SendMessage(topic *lmdbTopic, msg []Message) {
	topic.PersistedToPartition(msg)
}

func (queue *lmdbQueue) ConsumingMessage(topic *lmdbTopic, out []Message) {
	topic.ConsumingPartition(out)
}
