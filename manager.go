package lmq

import "sync"

type Manager interface {
	OpenQueue(name string) Queue
	CloseQueue(name string) error
}

type lmdbManager struct {
	queues map[string]Queue
}

func newLmdbManager() *lmdbManager {
	manager := &lmdbManager{
		queues: make(map[string]Queue),
	}
	return manager
}

var (
	manager *lmdbManager
	mu      sync.Mutex
)

func init() {
	manager = newLmdbManager()
}

func OpenQueue(path string, opt *QueueOpt) Queue {
	mu.Lock()
	defer mu.Unlock()
	queue := manager.queues[path]
	if queue != nil {
		return queue
	}
	queue = newLmdbQueue(path, opt)
	return queue
}
