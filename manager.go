package lmq

import "sync"

var (
	qm Manager
	mu sync.Mutex
)

func init() {
	qm = newManager()
}

// Manager is an interface  defined the method used to manager multi Queue
type Manager interface {
	OpenQueue(name string, conf *Config) Queue
	CloseQueue(name string) error
}

type manager struct {
	queues map[string]Queue
}

func newManager() Manager {
	return &manager{
		queues: make(map[string]Queue),
	}
}

func (m *manager) OpenQueue(path string, conf *Config) Queue {
	mu.Lock()
	defer mu.Unlock()
	queue := m.queues[path]
	if queue != nil {
		return queue
	}
	queue = newLmdbQueue(path, conf)
	return queue
}

func (m *manager) CloseQueue(name string) error {
	return nil
}

// OpenQueue creates a new Queue or return an exist Queue using given queue path and configuration
func OpenQueue(path string, conf *Config) Queue {
	return qm.OpenQueue(path, conf)
}
