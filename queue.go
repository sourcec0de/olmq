package lmq

type Queue interface {
	Topic(name string) Topic
	SendMessage(topic Topic, msg []Message) bool
	StartConsuming(topic Topic, maxFetch uint) bool
	StopConsuming(topic Topic) bool
}

type lmdbQueue struct {
	path   string
	topics map[string]*Topic
}

func newLmdbQueue(path string) *lmdbQueue {
	queue := &lmdbQueue{
		path:   path,
		topics: make(map[string]*Topic),
	}
	return queue
}
