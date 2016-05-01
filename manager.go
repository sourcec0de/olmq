package lmq

type Manager interface {
	OpenQueue(name string) Queue
	CloseQueue(name string) error
}
