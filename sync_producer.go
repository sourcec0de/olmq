package lmq

// SyncProducer publishes messages to lmq.
type SyncProducer interface {
	SendMessage(msg *ProducerMessage) (partition, offset uint64, err error)
	Close() error
}
