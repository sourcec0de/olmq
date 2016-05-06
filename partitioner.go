package lmq

// Partitioner is anything that, given a message and a number of partitions indexed [0...numPartitions-1],
// decides to which partition to send the message.
type Partitioner interface {
	// Partition takes a message and partition count and chooses a partition
	Partition(message *ProducerMessage, numPartitions uint64) (int32, error)
}
