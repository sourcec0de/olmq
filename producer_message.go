package lmq

// ProducerMessage is the collection of elements passed to the Producer in order to send a message.
type ProducerMessage struct{}

// ProducerError is the type of error generated when the producer fails to deliver a message.
// It contains the original ProducerMessage as well as the actual error value.
type ProducerError struct {
	Msg *ProducerMessage
	Err error
}
