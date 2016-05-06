package lmq

import "sync"

// AsyncProducer publishes messages using a non-blocking API. You must read from the
// Errors() channel or the producer will deadlock. You must call Close() or AsyncClose()
// on a producer to avoid leaks: it will not be garbage-collected automatically when it
// passes out of scope.
type AsyncProducer interface {

	// AsyncClose triggers a shutdown of the producer, flushing any messages it may
	// have buffered. The shutdown has completed when both the Errors and Successes
	// channels have been closed. When calling AsyncClose, you *must* continue to // read from those channels in order to drain the results of any messages in
	// flight.
	AsyncClose()

	// Close shuts down the producer and flushes any messages it may have buffered.
	// You must call this function before a producer object passes out of scope, as
	// it may otherwise leak memory. You must call this before calling Close on the
	// underlying client.
	Close() error

	// Input is the input channel for the user to write messages to that they
	// wish to send.
	Input() chan<- *ProducerMessage

	// Successes is the success output channel back to the user when AckSuccesses is
	// enabled. If Return.Successes is true, you MUST read from this channel or the
	// Producer will deadlock. It is suggested that you send and read messages
	// together in a single select statement.
	Successes() <-chan *ProducerMessage

	// Errors is the error output channel back to the user. You MUST read from this
	// channel or the Producer will deadlock when the channel is full. Alternatively,
	// you can set Producer.Return.Errors in your config to false, which prevents
	// errors to be returned.
	Errors() <-chan *ProducerError
}

type asyncProducer struct {
	queue    Queue // Queue type is an interface
	conf     *Config
	ownQueue bool

	errors           chan *ProducerError
	input, successes chan *ProducerMessage
	inFight          sync.WaitGroup
}

func NewAsyncProducer(path string, conf *Config) (AsyncProducer, error) {
	queue := OpenQueue(path, nil) // fix me, conf will be passed in
	p, err := NewAsyncProducerWithQueue(queue)
	if err != nil {
		return nil, err
	}
	p.(*asyncProducer).ownQueue = true
	return p, nil
}

func NewAsyncProducerWithQueue(queue Queue) (AsyncProducer, error) {
	return nil, nil
}

func (p *asyncProducer) AsyncClose() {

}

func (p *asyncProducer) Close() error {
	return nil
}

func (p *asyncProducer) Errors() <-chan *ProducerError {
	return p.errors
}

func (p *asyncProducer) Input() chan<- *ProducerMessage {
	return p.input
}

func (p *asyncProducer) Successes() <-chan *ProducerMessage {
	return p.successes
}
