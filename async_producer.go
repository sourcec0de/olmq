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
	client   Client
	conf     *Config
	ownQueue bool

	errors           chan *ProducerError
	input, successes chan *ProducerMessage
	inFight          sync.WaitGroup
}

func NewAsyncProducer(path string, conf *Config) (AsyncProducer, error) {
	client := NewClient(path, conf)
	p, err := NewAsyncProducerWithQueue(client)
	if err != nil {
		return nil, err
	}
	p.(*asyncProducer).ownQueue = true
	return p, nil
}

func NewAsyncProducerWithQueue(client Client) (AsyncProducer, error) {
	// TODO: Add queue.Closed
	p := &asyncProducer{
		client:    client,
		conf:      client.Config(),
		errors:    make(chan *ProducerError),
		input:     make(chan *ProducerMessage),
		successes: make(chan *ProducerMessage),
	}
	go withRecover(p.dispatcher)
	return p, nil
}

func (p *asyncProducer) dispatcher() {
	handlers := make(map[string]chan<- *ProducerMessage)
	//shuttingDown := false

	for msg := range p.input {
		if msg == nil {
			// TODO: add logger, ignored nil msg
			continue
		}
		/* TODO: add add shutDown handle
		if msg.flags&shutDown != 0 {
			shuttingDown = true
			p.inFight.Done()
			continue
		}
		*/
		handler := handlers[msg.Topic]
		if handler == nil {
			handler = p.newTopicProducer(msg.Topic)
			handlers[msg.Topic] = handler
		}
		handler <- msg
	}
	for _, handler := range handlers {
		close(handler)
	}
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

type topicProducer struct {
	parent *asyncProducer
	topic  string
	input  <-chan *ProducerMessage

	handlers    map[uint64]chan<- *ProducerMessage
	partitioner Partitioner
}

func (p *asyncProducer) newTopicProducer(topic string) chan<- *ProducerMessage {
	input := make(chan *ProducerMessage, p.conf.ChannelBufferSize)
	tp := &topicProducer{
		parent:      p,
		topic:       topic,
		input:       input,
		handlers:    make(map[uint64]chan<- *ProducerMessage),
		partitioner: nil, // TODO: call openPartitionForPersisted
	}
	go withRecover(tp.dispatch)
	return input
}

func (tp *topicProducer) dispatch() {
	for msg := range tp.input {
		handler := tp.handlers[msg.Partition]
		if handler == nil {
			handler = tp.parent.newPartitionProducer(msg.Topic, msg.Partition)
			tp.handlers[msg.Partition] = handler
		}
		handler <- msg
	}
	for _, handler := range tp.handlers {
		close(handler)
	}
}

type partitionProducer struct {
	parent    *asyncProducer
	topic     string
	partition uint64
	input     <-chan *ProducerMessage
	out       chan<- *ProducerMessage
}

func (p *asyncProducer) newPartitionProducer(topic string, partition uint64) chan<- *ProducerMessage {
	input := make(chan *ProducerMessage, p.conf.ChannelBufferSize)
	pp := &partitionProducer{
		parent:    p,
		topic:     topic,
		partition: partition,
		input:     input,
	}
	go withRecover(pp.dispatch)
	return input
}

func (pp *partitionProducer) dispatch() {
	for msg := range pp.input {
		pp.parent.successes <- msg
	}
}
