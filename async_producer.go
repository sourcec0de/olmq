package lmq

// AsyncProducer publishes messages using a non-blocking API. You must read from the
// Errors() channel or the producer will deadlock. You must call Close() or AsyncClose()
// on a producer to avoid leaks: it will not be garbage-collected automatically when it
// passes out of scope.
type AsyncProducer interface {

	// AsyncClose triggers a shutdown of the producer, flushing any messages it may
	// have buffered. The shutdown has completed when both the Errors and Successes
	// channels have been closed. When calling AsyncClose, you *must* continue to
	// read from those channels in order to drain the results of any messages in
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
}

type asyncProducer struct {
	client   Client
	conf     *Config
	ownQueue bool
	input    chan *ProducerMessage
}

// NewAsyncProducer creates a new AsyncProducer using the given mq path and configuration.
func NewAsyncProducer(path string, conf *Config) (AsyncProducer, error) {
	client, err := NewClient(path, conf)
	if err != nil {
		return nil, err
	}
	p, err := NewAsyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}
	p.(*asyncProducer).ownQueue = true
	return p, nil
}

// NewAsyncProducerFromClient creates a new Producer using the given client. It is still
// necessary to call Close() on the underlying client when shutting down this producer.
func NewAsyncProducerFromClient(client Client) (AsyncProducer, error) {
	p := &asyncProducer{
		client: client,
		conf:   client.Config(),
		input:  make(chan *ProducerMessage),
	}
	go withRecover(p.dispatcher)
	return p, nil
}

func (p *asyncProducer) dispatcher() {
	handlers := make(map[string]chan<- *ProducerMessage)
	for msg := range p.input {
		if msg == nil {
			continue
		}
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

func (p *asyncProducer) Input() chan<- *ProducerMessage {
	return p.input
}

type topicProducer struct {
	parent *asyncProducer
	topic  string
	input  <-chan *ProducerMessage

	handlers map[uint64]chan<- *ProducerMessage
}

func (p *asyncProducer) newTopicProducer(topic string) chan<- *ProducerMessage {
	input := make(chan *ProducerMessage, p.conf.ChannelBufferSize)
	tp := &topicProducer{
		parent:   p,
		topic:    topic,
		input:    input,
		handlers: make(map[uint64]chan<- *ProducerMessage),
	}
	p.client.RefleshTopicMeta(topic)
	go withRecover(tp.dispatch)
	return input
}

func (tp *topicProducer) dispatch() {
	_, _ = tp.partitionMessage() // fix me, should auto fix env create, not show this to user
	i := 0
	var msgs []Message
	for msg := range tp.input {
		i++
		msgs = append(msgs, Message(msg.payload))
		if len(msgs) >= 9 {
			tp.parent.client.WriteMessages(msgs, tp.topic)
			msgs = msgs[:0]
		}
	}
}

func (tp *topicProducer) partitionMessage() (uint64, error) {
	return tp.parent.client.WritablePartition(tp.topic)
}
