package lmq

import (
	"fmt"
	"os"
	"testing"
)

func TestNewAsyncProducer(t *testing.T) {
	conf := &Config{}
	conf.Topic.maxNum = 256
	conf.Topic.mapSize = 256 * 1024 * 1024
	conf.Topic.partitionSize = 4096 * 100
	conf.Topic.partitionsToKeep = 8
	conf.ChannelBufferSize = 256

	root, _ := os.Getwd()
	path := fmt.Sprintf("%s/test-newAsyncProducer", root)
	_ = os.Mkdir(path, 0755)

	producer, err := NewAsyncProducer(path, conf)
	if producer == nil || err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		producer.Input() <- &ProducerMessage{
			Topic:   "Topic1",
			payload: "hello",
		}
	}
}

func TestNewAsyncProducerWithMultiThread(t *testing.T) {
	conf := &Config{}
	conf.Topic.maxNum = 256
	conf.Topic.mapSize = 256 * 1024 * 1024
	conf.Topic.partitionSize = 4096 * 100
	conf.Topic.partitionsToKeep = 8
	conf.ChannelBufferSize = 256

	root, _ := os.Getwd()
	path := fmt.Sprintf("%s/test-newMultiAsyncProducer", root)
	_ = os.Mkdir(path, 0755)

	quit := make(chan bool)
	go func() {
		producer, err := NewAsyncProducer(path, conf)
		if producer == nil || err != nil {
			t.Fatal(err)
		}

		for i := 0; i < 10; i++ {
			producer.Input() <- &ProducerMessage{
				Topic:   "Topic1",
				payload: "hello",
			}
		}
		quit <- true
	}()
	producer, err := NewAsyncProducer(path, conf)
	if producer == nil || err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		producer.Input() <- &ProducerMessage{
			Topic:   "Topic1",
			payload: "hello",
		}
	}
	<-quit
}

func BenchmarkNewAsyncProducer(b *testing.B) {
	conf := &Config{}
	conf.Topic.maxNum = 256
	conf.Topic.mapSize = 256 * 1024 * 1024
	conf.Topic.partitionSize = 4096 * 100
	conf.Topic.partitionsToKeep = 8
	conf.ChannelBufferSize = 256

	root, _ := os.Getwd()
	path := fmt.Sprintf("%s/test-newBenchAsyncProducer", root)
	_ = os.Mkdir(path, 0755)

	producer, err := NewAsyncProducer(path, conf)
	if producer == nil || err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		producer.Input() <- &ProducerMessage{
			Topic:   "Topic1",
			payload: "hello",
		}
	}
}

func BenchmarkNewAsyncProducerWithMultiThread(b *testing.B) {
	conf := &Config{}
	conf.Topic.maxNum = 256
	conf.Topic.mapSize = 256 * 1024 * 1024
	conf.Topic.partitionSize = 4096 * 100
	conf.Topic.partitionsToKeep = 8
	conf.ChannelBufferSize = 256

	root, _ := os.Getwd()
	path := fmt.Sprintf("%s/test-newBenchMultiAsyncProducer", root)
	_ = os.Mkdir(path, 0755)

	quit := make(chan bool)
	go func() {
		producer, err := NewAsyncProducer(path, conf)
		if producer == nil || err != nil {
			b.Fatal(err)
		}

		for i := 0; i < 10; i++ {
			producer.Input() <- &ProducerMessage{
				Topic:   "Topic1",
				payload: "hello",
			}
		}

		quit <- true
	}()

	producer, err := NewAsyncProducer(path, conf)
	if producer == nil || err != nil {
		b.Fatal(err)
	}

	for i := 0; i < 10000000; i++ {
		producer.Input() <- &ProducerMessage{
			Topic:   "Topic1",
			payload: "hello",
		}
	}
}
