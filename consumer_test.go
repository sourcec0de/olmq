package lmq

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestNewConsumer(t *testing.T) {
	conf := &Config{}
	conf.Topic.maxNum = 256
	conf.Topic.mapSize = 256 * 1024 * 1024
	conf.Topic.partitionSize = 1024 * 1024 * 1024
	conf.Topic.partitionsToKeep = 8
	conf.ChannelBufferSize = 256

	root, _ := os.Getwd()
	path := fmt.Sprintf("%s/test-newConsumer", root)
	_ = os.Mkdir(path, 0755)
	defer func() {
		_ = os.RemoveAll(path)
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

	recvd := 0
	results := producer.Successes()
	for recvMsg := range results {
		if !strings.EqualFold(recvMsg.payload, "hello") {
			t.Error(recvMsg.payload)
		}
		recvd++
		if recvd == 10 {
			break
		}
	}
	if recvd != 10 {
		t.Error("Data lost")
	}

	consumer, err := NewConsumer(path, conf)
	if consumer == nil || err != nil {
		t.Fatal(err)
	}

	reads := consumer.ReadMessages("Consumer1", "Topic1")
	recvd = 0
	for recvMsg := range reads {
		if !bytes.Equal(recvMsg, []byte("hello")) {
			t.Error(recvMsg)
		}
		recvd++
		if recvd == 9 {
			break
		}
	}
	if recvd != 9 {
		t.Error("Data lost")
	}
}

func TestNewConsumerWithMultiThread(t *testing.T) {
	conf := &Config{}
	conf.Topic.maxNum = 256
	conf.Topic.mapSize = 256 * 1024 * 1024
	conf.Topic.partitionSize = 1024 * 1024 * 1024
	conf.Topic.partitionsToKeep = 8
	conf.ChannelBufferSize = 256

	root, _ := os.Getwd()
	path := fmt.Sprintf("%s/test-newConsumer", root)
	_ = os.Mkdir(path, 0755)
	defer func() {
		_ = os.RemoveAll(path)
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

	quit := make(chan bool)
	go func() {
		recvd := 0
		results := producer.Successes()
		for recvMsg := range results {
			if !strings.EqualFold(recvMsg.payload, "hello") {
				t.Error(recvMsg.payload)
			}
			recvd++
			if recvd == 10 {
				break
			}
		}
		if recvd != 10 {
			t.Error("Data lost")
		}

		consumer, err := NewConsumer(path, conf)
		if consumer == nil || err != nil {
			t.Fatal(err)
		}

		reads := consumer.ReadMessages("Consumer1", "Topic1")
		recvd = 0
		for recvMsg := range reads {
			if !bytes.Equal(recvMsg, []byte("hello")) {
				t.Error(recvMsg)
			}
			recvd++
			if recvd == 9 {
				break
			}
		}
		if recvd != 9 {
			t.Error("Data lost")
		}
		quit <- true
	}()
	recvd := 0
	results := producer.Successes()
	for recvMsg := range results {
		if !strings.EqualFold(recvMsg.payload, "hello") {
			t.Error(recvMsg.payload)
		}
		recvd++
		if recvd == 10 {
			break
		}
	}
	if recvd != 10 {
		t.Error("Data lost")
	}

	consumer, err := NewConsumer(path, conf)
	if consumer == nil || err != nil {
		t.Fatal(err)
	}

	reads := consumer.ReadMessages("Consumer1", "Topic1")
	recvd = 0
	for recvMsg := range reads {
		if !bytes.Equal(recvMsg, []byte("hello")) {
			t.Error(recvMsg)
		}
		recvd++
		if recvd == 9 {
			break
		}
	}
	if recvd != 9 {
		t.Error("Data lost")
	}
	<-quit
}

/*
func BenchmarkNewConsumer(b *testing.B) {
	conf := &Config{}
	conf.Topic.maxNum = 256
	conf.Topic.mapSize = 256 * 1024 * 1024
	conf.Topic.partitionSize = 1024 * 1024 * 1024
	conf.Topic.partitionsToKeep = 8
	conf.ChannelBufferSize = 256

	root, _ := os.Getwd()
	path := fmt.Sprintf("%s/test-newConsumer", root)
	_ = os.Mkdir(path, 0755)
	defer func() {
		_ = os.RemoveAll(path)
	}()
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

	recvd := 0
	results := producer.Successes()
	for recvMsg := range results {
		if !strings.EqualFold(recvMsg.payload, "hello") {
			b.Error(recvMsg.payload)
		}
		recvd++
		if recvd == 10 {
			break
		}
	}
	if recvd != 10 {
		b.Error("Data lost")
	}

	consumer, err := NewConsumer(path, conf)
	if consumer == nil || err != nil {
		b.Fatal(err)
	}

	reads := consumer.ReadMessages("Consumer1", "Topic1")
	recvd = 0
	for recvMsg := range reads {
		if !bytes.Equal(recvMsg, []byte("hello")) {
			b.Error(recvMsg)
		}
		recvd++
		if recvd == 9 {
			break
		}
	}
	if recvd != 9 {
		b.Error("Data lost")
	}
}
*/
