package lmq

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestNewAsyncProducer(t *testing.T) {
	conf := &Config{}
	conf.Topic.maxNum = 256
	conf.Topic.mapSize = 256 * 1024 * 1024
	conf.Topic.name = make([]string, 2)
	conf.Topic.name[0] = "Topic0"
	conf.Topic.name[1] = "Topic1"
	conf.Topic.partitionSize = 1024 * 1024 * 1024
	conf.Topic.partitionsToKeep = 8

	root, _ := os.Getwd()
	path := fmt.Sprintf("%s/test-newAsyncProducer", root)
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
}
