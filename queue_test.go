package lmq

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

func TestNewLmdbQueue(t *testing.T) {
	root, _ := os.Getwd()
	path := fmt.Sprintf("%s/test-newLmdbQueue", root)
	os.Mkdir(path, 0755)
	defer os.RemoveAll(path)
	queue := newLmdbQueue(path, nil)
	if queue == nil {
		t.Error("newLmdbQueue failed")
	}
}

func TestTopic(t *testing.T) {
	root, _ := os.Getwd()
	path := fmt.Sprintf("%s/test-topic", root)
	os.Mkdir(path, 0755)
	defer os.RemoveAll(path)
	queue := newLmdbQueue(path, nil)
	if queue == nil {
		t.Error("newLmdbQueue failed")
	}
	topic := queue.Topic("GetTopic", nil)
	if topic == nil {
		t.Error("Topic failed")
	}
	topic.OpenPartitionForPersisted()
}

func TestSendMessage(t *testing.T) {
	root, _ := os.Getwd()
	path := fmt.Sprintf("%s/test-sendMessage", root)
	os.Mkdir(path, 0755)
	defer os.RemoveAll(path)
	queue := newLmdbQueue(path, nil)
	if queue == nil {
		t.Error("newLmdbQueue failed")
	}
	topic := queue.Topic("GetTopic", nil)
	if topic == nil {
		t.Error("Topic failed")
	}
	topic.OpenPartitionForPersisted()
	msgs := make([]Message, 2)
	msgs[0] = []byte("hello")
	msgs[1] = []byte("world")
	queue.SendMessage(topic, msgs)
}

func TestConsumingMessage(t *testing.T) {
	root, _ := os.Getwd()
	path := fmt.Sprintf("%s/test-consumingMessage", root)
	os.Mkdir(path, 0755)
	defer os.RemoveAll(path)
	queue := newLmdbQueue(path, nil)
	if queue == nil {
		t.Error("newLmdbQueue failed")
	}
	topic := queue.Topic("GetTopic", nil)
	if topic == nil {
		t.Error("Topic failed")
	}
	topic.OpenPartitionForPersisted()
	msgs := make([]Message, 2)
	msgs[0] = []byte("hello")
	msgs[1] = []byte("world")
	queue.SendMessage(topic, msgs)
	topic.OpenPartitionForConsuming("test1")
	rmsgs := make([]Message, 2)
	queue.ConsumingMessage(topic, rmsgs)
	if !bytes.Equal(msgs[0], rmsgs[0]) || !bytes.Equal(msgs[1], rmsgs[1]) {
		t.Error("Consuming failed")
	}
}
