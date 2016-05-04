package lmq

import (
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
	topic := queue.Topic("GetTopic", 0, nil)
	if topic == nil {
		t.Error("Topic failed")
	}
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
	topic := queue.Topic("GetTopic", 0, nil)
	if topic == nil {
		t.Error("Topic failed")
	}
	msgs := make([]Message, 2)
	msgs[0] = []byte("hello")
	msgs[1] = []byte("world")
	queue.SendMessage(topic, msgs)
}
