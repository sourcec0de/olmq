package lmq

import (
	"fmt"
	"os"
	"testing"
)

func TestNewLmdbQueue(t *testing.T) {
	path, _ := os.Getwd()
	queue := newLmdbQueue(path)
	if queue == nil {
		t.Error("newLmdbQueue failed")
	}
}

func TestTopic(t *testing.T) {
	root, _ := os.Getwd()
	path := fmt.Sprintf("%s/test-topic", root)
	os.Mkdir(path, 0755)
	defer os.RemoveAll(path)
	queue := newLmdbQueue(path)
	if queue == nil {
		t.Error("newLmdbQueue failed")
	}
	topic := queue.Topic("GetTopic")
	if topic != nil {
		t.Error("Topic failed")
	}
}
