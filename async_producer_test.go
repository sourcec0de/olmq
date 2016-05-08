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
	conf.Topic.name = make([]string, 2)
	conf.Topic.name[0] = "Topic0"
	conf.Topic.name[1] = "Topic1"
	conf.Topic.partitionSize = 1024 * 1024 * 1024
	conf.Topic.partitionsToKeep = 8

	root, _ := os.Getwd()
	path := fmt.Sprintf("%s/test-newAsyncProducer", root)
	os.Mkdir(path, 0755)
	defer os.RemoveAll(path)

	producer, err := NewAsyncProducer(path, conf)
	if producer == nil || err != nil {
		t.Fatal(err)
	}
}
