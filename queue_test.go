package lmq

import (
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
