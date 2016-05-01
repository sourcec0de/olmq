package lmq

import (
	"os"
	"testing"
)

func TestNewLmdbQueue(t *testing.T) {
	path := os.Getwd()
	queue := newLmdbQueue(path)
	if queue == nil {
		t.Error("newLmdbQueue failed")
	}
}
