package lmq

import (
	"os"
	"testing"
)

func TestOpenQueue(t *testing.T) {
	path, _ = os.Getwd()
	queue := OpenQueue(path)
	if queue == nil {
		t.Error("Failed to OpenQueue")
	}
}
