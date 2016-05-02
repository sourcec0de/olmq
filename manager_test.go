package lmq

import (
	"fmt"
	"os"
	"testing"
)

func TestOpenQueue(t *testing.T) {
	root, _ := os.Getwd()
	path := fmt.Sprintf("%s/test-OpenQueue", root)
	os.Mkdir(path, 0755)
	defer os.RemoveAll(path)
	queue := OpenQueue(path, nil)
	if queue == nil {
		t.Error("Failed to OpenQueue")
	}
}
