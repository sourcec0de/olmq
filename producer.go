package lmq

type Producer interface {
	SendMessage(msg []Message)
}
