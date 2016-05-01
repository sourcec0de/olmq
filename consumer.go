package lmq

type Consumer interface {
	Consume() []Message
}
