package lmq

type Queue interface {
	Topic(name string) Topic
	SendMessage(topic Topic, msg []Message) bool
	StartConsuming(topic Topic, maxFetch uint) bool
	StopConsuming(topic Topic) bool
}
