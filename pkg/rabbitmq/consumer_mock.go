package rabbitmq

import amqp "github.com/rabbitmq/amqp091-go"

type IConsumer interface {
	Consume(autoAck bool) <-chan amqp.Delivery
	Close()
}

type IStringConsumer interface {
	Consume(callback StringConsumerCallback)
	Close()
}

type IJsonConsumer[T any] interface {
	Consume(callback JsonConsumerCallback[T])
	Close()
}

type ConsumerMock struct {
	Messages chan amqp.Delivery
}

func (this ConsumerMock) Consume(_autoAck bool) <-chan amqp.Delivery {
	return this.Messages
}

func (this ConsumerMock) Close() {
	close(this.Messages)
}

type StringMessageMock struct {
	Content string
	Headers map[string]interface{}
}

type StringConsumerMock struct {
	Messages chan StringMessageMock
}

func (this StringConsumerMock) Consume(callback StringConsumerCallback) {
	for message := range this.Messages {
		callback(message.Content, message.Headers)
	}
}

func (this StringConsumerMock) Close() {
	close(this.Messages)
}

type JsonConsumerMock[T any] struct {
	Messages chan JsonMessageMock[T]
}

type JsonMessageMock[T any] struct {
	Content *T
	Headers map[string]interface{}
}

func (this JsonConsumerMock[T]) Consume(callback JsonConsumerCallback[T]) {
	for message := range this.Messages {
		callback(message.Content, message.Headers)
	}
}

func (this JsonConsumerMock[T]) Close() {
	close(this.Messages)
}
