package rabbitmq

import (
	"io"

	"github.com/streadway/amqp"
)

type ExchangeType string

const (
	ExchangeDirect  ExchangeType = "direct"
	ExchangeFanout  ExchangeType = "fanout"
	ExchangeTopic   ExchangeType = "topic"
	ExchangeHeaders ExchangeType = "headers"
)

type ArgumentsTable amqp.Table

type Message struct {
	delivery amqp.Delivery
	// for io.Reader implementation
	readIndex int
}

func (m *Message) Bytes() []byte {
	return m.delivery.Body
}

func (m *Message) Read(p []byte) (int, error) {
	if m.readIndex >= len(m.delivery.Body) {
		return 0, io.EOF
	}

	n := copy(p, m.delivery.Body[m.readIndex:])
	m.readIndex += n
	return n, nil
}

func (m *Message) Ack() error {
	return m.delivery.Ack(false)
}

func (m *Message) AckMultiple() error {
	return m.delivery.Ack(true)
}

func (m *Message) Reject(requeue bool) error {
	return m.delivery.Reject(requeue)
}

func (m *Message) Nack(requeue bool) error {
	return m.delivery.Nack(false, requeue)
}

func (m *Message) NackMultiple(requeue bool) error {
	return m.delivery.Nack(true, requeue)
}
