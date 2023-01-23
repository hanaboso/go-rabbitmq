package rabbitmq

import amqp "github.com/rabbitmq/amqp091-go"

type Queue struct {
	Name    string
	Options QueueOptions
}

type QueueOptions struct {
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
}

var DefaultQueueOptions = QueueOptions{
	Durable:    true,
	AutoDelete: false,
	Exclusive:  false,
	NoWait:     false,
}
