package rabbitmq

import amqp "github.com/rabbitmq/amqp091-go"

type Exchange struct {
	Name     string
	Kind     string
	Options  ExchangeOptions
	Bindings []BindOptions
}

type ExchangeOptions struct {
	Durable           bool
	AutoDelete        bool
	Internal          bool
	NoWait            bool
	Args              amqp.Table
	ConnectionTimeout int
}

type BindOptions struct {
	Queue  string
	Key    string
	NoWait bool
	Args   amqp.Table
}

type ExchangeBindOptions struct {
	Key  string
	Args amqp.Table
}

var DefaultExchangeOptions = ExchangeOptions{
	Durable:           true,
	AutoDelete:        false,
	Internal:          false,
	NoWait:            false,
	Args:              nil,
	ConnectionTimeout: 20,
}

var DefaultBindOptions = BindOptions{
	NoWait: false,
	Args:   nil,
}
