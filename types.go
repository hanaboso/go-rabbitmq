package rabbitmq

import (
	"io"
	"time"

	"github.com/streadway/amqp"
)

type ExchangeType string

const (
	ExchangeDirect  ExchangeType = amqp.ExchangeDirect
	ExchangeFanout  ExchangeType = amqp.ExchangeFanout
	ExchangeTopic   ExchangeType = amqp.ExchangeTopic
	ExchangeHeaders ExchangeType = amqp.ExchangeHeaders
)

type (
	Arguments amqp.Table
	Headers   amqp.Table
)

func (a Arguments) Validate() error {
	return amqp.Table(a).Validate()
}

func (h Headers) Validate() error {
	return amqp.Table(h).Validate()
}

type Message struct {
	amqp.Delivery
	// for io.Reader implementation
	readIndex int
}

func (m *Message) Bytes() []byte {
	return m.Delivery.Body
}

func (m *Message) Read(p []byte) (int, error) {
	if m.readIndex >= len(m.Delivery.Body) {
		return 0, io.EOF
	}

	n := copy(p, m.Delivery.Body[m.readIndex:])
	m.readIndex += n
	return n, nil
}

func (m *Message) Ack() error {
	return m.Delivery.Ack(false)
}

func (m *Message) AckMultiple() error {
	return m.Delivery.Ack(true)
}

func (m *Message) Nack(requeue bool) error {
	return m.Delivery.Nack(false, requeue)
}

func (m *Message) NackMultiple(requeue bool) error {
	return m.Delivery.Nack(true, requeue)
}

type DeliveryMode uint8

const (
	Transient  DeliveryMode = 1
	Persistent DeliveryMode = 2
)

type Publishing struct {
	Exchange        string
	RoutingKey      string
	Mandatory       bool
	Immediate       bool
	Body            []byte
	Headers         Headers
	ContentType     string
	ContentEncoding string
	DeliveryMode    DeliveryMode
	Priority        uint8
	CorrelationId   string
	ReplyTo         string
	Expiration      string
	MessageId       string
	Timestamp       time.Time
	Type            string
	UserId          string
	AppId           string
}

func PublishingWithDeliveryMode(mode DeliveryMode) func(*Publishing) {
	return func(p *Publishing) {
		p.DeliveryMode = mode
	}
}

func PublishingWithContentType(contentType string) func(*Publishing) {
	return func(p *Publishing) {
		p.ContentType = contentType
	}
}

// TODO define more options

type Subscription struct {
	Queue     string
	Consumer  string
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      Arguments
}

func SubscriptionWithConsumer(consumer string) func(*Subscription) {
	return func(subscription *Subscription) {
		subscription.Consumer = consumer
	}
}

func SubscriptionWithAutoAck(autoAck bool) func(*Subscription) {
	return func(subscription *Subscription) {
		subscription.AutoAck = autoAck
	}
}

func SubscriptionWithExclusive(exclusive bool) func(*Subscription) {
	return func(subscription *Subscription) {
		subscription.Exclusive = exclusive
	}
}

func SubscriptionWithNoLocal(noLocal bool) func(*Subscription) {
	return func(subscription *Subscription) {
		subscription.NoLocal = noLocal
	}
}

func SubscriptionWithNoWait(noWait bool) func(*Subscription) {
	return func(subscription *Subscription) {
		subscription.NoWait = noWait
	}
}

func SubscriptionWithArguments(args Arguments) func(*Subscription) {
	return func(subscription *Subscription) {
		subscription.Args = args
	}
}

type Exchange struct {
	Name       string
	Type       ExchangeType
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       Arguments
}

func ExchangeWithDurable(durable bool) func(*Exchange) {
	return func(exchange *Exchange) {
		exchange.Durable = durable
	}
}

func ExchangeWithAutoDelete(autoDelete bool) func(*Exchange) {
	return func(exchange *Exchange) {
		exchange.AutoDelete = autoDelete
	}
}

func ExchangeWithInternal(internal bool) func(*Exchange) {
	return func(exchange *Exchange) {
		exchange.Internal = internal
	}
}

func ExchangeWithNoWait(noWait bool) func(*Exchange) {
	return func(exchange *Exchange) {
		exchange.NoWait = noWait
	}
}

func ExchangeWithArguments(args Arguments) func(*Exchange) {
	return func(exchange *Exchange) {
		exchange.Args = args
	}
}

type Queue struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       Arguments
}

func QueueWithDurability(durable bool) func(*Queue) {
	return func(queue *Queue) {
		queue.Durable = durable
	}
}

func QueueWithAutoDelete(autoDelete bool) func(*Queue) {
	return func(queue *Queue) {
		queue.AutoDelete = autoDelete
	}
}

func QueueWithExclusive(exclusive bool) func(*Queue) {
	return func(queue *Queue) {
		queue.Exclusive = exclusive
	}
}

func QueueWithNoWait(noWait bool) func(*Queue) {
	return func(queue *Queue) {
		queue.NoWait = noWait
	}
}

func QueueWithArguments(args Arguments) func(*Queue) {
	return func(queue *Queue) {
		queue.Args = args
	}
}

type Binding struct {
	Name       string
	RoutingKey string
	Exchange   string
	NoWait     bool
	Args       Arguments
}

func BindingWithNoWait(noWait bool) func(*Binding) {
	return func(binding *Binding) {
		binding.NoWait = noWait
	}
}

func BindingWithArguments(args Arguments) func(*Binding) {
	return func(binding *Binding) {
		binding.Args = args
	}
}
