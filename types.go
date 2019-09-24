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
	ArgumentsTable amqp.Table
	Headers        amqp.Table
)

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

func SetPublishingDeliveryMode(mode DeliveryMode) func(*Publishing) {
	return func(p *Publishing) {
		p.DeliveryMode = mode
	}
}

func SetPublishingContentType(contentType string) func(*Publishing) {
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
	Args      ArgumentsTable
}

func SetSubscriptionConsumer(consumer string) func(*Subscription) {
	return func(subscription *Subscription) {
		subscription.Consumer = consumer
	}
}

// TODO define more options

type Exchange struct {
	Name       string
	Type       ExchangeType
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       ArgumentsTable
}

func SetExchangeDurability(durable bool) func(*Exchange) {
	return func(ex *Exchange) {
		ex.Durable = durable
	}
}

// TODO define more options

type Queue struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       ArgumentsTable
}

func SetQueueDurability(durable bool) func(*Queue) {
	return func(config *Queue) {
		config.Durable = durable
	}
}

func SetQueueAutoDelete(autoDelete bool) func(*Queue) {
	return func(config *Queue) {
		config.AutoDelete = autoDelete
	}
}

// TODO define more options

type Binding struct {
	Name       string
	RoutingKey string
	Exchange   string
	NoWait     bool
	Args       ArgumentsTable
}

// TODO define options
