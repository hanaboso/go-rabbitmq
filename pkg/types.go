package pkg

import (
	"io"
	"time"

	"github.com/streadway/amqp"
)

// ExchangeType is type for exchange type constants.
type ExchangeType string

// Constants for standard AMQP 0-9-1 exchange types.
const (
	ExchangeDirect  ExchangeType = amqp.ExchangeDirect
	ExchangeFanout  ExchangeType = amqp.ExchangeFanout
	ExchangeTopic   ExchangeType = amqp.ExchangeTopic
	ExchangeHeaders ExchangeType = amqp.ExchangeHeaders
)

type (
	// Arguments is wrappers for amqp.Table
	Arguments amqp.Table
	// Headers is wrappers for amqp.Table
	Headers amqp.Table
)

// Validate returns and error if any Go types in the table are incompatible with AMQP types.
func (a Arguments) Validate() error {
	return amqp.Table(a).Validate()
}

// Validate returns and error if any Go types in the table are incompatible with AMQP types.
func (h Headers) Validate() error {
	return amqp.Table(h).Validate()
}

// Message is wrapper for amqp.Delivery with io.Reader implementation.
type Message struct {
	amqp.Delivery
	// for io.Reader implementation
	readIndex int
}

// Bytes returns whole response as array of bytes.
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

// Ack is for message acknowledgment
func (m *Message) Ack() error {
	return m.Delivery.Ack(false)
}

// AckMultiple is for multiple acknowledgment.
// This message and all prior unacknowledged messages on the same channel will be acknowledged.
// This is useful for batch processing of deliveries.
func (m *Message) AckMultiple() error {
	return m.Delivery.Ack(true)
}

// Nack is for negatively message acknowledgment.
func (m *Message) Nack(requeue bool) error {
	return m.Delivery.Nack(false, requeue)
}

// NackMultiple is for multiple negatively message acknowledgment.
// Nack messages up to and including delivered messages up until the delivery tag delivered on the same channel.
func (m *Message) NackMultiple(requeue bool) error {
	return m.Delivery.Nack(true, requeue)
}

// DeliveryMode is type for delivery mode constants.
type DeliveryMode uint8

// Constants for standard AMQP 0-9-1 delivery modes.
const (
	Transient  DeliveryMode = 1
	Persistent DeliveryMode = 2
)

// Publishing contains all parameters for (*amqp.Channel).Publish method.
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
	CorrelationID   string
	ReplyTo         string
	Expiration      string
	MessageID       string
	Timestamp       time.Time
	Type            string
	UserID          string
	AppID           string
}

// PublishingWithMandatory provides publishing with mandatory.
func PublishingWithMandatory(mandatory bool) func(*Publishing) {
	return func(publishing *Publishing) {
		publishing.Mandatory = mandatory
	}
}

// PublishingWithImmediate provides publishing with immediate.
func PublishingWithImmediate(immediate bool) func(*Publishing) {
	return func(publishing *Publishing) {
		publishing.Immediate = immediate
	}
}

// PublishingWithHeaders provides publishing with headers.
func PublishingWithHeaders(headers Headers) func(*Publishing) {
	return func(publishing *Publishing) {
		publishing.Headers = headers
	}
}

// PublishingWithContentType provides publishing with contentType.
func PublishingWithContentType(contentType string) func(*Publishing) {
	return func(publishing *Publishing) {
		publishing.ContentType = contentType
	}
}

// PublishingWithContentEncoding provides publishing with contentEncoding.
func PublishingWithContentEncoding(contentEncoding string) func(*Publishing) {
	return func(publishing *Publishing) {
		publishing.ContentEncoding = contentEncoding
	}
}

// PublishingWithDeliveryMode provides publishing with deliveryMode.
func PublishingWithDeliveryMode(deliveryMode DeliveryMode) func(*Publishing) {
	return func(publishing *Publishing) {
		publishing.DeliveryMode = deliveryMode
	}
}

// PublishingWithAppID provides publishing with appId
func PublishingWithAppID(appID string) func(*Publishing) {
	return func(publishing *Publishing) {
		publishing.AppID = appID
	}
}

// PublishingWithType provides publishing with type
func PublishingWithType(pubType string) func(*Publishing) {
	return func(publishing *Publishing) {
		publishing.Type = pubType
	}
}

// Subscription contains all parameters for (*amqp.Channel).Consume method.
type Subscription struct {
	Queue     string
	Consumer  string
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      Arguments
}

// SubscriptionWithConsumer provides subscription with consumer.
func SubscriptionWithConsumer(consumer string) func(*Subscription) {
	return func(subscription *Subscription) {
		subscription.Consumer = consumer
	}
}

// SubscriptionWithAutoAck provides subscription with autoAck.
func SubscriptionWithAutoAck(autoAck bool) func(*Subscription) {
	return func(subscription *Subscription) {
		subscription.AutoAck = autoAck
	}
}

// SubscriptionWithExclusive provides subscription with exclusive.
func SubscriptionWithExclusive(exclusive bool) func(*Subscription) {
	return func(subscription *Subscription) {
		subscription.Exclusive = exclusive
	}
}

// SubscriptionWithNoLocal provides subscription with noLocal.
func SubscriptionWithNoLocal(noLocal bool) func(*Subscription) {
	return func(subscription *Subscription) {
		subscription.NoLocal = noLocal
	}
}

// SubscriptionWithNoWait provides subscription with noWait.
func SubscriptionWithNoWait(noWait bool) func(*Subscription) {
	return func(subscription *Subscription) {
		subscription.NoWait = noWait
	}
}

// SubscriptionWithArguments provides subscription with arguments.
func SubscriptionWithArguments(args Arguments) func(*Subscription) {
	return func(subscription *Subscription) {
		subscription.Args = args
	}
}

// Exchange contains all parameters for (*amqp.Channel).ExchangeDeclare method.
type Exchange struct {
	Name       string
	Type       ExchangeType
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       Arguments
}

// ExchangeWithDurable provides exchange with durable.
func ExchangeWithDurable(durable bool) func(*Exchange) {
	return func(exchange *Exchange) {
		exchange.Durable = durable
	}
}

// ExchangeWithAutoDelete provides exchange with autoDelete.
func ExchangeWithAutoDelete(autoDelete bool) func(*Exchange) {
	return func(exchange *Exchange) {
		exchange.AutoDelete = autoDelete
	}
}

// ExchangeWithInternal provides exchange with internal.
func ExchangeWithInternal(internal bool) func(*Exchange) {
	return func(exchange *Exchange) {
		exchange.Internal = internal
	}
}

// ExchangeWithNoWait provides exchange with noWait.
func ExchangeWithNoWait(noWait bool) func(*Exchange) {
	return func(exchange *Exchange) {
		exchange.NoWait = noWait
	}
}

// ExchangeWithArguments provides exchange with arguments.
func ExchangeWithArguments(args Arguments) func(*Exchange) {
	return func(exchange *Exchange) {
		exchange.Args = args
	}
}

// QueueType is type for queue type constants.
type QueueType string

// Constants for standard AMQP 0-9-1 x-queue-type.
const (
	QuorumType  QueueType = "quorum"
	ClassicType QueueType = "classic"
)

// Queue contains all parameters for (*amqp.Channel).QueueDeclare method.
type Queue struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       Arguments

	bindings []Binding
}

// QueueWithQuorumType wil declare Queue as quorum type.
func QueueWithQuorumType() func(*Queue) {
	return func(queue *Queue) {
		queue.Args["x-queue-type"] = string(QuorumType)
	}
}

// QueueWithClassicType wil declare Queue as classic type.
func QueueWithClassicType() func(*Queue) {
	return func(queue *Queue) {
		queue.Args["x-queue-type"] = string(ClassicType)
	}
}

// QueueWithDurability provides queue with durable.
func QueueWithDurability(durable bool) func(*Queue) {
	return func(queue *Queue) {
		queue.Durable = durable
	}
}

// QueueWithAutoDelete provides queue with autoDelete.
func QueueWithAutoDelete(autoDelete bool) func(*Queue) {
	return func(queue *Queue) {
		queue.AutoDelete = autoDelete
	}
}

// QueueWithExclusive provides queue with exclusive.
func QueueWithExclusive(exclusive bool) func(*Queue) {
	return func(queue *Queue) {
		queue.Exclusive = exclusive
	}
}

// QueueWithNoWait provides queue with noWait.
func QueueWithNoWait(noWait bool) func(*Queue) {
	return func(queue *Queue) {
		queue.NoWait = noWait
	}
}

// QueueWithArguments provides queue with arguments.
func QueueWithArguments(args Arguments) func(*Queue) {
	return func(queue *Queue) {
		queue.Args = args
	}
}

// Binding contains all parameters for (*amqp.Channel).QueueBind method.
type Binding struct {
	Name       string
	RoutingKey string
	Exchange   string
	NoWait     bool
	Args       Arguments
}

// BindingWithNoWait provides binding with noWait.
func BindingWithNoWait(noWait bool) func(*Binding) {
	return func(binding *Binding) {
		binding.NoWait = noWait
	}
}

// BindingWithArguments provides binding with arguments.
func BindingWithArguments(args Arguments) func(*Binding) {
	return func(binding *Binding) {
		binding.Args = args
	}
}
