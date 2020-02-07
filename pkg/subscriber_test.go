package pkg_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/hanaboso/go-log/pkg/zap"
	"testing"
	"time"

	rabbitmq "github.com/hanaboso/go-rabbitmq/pkg"
)

func TestSubscribe(t *testing.T) {
	const (
		queueName    = "subscribe-test" // empty for random name
		exchangeName = "my awesome exchange"
		routingKey   = "routing.key.one"
	)

	ctx := createCtx()
	conn, err := rabbitmq.Connect(
		ctx,
		DSNForTest(t),
		rabbitmq.ConnectionWithLogger(zap.NewLogger()),
		rabbitmq.ConnectionWithPrefetchLimit(20),
	)
	nilErr(t, err, "connection failed")
	defer func() { _ = conn.Close() }()

	sub, err := rabbitmq.NewSubscriber(ctx, conn, rabbitmq.SubscriberWithPrefetchLimit(10))
	nilErr(t, err, "failed to create subscriber")
	defer func() { _ = sub.Close() }()

	q, err := conn.QueueDeclare(ctx, rabbitmq.NewQueue(queueName, rabbitmq.QueueWithAutoDelete(true)))
	nilErr(t, err, "queue declaration failed")
	nilErr(t, conn.ExchangeDeclare(ctx, rabbitmq.NewExchange(exchangeName, rabbitmq.ExchangeTopic, rabbitmq.ExchangeWithDurable(true))), "exchange declaration failed")
	nilErr(t, conn.QueueBind(ctx, &q, routingKey, exchangeName), "queue bind failed")

	msgs, err := sub.Subscribe(ctx, &q, rabbitmq.SubscriptionWithConsumer("Hello, i'm Mr. Meeseek, look at me!"))
	nilErr(t, err, "subscription failed")
	for msg := range msgs {
		var s struct {
			Message string `json:"message"`
		}
		nilErr(t, json.NewDecoder(&msg).Decode(&s), "decode JSON failed")
		fmt.Printf("Message: %s\n", s.Message)

		nilErr(t, msg.Ack(), "ACK failed")
	}
}

func TestSubscribeQueueClassic(t *testing.T) {
	const (
		queueName = "subscribe-test-classic" // empty for random name
	)

	ctx := createCtx()
	logger := zap.NewLogger()
	conn, err := rabbitmq.Connect(
		ctx,
		DSNForTest(t),
		rabbitmq.ConnectionWithLogger(logger),
		rabbitmq.ConnectionWithPrefetchLimit(20),
	)
	nilErr(t, err, "connection failed")
	defer func() { _ = conn.Close() }()

	q := rabbitmq.NewQueue(queueName, rabbitmq.QueueWithAutoDelete(true), rabbitmq.QueueWithQuorumType())
	sub, err := rabbitmq.NewSubscriber(
		ctx,
		conn,
		rabbitmq.SubscriberWithPrefetchLimit(1),
		rabbitmq.SubscriberWithLogger(logger),
		rabbitmq.SubscriberAddQueue(q),
	)
	nilErr(t, err, "failed to create subscriber")
	defer func() { _ = sub.Close() }()

	msgs, err := sub.Subscribe(ctx, &q, rabbitmq.SubscriptionWithConsumer("Hello, i'm Mr. Meeseek, look at me!"))
	nilErr(t, err, "subscription failed")
	for msg := range msgs {
		var s struct {
			Message string `json:"message"`
		}
		nilErr(t, json.NewDecoder(&msg).Decode(&s), "decode JSON failed")
		fmt.Printf("Message: %s\n", s.Message)

		nilErr(t, msg.Ack(), "ACK failed")
	}
}

func TestSubscribeQueueQuorum(t *testing.T) {
	const (
		queueName = "subscribe-test-quorum" // empty for random name
	)

	ctx := createCtx()
	logger := zap.NewLogger()
	conn, err := rabbitmq.Connect(
		ctx,
		DSNForTest(t),
		rabbitmq.ConnectionWithLogger(logger),
		rabbitmq.ConnectionWithPrefetchLimit(20),
	)
	nilErr(t, err, "connection failed")
	defer func() { _ = conn.Close() }()

	q := rabbitmq.NewQueue(queueName, rabbitmq.QueueWithQuorumType())
	sub, err := rabbitmq.NewSubscriber(
		ctx,
		conn,
		rabbitmq.SubscriberWithPrefetchLimit(1),
		rabbitmq.SubscriberWithLogger(logger),
		rabbitmq.SubscriberAddQueue(q),
	)
	nilErr(t, err, "failed to create subscriber")
	defer func() { _ = sub.Close() }()

	msgs, err := sub.Subscribe(ctx, &q, rabbitmq.SubscriptionWithConsumer("Hello, i'm Mr. Meeseek, look at me!"))
	nilErr(t, err, "subscription failed")
	for msg := range msgs {
		var s struct {
			Message string `json:"message"`
		}
		nilErr(t, json.NewDecoder(&msg).Decode(&s), "decode JSON failed")
		fmt.Printf("Message: %s\n", s.Message)

		nilErr(t, msg.Ack(), "ACK failed")
	}
}

func TestSubscribeExchange(t *testing.T) {
	const (
		queueName    = "subscribe-test-exchange" // empty for random name
		exchangeName = "my awesome exchange"
		routingKey   = "routing.key.one"
	)

	ctx := createCtx()
	logger := zap.NewLogger()
	conn, err := rabbitmq.Connect(
		ctx,
		DSNForTest(t),
		rabbitmq.ConnectionWithLogger(logger),
		rabbitmq.ConnectionWithPrefetchLimit(20),
	)
	nilErr(t, err, "connection failed")
	defer func() { _ = conn.Close() }()

	sub, err := rabbitmq.NewSubscriber(
		ctx,
		conn,
		rabbitmq.SubscriberWithPrefetchLimit(10),
		rabbitmq.SubscriberWithLogger(logger),
		rabbitmq.SubscriberAddExchange(rabbitmq.NewExchange(exchangeName, rabbitmq.ExchangeTopic, rabbitmq.ExchangeWithDurable(true))),
	)
	nilErr(t, err, "failed to create subscriber")
	defer func() { _ = sub.Close() }()

	q, err := conn.QueueDeclare(ctx, rabbitmq.NewQueue(queueName, rabbitmq.QueueWithAutoDelete(true), rabbitmq.QueueWithClassicType()))
	nilErr(t, err, "queue declaration failed")
	nilErr(t, conn.QueueBind(ctx, &q, routingKey, exchangeName), "queue bind failed")

	msgs, err := sub.Subscribe(ctx, &q, rabbitmq.SubscriptionWithConsumer("Hello, i'm Mr. Meeseek, look at me!"))
	nilErr(t, err, "subscription failed")
	for msg := range msgs {
		var s struct {
			Message string `json:"message"`
		}
		nilErr(t, json.NewDecoder(&msg).Decode(&s), "decode JSON failed")
		fmt.Printf("Message: %s\n", s.Message)

		nilErr(t, msg.Ack(), "ACK failed")
	}
}

func createCtx() context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	return ctx
}
