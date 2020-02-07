package pkg_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/hanaboso/go-log/pkg/zap"
	rabbitmq "github.com/hanaboso/go-rabbitmq/pkg"
	"github.com/streadway/amqp"
	"time"
)

func ExamplePublisher_Publish() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const dataSourceName = "amqp://guest:guest@localhost:5672/"
	conn, err := rabbitmq.Connect(ctx, dataSourceName,
		rabbitmq.ConnectionWithLogger(zap.NewLogger()),
		rabbitmq.ConnectionWithConfig(amqp.Config{}),
		rabbitmq.ConnectionWithCustomBackOff(time.Millisecond, time.Second, 1, true),
	)
	if err != nil {
		fmt.Printf("connection failed: %v", err)
		return
	}
	await := make(chan struct{})
	defer func() {
		_ = conn.Close()
		await <- struct{}{}
	}()

	pub, err := rabbitmq.NewPublisher(ctx, conn)
	if err != nil {
		fmt.Printf("failed to create publisher: %v", err)
		return
	}
	defer func() { _ = pub.Close() }()

	msg := map[string]interface{}{
		"message": "Hello, Consumer!",
	}

	var buff bytes.Buffer
	if err := json.NewEncoder(&buff).Encode(msg); err != nil {
		fmt.Printf("failed to encode message: %v", err)
		return
	}

	if err := pub.Publish(ctx, "", "my.routing.key", buff.Bytes(),
		rabbitmq.PublishingWithDeliveryMode(rabbitmq.Persistent),
	); err != nil {
		fmt.Printf("failed to publish: %v", err)
		return
	}

	<-await
}

func ExampleSubscriber_Subscribe() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const dataSourceName = "amqp://guest:guest@localhost:5672/"
	conn, err := rabbitmq.Connect(ctx, dataSourceName,
		rabbitmq.ConnectionWithLogger(zap.NewLogger()),
	)
	if err != nil {
		fmt.Printf("connection failed: %v", err)
		return
	}
	defer func() { _ = conn.Close() }()

	sub, err := rabbitmq.NewSubscriber(ctx, conn)
	if err != nil {
		fmt.Printf("failed to create subscriber: %v", err)
		return
	}
	defer func() { _ = sub.Close() }()

	q, err := conn.QueueDeclare(ctx, rabbitmq.NewQueue("my.routing.key", rabbitmq.QueueWithDurability(true)))
	if err != nil {
		fmt.Printf("failed to declare queue: %v", err)
		return
	}

	msgs, err := sub.Subscribe(ctx, &q)
	if err != nil {
		fmt.Printf("failed to init subscribe: %v", err)
		return
	}
	for msg := range msgs {
		var s struct {
			Message string `json:"message"`
		}

		if err := json.NewDecoder(&msg).Decode(&s); err != nil {
			fmt.Printf("failed to decode message: %v", err)
			return
		}
		fmt.Println("received message:", s)

		if err := msg.Ack(); err != nil {
			fmt.Printf("ACK failed: %v", err)
			return
		}
	}
}
