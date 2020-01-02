package rabbitmq_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/hanaboso-go/rabbitmq"
)

func ExamplePublisher_Publish() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const dataSourceName = "amqp://guest:guest@localhost:5672/"
	conn, err := rabbitmq.Connect(ctx, dataSourceName,
		rabbitmq.ConnectionWithLogger(log.New(os.Stdout, "[RabbitMQ]", log.LstdFlags), rabbitmq.Debug),
	)
	if err != nil {
		fmt.Printf("connection failed: %v", err)
		return
	}
	defer conn.Close()

	pub, err := rabbitmq.NewPublisher(ctx, conn)
	if err != nil {
		fmt.Printf("failed to create publisher: %v", err)
		return
	}
	defer pub.Close()

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
}

func ExampleSubscriber_Subscribe() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const dataSourceName = "amqp://guest:guest@localhost:5672/"
	conn, err := rabbitmq.Connect(ctx, dataSourceName,
		rabbitmq.ConnectionWithLogger(log.New(os.Stdout, "[RabbitMQ]", log.LstdFlags), rabbitmq.Debug),
	)
	if err != nil {
		fmt.Printf("connection failed: %v", err)
		return
	}
	defer conn.Close()

	sub, err := rabbitmq.NewSubscriber(ctx, conn)
	if err != nil {
		fmt.Printf("failed to create subscriber: %v", err)
		return
	}
	defer sub.Close()

	q, err := conn.QueueDeclare(ctx, "my.routing.key", rabbitmq.QueueWithDurability(true))
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
