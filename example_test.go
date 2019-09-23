package rabbitmq_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Hanaboso/rabbitmq"
)

func ExamplePublish() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dsn := "amqp://guest:guest@localhost:5672/"
	logger := log.New(os.Stdout, "[RabbitMQ]", log.LstdFlags)
	conn, err := rabbitmq.ConnectCtx(ctx, dsn, rabbitmq.SetLogger(logger, rabbitmq.Debug))
	if err != nil {
		fmt.Printf("connection failed: %v", err)
		return
	}
	defer conn.Close()

	pub, err := rabbitmq.NewPublisher(conn, rabbitmq.SetDeliveryMode(rabbitmq.Persistent))
	if err != nil {
		fmt.Printf("failed to create publisher: %v", err)
		return
	}
	defer pub.Close()

	msg := map[string]interface{}{
		"message": "Hi",
	}

	var buff bytes.Buffer
	if err := json.NewEncoder(&buff).Encode(msg); err != nil {
		fmt.Printf("failed to encode message: %v", err)
		return
	}

	if err := pub.PublishCtx(ctx, "", "routing.key", buff.Bytes()); err != nil {
		fmt.Printf("failed to publish: %v", err)
		return
	}
}

func ExampleSubscribe() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dsn := "amqp://guest:guest@localhost:5672/"
	logger := log.New(os.Stdout, "[RabbitMQ]", log.LstdFlags)
	conn, err := rabbitmq.Connect(dsn, rabbitmq.SetLogger(logger, rabbitmq.Debug))
	if err != nil {
		fmt.Printf("connection failed: %v", err)
		return
	}
	defer conn.Close()

	sub, err := rabbitmq.NewSubscriber(conn)
	if err != nil {
		fmt.Printf("failed to create subscriber: %v", err)
		return
	}
	defer sub.Close()

	conf := rabbitmq.QueueConfig{
		Name:    "routing.key",
		Durable: true,
	}

	msgs, err := sub.SubscribeCtx(ctx, conf)
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
