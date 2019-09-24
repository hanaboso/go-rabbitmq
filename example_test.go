package rabbitmq_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/Hanaboso/rabbitmq"
)

func ExamplePublish() {
	const dataSourceName = "amqp://guest:guest@localhost:5672/"
	conn, err := rabbitmq.Connect(dataSourceName,
		rabbitmq.SetLogger(log.New(os.Stdout, "[RabbitMQ]", log.LstdFlags), rabbitmq.Debug),
	)
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
		"message": "Hello, Consumer!",
	}

	var buff bytes.Buffer
	if err := json.NewEncoder(&buff).Encode(msg); err != nil {
		fmt.Printf("failed to encode message: %v", err)
		return
	}

	if err := pub.Publish("", "my.routing.key", buff.Bytes()); err != nil {
		fmt.Printf("failed to publish: %v", err)
		return
	}
}

func ExampleSubscribe() {
	const dataSourceName = "amqp://guest:guest@localhost:5672/"
	conn, err := rabbitmq.Connect(dataSourceName,
		rabbitmq.SetLogger(log.New(os.Stdout, "[RabbitMQ]", log.LstdFlags), rabbitmq.Debug),
	)
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

	q, err := conn.QueueDeclare("my.routing.key", rabbitmq.SetQueueDurability(true))
	if err != nil {
		fmt.Printf("failed to declare queue: %v", err)
		return
	}

	msgs, err := sub.Subscribe(q.Name)
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
