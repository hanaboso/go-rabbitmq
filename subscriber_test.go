package rabbitmq_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/hanaboso-go/rabbitmq"
)

func TestSubscribe(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	dataSourceName := DSNForTest(t)
	conn, err := rabbitmq.Connect(ctx, dataSourceName,
		rabbitmq.ConnectionWithLogger(log.New(os.Stdout, "[RabbitMQ]", log.LstdFlags), rabbitmq.Debug),
	)
	nilErr(t, err, "connection failed")
	defer conn.Close()

	sub, err := rabbitmq.NewSubscriber(ctx, conn)
	nilErr(t, err, "failed to create subscriber")
	defer sub.Close()

	const (
		queueName    = "" // empty for random name
		exchangeName = "my awesome exchange"
		routingKey   = "routing.key.one"
	)
	q, err := conn.QueueDeclare(ctx, queueName, rabbitmq.QueueWithAutoDelete(true))
	nilErr(t, err, "queue declaration failed")
	nilErr(t, conn.ExchangeDeclare(ctx, exchangeName, rabbitmq.ExchangeTopic, rabbitmq.ExchangeWithDurable(true)), "exchange declaration failed")
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
