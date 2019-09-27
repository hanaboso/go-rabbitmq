package rabbitmq_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/Hanaboso/rabbitmq"
)

func TestSubscribe(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	const dataSourceName = "amqp://guest:guest@localhost:5672/"
	conn, err := rabbitmq.ConnectCtx(ctx, dataSourceName,
		rabbitmq.WithLogger(log.New(os.Stdout, "[RabbitMQ]", log.LstdFlags), rabbitmq.Debug),
	)
	nilErr(t, err, "connection failed")
	defer conn.Close()

	sub, err := rabbitmq.NewSubscriber(conn)
	nilErr(t, err, "failed to create subscriber")
	defer sub.Close()

	const (
		queueName    = "" // empty for random name
		exchangeName = "my awesome exchange"
		routingKey   = "routing.key.one"
	)
	q, err := conn.QueueDeclareCtx(ctx, queueName, rabbitmq.SetQueueAutoDelete(true))
	nilErr(t, err, "queue declaration failed")
	nilErr(t, conn.ExchangeDeclareCtx(ctx, exchangeName, rabbitmq.ExchangeTopic, rabbitmq.SetExchangeDurability(true)), "exchange declaration failed")
	nilErr(t, conn.QueueBindCtx(ctx, q.Name, routingKey, exchangeName), "queue bind failed")

	msgs, err := sub.SubscribeCtx(ctx, q.Name, rabbitmq.SetSubscriptionConsumer("Hello, i'm Mr. Meeseek, look at me!"))
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
