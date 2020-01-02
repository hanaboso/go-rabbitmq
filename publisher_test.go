package rabbitmq_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/hanaboso-go/rabbitmq"
)

func nilErr(t *testing.T, err error, args ...interface{}) {
	if err != nil {
		t.Fatalf("%s: %v", fmt.Sprint(args...), err)
	}
}

func TestPublish(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dataSourceName := DSNForTest(t)
	conn, err := rabbitmq.Connect(ctx, dataSourceName,
		rabbitmq.ConnectionWithLogger(log.New(os.Stdout, "[RabbitMQ]", log.LstdFlags), rabbitmq.Debug),
	)
	nilErr(t, err, "failed to connect")
	defer conn.Close()

	const (
		exchangeName = "my awesome exchange"
		routingKey   = "routing.key.one"
	)
	nilErr(t, conn.ExchangeDeclare(ctx, exchangeName, rabbitmq.ExchangeTopic, rabbitmq.ExchangeWithDurable(true)), "exchange declaration failed")

	pub, err := rabbitmq.NewPublisher(ctx, conn)
	nilErr(t, err)
	defer pub.Close()

	msg := map[string]interface{}{
		"message": "Hello, Consumer!",
	}

	var buff bytes.Buffer
	nilErr(t, json.NewEncoder(&buff).Encode(msg))

	nilErr(t, pub.Publish(ctx, exchangeName, routingKey, buff.Bytes(),
		rabbitmq.PublishingWithDeliveryMode(rabbitmq.Persistent),
		rabbitmq.PublishingWithContentType("application/json"),
		func(p *rabbitmq.Publishing) {
			p.ContentEncoding = "utf-8"
		},
	), "failed to publish message")
}
