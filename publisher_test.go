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

	"github.com/Hanaboso/rabbitmq"
)

func nilErr(t *testing.T, err error, args ...interface{}) {
	if err != nil {
		t.Fatalf("%s: %v", fmt.Sprint(args...), err)
	}
}

func TestPublish(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const dataSourceName = "amqp://guest:guest@localhost:5672/"
	conn, err := rabbitmq.ConnectCtx(ctx, dataSourceName,
		rabbitmq.SetLogger(log.New(os.Stdout, "[RabbitMQ]", log.LstdFlags), rabbitmq.Debug),
	)
	nilErr(t, err, "failed to connect")
	defer conn.Close()

	const (
		exchangeName = "my awesome exchange"
		routingKey   = "routing.key.one"
	)
	nilErr(t, conn.ExchangeDeclare(exchangeName, rabbitmq.ExchangeTopic, rabbitmq.SetExchangeDurability(true)), "exchange declaration failed")

	pub, err := rabbitmq.NewPublisher(conn)
	nilErr(t, err)
	defer pub.Close()

	msg := map[string]interface{}{
		"message": "Hello, Consumer!",
	}

	var buff bytes.Buffer
	nilErr(t, json.NewEncoder(&buff).Encode(msg))

	nilErr(t, pub.PublishCtx(ctx, exchangeName, routingKey, buff.Bytes()),
		rabbitmq.SetPublishingDeliveryMode(rabbitmq.Persistent),
		rabbitmq.SetPublishingContentType("application/json"),
		func(p *rabbitmq.Publishing) {
			p.ContentEncoding = "utf-8"
		},
	)
}
