package pkg_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/hanaboso/go-log/pkg/zap"
	"testing"
	"time"

	rabbitmq "github.com/hanaboso/go-rabbitmq/pkg"
)

func nilErr(t *testing.T, err error, args ...interface{}) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", fmt.Sprint(args...), err)
	}
}

func TestPublish(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := rabbitmq.Connect(
		ctx,
		DSNForTest(t),
		rabbitmq.ConnectionWithLogger(zap.NewLogger()),
	)
	nilErr(t, err, "failed to connect")
	defer func() { _ = conn.Close() }()

	const (
		exchangeName = "my awesome exchange"
		routingKey   = "routing.key.one"
	)
	nilErr(t, conn.ExchangeDeclare(ctx, rabbitmq.NewExchange(exchangeName, rabbitmq.ExchangeTopic, rabbitmq.ExchangeWithDurable(true))), "exchange declaration failed")

	pub, err := rabbitmq.NewPublisher(ctx, conn)
	nilErr(t, err)
	defer func() { _ = pub.Close() }()

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
