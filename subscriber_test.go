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

	dsn := "amqp://guest:guest@localhost:5672/"
	logger := log.New(os.Stdout, "[RabbitMQ]", log.LstdFlags)
	conn, err := rabbitmq.ConnectCtx(ctx, dsn, rabbitmq.SetLogger(logger, rabbitmq.Debug))
	nilErr(t, err)
	defer conn.Close()

	sub, err := rabbitmq.NewSubscriber(conn)
	nilErr(t, err)
	defer sub.Close()

	conf := rabbitmq.QueueConfig{
		Name: "doesn't matter",
		RoutingKeys: map[string][]string{
			"exch": {
				"routing.key.1",
			},
		},
		Consumer: "Hello, i'm Mr. Meeseek, look at me!",
		Durable:  true,
	}
	msgs, err := sub.Subscribe(conf)
	nilErr(t, err)
	for msg := range msgs {
		var s struct {
			Key string `json:"key"`
		}
		nilErr(t, json.NewDecoder(&msg).Decode(&s))
		fmt.Println("Key", s.Key)

		//nilErr(t, msg.Nack(false), "NACK failed")
		nilErr(t, msg.Ack(), "ACK failed")
	}
}
