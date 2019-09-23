package rabbitmq_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/Hanaboso/rabbitmq"
)

func nilErr(t *testing.T, err error, args ...interface{}) {
	if err != nil {
		t.Fatalf("%s: %v", fmt.Sprint(args...), err)
	}
}

func TestPublish(t *testing.T) {
	dsn := "amqp://guest:guest@localhost:5672/"
	logger := log.New(os.Stdout, "[RabbitMQ]", log.LstdFlags)
	conn, err := rabbitmq.Connect(dsn, rabbitmq.SetLogger(logger, rabbitmq.Debug))
	nilErr(t, err, "failed to connect")
	defer conn.Close()

	pub, err := rabbitmq.NewPublisher(conn, rabbitmq.SetDeliveryMode(rabbitmq.Persistent))
	nilErr(t, err)
	defer pub.Close()

	s := struct {
		Key string `json:"key"`
	}{Key: "routing.key.1"}

	var buff bytes.Buffer
	nilErr(t, json.NewEncoder(&buff).Encode(s))

	nilErr(t, pub.Publish("exch", s.Key, buff.Bytes()))
}
