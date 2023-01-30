package rabbitmq

import (
	"github.com/hanaboso/go-rabbitmq/pkg/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"testing"
)

const publish_key = "publisher-test"

var (
	publisher *rabbitmq.Publisher
	exchange  rabbitmq.Exchange
)

func TestPublisher(t *testing.T) {
	t.Skip("")
	preparePublisher()
	t.Run("reconnect", testReconnectPublisher)
	t.Run("recreate", testRecreateExchange)
}

func testReconnectPublisher(t *testing.T) {
	checkPublisher(t)
	closeConnection()
	checkPublisher(t)
}

func testRecreateExchange(t *testing.T) {
	checkPublisher(t)
	assert.Equal(t, nil, client.DeleteExchange(exchange))
	checkPublisher(t)
}

func TestClosePublisher(t *testing.T) {
	preparePublisher()
}

func checkPublisher(t *testing.T) {
	err := publisher.Publish(amqp.Publishing{})
	assert.Equal(t, nil, err)
}

func preparePublisher() {
	publisher = client.NewPublisher(publish_key, "")
	exchange = rabbitmq.Exchange{
		Name: publish_key,
		Kind: amqp.ExchangeDirect,
	}

	client.AddExchange(exchange)
	if err := client.InitializeQueuesExchanges(); err != nil {
		panic(err)
	}
}
