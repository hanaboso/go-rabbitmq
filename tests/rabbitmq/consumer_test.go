package rabbitmq

import (
	"github.com/hanaboso/go-rabbitmq/pkg/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"testing"
)

const consumer_key = "consumer-test"

var (
	consumer         *rabbitmq.Consumer
	consumerMessages <-chan amqp.Delivery
	consumerPublish  *rabbitmq.Publisher
	queue            rabbitmq.Queue
)

func TestConsumer(t *testing.T) {
	t.Skip("")
	prepareConsumer()
	t.Run("reconnect", testReconnectQueue)
	t.Run("recreate", testRecreateQueue)
}

func testReconnectQueue(t *testing.T) {
	checkConsumer(t)
	closeConnection()
	checkConsumer(t)
}

func testRecreateQueue(t *testing.T) {
	checkConsumer(t)
	assert.Equal(t, nil, client.DeleteQueue(queue))
	checkConsumer(t)
}

func checkConsumer(t *testing.T) {
	err := consumerPublish.Publish(amqp.Publishing{})
	assert.Equal(t, nil, err)
	<-consumerMessages
}

func prepareConsumer() {
	consumerPublish = client.NewPublisherOpt("", consumer_key, rabbitmq.PublisherOptions{RetryAttempts: 5})
	queue = rabbitmq.Queue{
		Name:    consumer_key,
		Options: rabbitmq.DefaultQueueOptions,
	}

	client.AddQueue(queue)
	consumer = client.NewConsumer(consumer_key, 10)
	consumerMessages = consumer.Consume(true)
}
