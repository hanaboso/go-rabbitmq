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

func TestPublish(t *testing.T) {
	preparePublisher()

	//time.Sleep(1 * time.Second)

	message := amqp.Publishing{
		Headers: amqp.Table{
			"foo": "bar",
			"h1":  "k1",
		},
	}

	err := publisher.Publish(message)
	assert.Nil(t, err)
}

func checkPublisher(t *testing.T) {
	err := publisher.Publish(amqp.Publishing{})
	assert.Equal(t, nil, err)
}

func preparePublisher() {
	queue := rabbitmq.Queue{
		Name:    "test_queue",
		Options: rabbitmq.DefaultQueueOptions,
	}

	client.AddQueue(queue)

	publisher = client.NewPublisher(publish_key, "1")
	exchange := rabbitmq.Exchange{
		Name: publish_key,
		Kind: amqp.ExchangeDirect,
		Bindings: []rabbitmq.BindOptions{
			{
				Queue:  "test_queue",
				Key:    "1",
				NoWait: false,
				Args:   nil,
			},
		},
	}

	client.AddExchange(exchange)
	if err := client.InitializeQueuesExchanges(); err != nil {
		panic(err)
	}
}
