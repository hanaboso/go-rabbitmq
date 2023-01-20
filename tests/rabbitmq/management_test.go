package rabbitmq

import (
	"github.com/hanaboso/go-rabbitmq/pkg/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"testing"
)

const declaration_key = "declaration-test"

func TestDeclare(t *testing.T) {
	t.Skip("")
	_ = client.DeleteQueue(rabbitmq.Queue{
		Name: declaration_key,
	})
	_ = client.DeleteExchange(rabbitmq.Exchange{
		Name: declaration_key,
	})

	t.Run("queue", testDeclareQueue)
	t.Run("exchange", testDeclareExchange)
}

func testDeclareQueue(t *testing.T) {
	queue := rabbitmq.Queue{
		Name:    declaration_key,
		Options: rabbitmq.DefaultQueueOptions,
	}

	err := client.DeclareQueue(queue)
	assert.Equal(t, nil, err)
}

func testDeclareExchange(t *testing.T) {
	exchange := rabbitmq.Exchange{
		Name:    declaration_key,
		Kind:    amqp.ExchangeDirect,
		Options: rabbitmq.DefaultExchangeOptions,
		Bindings: []rabbitmq.BindOptions{
			{
				Queue: declaration_key,
				Key:   "",
			},
		},
	}

	err := client.DeclareExchange(exchange)
	assert.Equal(t, nil, err)
}
