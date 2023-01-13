```
package main

import (
	"github.com/hanaboso/go-log/pkg/zap"
	"github.com/hanaboso/go-rabbitmq/pkg/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	logger := zap.NewLogger()
	client := rabbitmq.NewClient("amqp://rabbitmq", logger, true)
	// 3rd param "singleConnection" decides whether or library should create separate connection/channels for
	// publishers and consumers for high load traffic

	queue := rabbitmq.Queue{
		Name:    "test",
		Options: rabbitmq.DefaultQueueOptions,
	}
	exchange := rabbitmq.Exchange{
		Name: "asd",
		Kind: "direct",
		Bindings: []rabbitmq.BindOptions{
			{
				Queue:  "test",
				Key:    "",
				NoWait: false,
				Args:   nil,
			},
		},
	}
	// Register all used queses & exchanges otherwise publisher/consumer cannot recover when queues are deleted
	// Consumer also needs to know queue specification in order to connect - just register all used
	client.AddExchange(exchange)
	client.AddQueue(queue)

	// Ensures that all Queues & Exchanges are created and valid
	_ = client.InitializeQueuesExchanges()

	publisher := client.NewPublisher("asd", "")
	_ = publisher.Publish(amqp.Publishing{})

	consumer := client.NewConsumer("test", 10)

	// Callback consumer
	type MessageContent struct {
		Losos string `json:"losos"`
	}

	callback := func(content *MessageContent, headers map[string]interface{}) rabbitmq.Acked {
		// Do what ever you need with a message
		return rabbitmq.Ack
	}

	coonsumer := rabbitmq.JsonConsumer[MessageContent]{consumer}
	go coonsumer.Consume(callback)

	// Manual/Auto Ack consumer
	for msg := range consumer.Consume(false) {
		// For "autoAck: false" consumer make sure to call Ack manually
		_ = msg.Ack(true)
	}
}
```
