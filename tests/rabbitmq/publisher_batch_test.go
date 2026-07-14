package rabbitmq

import (
	"errors"
	"testing"
	"time"

	"github.com/hanaboso/go-rabbitmq/pkg/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
)

func TestPublishBatch(t *testing.T) {
	queueName := "batch-test"
	exchangeName := "batch-test-ex"
	publisher := prepareBatch(t, queueName, exchangeName)

	assert.Nil(t, publisher.PublishBatch(batchMessages(1000)))
	assert.Equal(t, 1000, queueDepth(t, queueName))

	assert.Nil(t, publisher.Publish(amqp.Publishing{Body: []byte("{}")}))
	assert.Equal(t, 1001, queueDepth(t, queueName))
}

func TestPublishBatchInterleaved(t *testing.T) {
	queueName := "batch-interleaved"
	prepareBatchQueue(t, queueName, rabbitmq.DefaultQueueOptions)
	publisher := client.NewPublisherOpt("", queueName, rabbitmq.PublisherOptions{RetryAttempts: 0})

	assert.Nil(t, publisher.Publish(amqp.Publishing{}))
	assert.Nil(t, publisher.PublishBatch(batchMessages(10)))
	assert.Nil(t, publisher.Publish(amqp.Publishing{}))
	assert.Nil(t, publisher.PublishBatch(batchMessages(10)))
	assert.Equal(t, 22, queueDepth(t, queueName))
}

func TestPublishBatchNackMapping(t *testing.T) {
	queueName := "batch-overflow"
	prepareBatchQueue(t, queueName, rabbitmq.QueueOptions{
		Durable: true,
		Args: amqp.Table{
			"x-max-length": int32(5),
			"x-overflow":   "reject-publish",
		},
	})
	publisher := client.NewPublisherOpt("", queueName, rabbitmq.PublisherOptions{RetryAttempts: 0})

	err := publisher.PublishBatch(batchMessages(10))

	var batchErr *rabbitmq.BatchPublishError
	assert.True(t, errors.As(err, &batchErr))
	assert.Equal(t, []int{5, 6, 7, 8, 9}, batchErr.FailedIndexes)
	assert.Equal(t, 5, queueDepth(t, queueName))
}

func TestPublishBatchRecreateExchange(t *testing.T) {
	queueName := "batch-recreate"
	exchangeName := "batch-recreate-ex"
	publisher := prepareBatch(t, queueName, exchangeName)

	channel, err := client.RawConnection().Channel()
	assert.Nil(t, err)
	assert.Nil(t, channel.ExchangeDelete(exchangeName, false, false))
	_ = channel.Close()

	assert.Nil(t, publisher.PublishBatch(batchMessages(10)))
	assert.Equal(t, 10, queueDepth(t, queueName))
}

func TestPublishBatchEmptyAndSingle(t *testing.T) {
	queueName := "batch-single"
	prepareBatchQueue(t, queueName, rabbitmq.DefaultQueueOptions)
	publisher := client.NewPublisherOpt("", queueName, rabbitmq.PublisherOptions{RetryAttempts: 0})

	assert.Nil(t, publisher.PublishBatch(nil))
	assert.Nil(t, publisher.PublishBatch([]amqp.Publishing{}))
	assert.Equal(t, 0, queueDepth(t, queueName))

	assert.Nil(t, publisher.PublishBatch(batchMessages(1)))
	assert.Equal(t, 1, queueDepth(t, queueName))
}

func TestPublishBatchFasterThanLoop(t *testing.T) {
	queueName := "batch-speed"
	prepareBatchQueue(t, queueName, rabbitmq.DefaultQueueOptions)
	publisher := client.NewPublisherOpt("", queueName, rabbitmq.PublisherOptions{RetryAttempts: 0})

	const count = 200
	messages := batchMessages(count)

	loopStart := time.Now()
	for _, message := range messages {
		assert.Nil(t, publisher.Publish(message))
	}
	loopDuration := time.Since(loopStart)

	batchStart := time.Now()
	assert.Nil(t, publisher.PublishBatch(messages))
	batchDuration := time.Since(batchStart)

	t.Logf("loop of %d publishes: %v, one batch: %v", count, loopDuration, batchDuration)
	assert.Equal(t, 2*count, queueDepth(t, queueName))
	assert.Less(t, 2*batchDuration, loopDuration)
}

func batchMessages(count int) []amqp.Publishing {
	messages := make([]amqp.Publishing, count)
	for i := range messages {
		messages[i] = amqp.Publishing{Body: []byte("{}")}
	}

	return messages
}

func prepareBatchQueue(t *testing.T, queueName string, options rabbitmq.QueueOptions) {
	queue := rabbitmq.Queue{Name: queueName, Options: options}
	_ = client.DeleteQueue(queue)
	client.AddQueue(queue)
	assert.Nil(t, client.DeclareQueue(queue))
}

func prepareBatch(t *testing.T, queueName, exchangeName string) *rabbitmq.Publisher {
	prepareBatchQueue(t, queueName, rabbitmq.DefaultQueueOptions)
	exchange := rabbitmq.Exchange{
		Name: exchangeName,
		Kind: amqp.ExchangeDirect,
		Bindings: []rabbitmq.BindOptions{
			{
				Queue: queueName,
				Key:   "1",
			},
		},
	}
	client.AddExchange(exchange)
	assert.Nil(t, client.DeclareExchange(exchange))

	return client.NewPublisherOpt(exchangeName, "1", rabbitmq.PublisherOptions{RetryAttempts: 3})
}

func queueDepth(t *testing.T, queueName string) int {
	channel, err := client.RawConnection().Channel()
	assert.Nil(t, err)
	defer func() {
		_ = channel.Close()
	}()

	state, err := channel.QueueInspect(queueName)
	assert.Nil(t, err)

	return state.Messages
}
