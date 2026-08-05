package rabbitmq

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/hanaboso/go-log/pkg/zap"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
)

func TestChannelRequestRefresh(t *testing.T) {
	client := brokerClient(t)
	defer client.Close()

	queueName := "batch-refresh"
	prepareRefreshQueue(t, client, queueName)

	publisher := client.NewPublisherOpt("", queueName, PublisherOptions{RetryAttempts: 0})
	assert.Nil(t, publisher.PublishBatch(refreshBatch(3)))

	oldChannel, _ := publisher.channel.snapshot()
	publisher.channel.requestRefresh(10 * time.Second)

	ch, _, live := awaitLiveChannel(publisher.channel)
	assert.True(t, live)
	assert.NotSame(t, oldChannel, ch)
	assert.True(t, oldChannel.IsClosed())
	assert.Equal(t, uint64(0), publisher.channel.deliveryTag)

	assert.Nil(t, publisher.Publish(amqp.Publishing{Body: []byte("{}")}))
	assert.Nil(t, publisher.PublishBatch(refreshBatch(5)))
	assert.Equal(t, 9, refreshQueueDepth(t, client, queueName))
}

func TestBatchTimeoutRefreshesChannel(t *testing.T) {
	client := brokerClient(t)
	defer client.Close()

	queueName := "batch-refresh-timeout"
	prepareRefreshQueue(t, client, queueName)

	publisher := client.NewPublisherOpt("", queueName, PublisherOptions{Timeout: -1, RetryAttempts: 1})
	oldChannel, _ := publisher.channel.snapshot()

	err := publisher.PublishBatch(refreshBatch(50))

	var batchErr *BatchPublishError
	assert.True(t, errors.As(err, &batchErr))
	assert.NotEmpty(t, batchErr.FailedIndexes)

	ch, _, live := awaitLiveChannel(publisher.channel)
	assert.True(t, live)
	assert.NotSame(t, oldChannel, ch)
}

func TestSinglePublishTimeoutDoesNotDesyncBatch(t *testing.T) {
	client := brokerClient(t)
	defer client.Close()

	queueName := "single-timeout-desync"
	queue := Queue{Name: queueName, Options: QueueOptions{
		Durable: true,
		Args: amqp.Table{
			"x-max-length": int32(3),
			"x-overflow":   "reject-publish",
		},
	}}
	_ = client.DeleteQueue(queue)
	client.AddQueue(queue)
	assert.Nil(t, client.DeclareQueue(queue))

	publisher := client.NewPublisherOpt("", queueName, PublisherOptions{RetryAttempts: 0})

	publisher.timeout = -1
	assert.NotNil(t, publisher.Publish(amqp.Publishing{Body: []byte("{}")}))

	publisher.timeout = 10
	err := publisher.PublishBatch(refreshBatch(5))

	var batchErr *BatchPublishError
	assert.True(t, errors.As(err, &batchErr))
	assert.Equal(t, []int{2, 3, 4}, batchErr.FailedIndexes)
	assert.Equal(t, 3, refreshQueueDepth(t, client, queueName))
}

func TestPublishAfterRefresh(t *testing.T) {
	client := brokerClient(t)
	defer client.Close()

	queueName := "publish-after-refresh"
	prepareRefreshQueue(t, client, queueName)

	publisher := client.NewPublisherOpt("", queueName, PublisherOptions{RetryAttempts: 0})
	assert.Nil(t, publisher.Publish(amqp.Publishing{Body: []byte("{}")}))

	publisher.channel.requestRefresh(10 * time.Second)

	assert.Nil(t, publisher.Publish(amqp.Publishing{Body: []byte("{}")}))
	assert.Equal(t, 2, refreshQueueDepth(t, client, queueName))
}

func TestRefreshExchangeClosedPublisher(t *testing.T) {
	client := brokerClient(t)
	defer client.Close()

	publisher := client.NewPublisherOpt("", "refresh-closed", PublisherOptions{RetryAttempts: 0})
	publisher.Close()

	start := time.Now()
	assert.NotNil(t, publisher.refreshExchange())
	assert.Less(t, time.Since(start), 10*time.Second)
}

func TestPublishBatchSurvivesPublisherClose(t *testing.T) {
	client := brokerClient(t)
	defer client.Close()

	queueName := "batch-survives-close"
	prepareRefreshQueue(t, client, queueName)

	publisher := client.NewPublisherOpt("", queueName, PublisherOptions{RetryAttempts: 0})

	done := make(chan error, 1)
	go func() {
		done <- publisher.PublishBatch(refreshBatch(2000))
	}()
	go func() {
		time.Sleep(20 * time.Millisecond)
		publisher.Close()
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("PublishBatch did not terminate after Publisher.Close")
	}
}

func brokerClient(t *testing.T) *Client {
	dsn := os.Getenv("RABBITMQ_DSN")
	if dsn == "" {
		t.Skip("RABBITMQ_DSN not set")
	}

	return NewClient(dsn, zap.NewLogger(), true)
}

func prepareRefreshQueue(t *testing.T, client *Client, queueName string) {
	queue := Queue{Name: queueName, Options: DefaultQueueOptions}
	_ = client.DeleteQueue(queue)
	client.AddQueue(queue)
	assert.Nil(t, client.DeclareQueue(queue))
}

func refreshBatch(count int) []amqp.Publishing {
	messages := make([]amqp.Publishing, count)
	for i := range messages {
		messages[i] = amqp.Publishing{Body: []byte("{}")}
	}

	return messages
}

func refreshQueueDepth(t *testing.T, client *Client, queueName string) int {
	channel, err := client.RawConnection().Channel()
	assert.Nil(t, err)
	defer func() {
		_ = channel.Close()
	}()

	state, err := channel.QueueInspect(queueName)
	assert.Nil(t, err)

	return state.Messages
}
