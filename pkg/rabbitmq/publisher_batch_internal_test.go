package rabbitmq

import (
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
)

func testChannel(deliveryTag uint64) *channel {
	return &channel{deliveryTag: deliveryTag, connection: &connection{open: true}}
}

func farDeadline() time.Time {
	return time.Now().Add(time.Minute)
}

func feedConfirms(confirmations ...amqp.Confirmation) chan amqp.Confirmation {
	feed := make(chan amqp.Confirmation, confirmBufferCap)
	for _, confirmation := range confirmations {
		feed <- confirmation
	}

	return feed
}

func TestBatchConfirmsAllAck(t *testing.T) {
	channelContainer := testChannel(5)
	confirms := &batchConfirms{channel: channelContainer, firstTag: 6, limit: 3}
	feed := feedConfirms(
		amqp.Confirmation{DeliveryTag: 6, Ack: true},
		amqp.Confirmation{DeliveryTag: 7, Ack: true},
		amqp.Confirmation{DeliveryTag: 8, Ack: true},
	)

	outcome := confirms.await(feed, 3, time.Second, farDeadline())

	assert.Equal(t, awaitDone, outcome)
	assert.False(t, confirms.closed)
	assert.Equal(t, 3, confirms.resolved)
	assert.Empty(t, confirms.failedOffsets)
	assert.Equal(t, uint64(8), channelContainer.deliveryTag)
}

func TestBatchConfirmsNackMapping(t *testing.T) {
	confirms := &batchConfirms{channel: testChannel(0), firstTag: 1, limit: 5}
	feed := feedConfirms(
		amqp.Confirmation{DeliveryTag: 1, Ack: true},
		amqp.Confirmation{DeliveryTag: 2, Ack: true},
		amqp.Confirmation{DeliveryTag: 3, Ack: false},
		amqp.Confirmation{DeliveryTag: 4, Ack: true},
		amqp.Confirmation{DeliveryTag: 5, Ack: false},
	)

	outcome := confirms.await(feed, 5, time.Second, farDeadline())

	assert.Equal(t, awaitDone, outcome)
	assert.Equal(t, 5, confirms.resolved)
	assert.Equal(t, []int{2, 4}, confirms.failedOffsets)
}

func TestBatchConfirmsSkipsStale(t *testing.T) {
	channelContainer := testChannel(3)
	confirms := &batchConfirms{channel: channelContainer, firstTag: 4, limit: 2}
	feed := feedConfirms(
		amqp.Confirmation{DeliveryTag: 2, Ack: false},
		amqp.Confirmation{DeliveryTag: 3, Ack: true},
		amqp.Confirmation{DeliveryTag: 4, Ack: true},
		amqp.Confirmation{DeliveryTag: 5, Ack: true},
	)

	outcome := confirms.await(feed, 2, time.Second, farDeadline())

	assert.Equal(t, awaitDone, outcome)
	assert.Equal(t, 2, confirms.resolved)
	assert.Empty(t, confirms.failedOffsets)
	assert.Equal(t, uint64(5), channelContainer.deliveryTag)
}

func TestBatchConfirmsClosedMidStream(t *testing.T) {
	confirms := &batchConfirms{channel: testChannel(0), firstTag: 1, limit: 3}
	feed := feedConfirms(amqp.Confirmation{DeliveryTag: 1, Ack: true})
	close(feed)

	outcome := confirms.await(feed, 3, time.Second, farDeadline())

	assert.Equal(t, awaitClosed, outcome)
	assert.True(t, confirms.closed)
	assert.Equal(t, 1, confirms.resolved)
}

func TestBatchConfirmsProgressResetsDeadline(t *testing.T) {
	confirms := &batchConfirms{channel: testChannel(0), firstTag: 1, limit: 5}
	feed := make(chan amqp.Confirmation)
	go func() {
		for tag := uint64(1); tag <= 5; tag++ {
			time.Sleep(50 * time.Millisecond)
			feed <- amqp.Confirmation{DeliveryTag: tag, Ack: true}
		}
	}()

	outcome := confirms.await(feed, 5, 150*time.Millisecond, farDeadline())

	assert.Equal(t, awaitDone, outcome)
	assert.Equal(t, 5, confirms.resolved)
}

func TestBatchConfirmsTimeoutWithoutProgress(t *testing.T) {
	confirms := &batchConfirms{channel: testChannel(0), firstTag: 1, limit: 3}
	feed := feedConfirms(amqp.Confirmation{DeliveryTag: 1, Ack: true})

	outcome := confirms.await(feed, 3, 100*time.Millisecond, farDeadline())

	assert.Equal(t, awaitTimeout, outcome)
	assert.False(t, confirms.closed)
	assert.Equal(t, 1, confirms.resolved)
}

func TestBatchConfirmsRoundBudget(t *testing.T) {
	confirms := &batchConfirms{channel: testChannel(0), firstTag: 1, limit: 5}
	feed := make(chan amqp.Confirmation, 5)
	go func() {
		for tag := uint64(1); tag <= 5; tag++ {
			time.Sleep(50 * time.Millisecond)
			feed <- amqp.Confirmation{DeliveryTag: tag, Ack: true}
		}
	}()

	outcome := confirms.await(feed, 5, 150*time.Millisecond, time.Now().Add(120*time.Millisecond))

	assert.Equal(t, awaitTimeout, outcome)
	assert.Less(t, confirms.resolved, 5)
}

func TestBatchConfirmsAborted(t *testing.T) {
	confirms := &batchConfirms{channel: &channel{connection: &connection{open: false}}, firstTag: 1, limit: 3}
	feed := feedConfirms(amqp.Confirmation{DeliveryTag: 1, Ack: true})

	outcome := confirms.await(feed, 3, time.Second, farDeadline())

	assert.Equal(t, awaitAborted, outcome)
	assert.Equal(t, 0, confirms.resolved)
}

func TestBatchConfirmsDrain(t *testing.T) {
	confirms := &batchConfirms{channel: testChannel(0), firstTag: 1, limit: 3}
	feed := feedConfirms(
		amqp.Confirmation{DeliveryTag: 1, Ack: true},
		amqp.Confirmation{DeliveryTag: 2, Ack: false},
	)

	assert.True(t, confirms.drain(feed))
	assert.Equal(t, 2, confirms.resolved)
	assert.Equal(t, []int{1}, confirms.failedOffsets)

	assert.True(t, confirms.drain(feed))
	assert.Equal(t, 2, confirms.resolved)

	close(feed)
	assert.False(t, confirms.drain(feed))
	assert.True(t, confirms.closed)
}

func TestNextPending(t *testing.T) {
	assert.Equal(t, []int{1, 4, 6, 7, 8, 9}, nextPending([]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 6, []int{1, 4}))
	assert.Equal(t, []int{2, 7}, nextPending([]int{2, 5, 7}, 2, []int{0}))
	assert.Empty(t, nextPending([]int{0, 1, 2}, 3, nil))
	assert.Equal(t, []int{0, 1, 2}, nextPending([]int{0, 1, 2}, 0, nil))
}

func TestPublisherMockPublishBatch(t *testing.T) {
	mock := &PublisherMock{}
	assert.Nil(t, mock.PublishBatch([]amqp.Publishing{{}, {}, {}}))

	mock.ReturnError = errors.New("always")
	err := mock.PublishBatch([]amqp.Publishing{{}, {}})
	var batchErr *BatchPublishError
	assert.True(t, errors.As(err, &batchErr))
	assert.Equal(t, []int{0, 1}, batchErr.FailedIndexes)
	assert.Equal(t, mock.ReturnError, batchErr.Err)
}
