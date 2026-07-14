package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type BatchPublishError struct {
	FailedIndexes []int
	Err           error
}

func (this *BatchPublishError) Error() string {
	return fmt.Sprintf("batch publish failed for %d message(s): %v", len(this.FailedIndexes), this.Err)
}

func (this *BatchPublishError) Unwrap() error {
	return this.Err
}

func (this *Publisher) PublishBatch(messages []amqp.Publishing) error {
	return this.PublishBatchExchangeRoutingKey(messages, this.exchange, this.routingKey)
}

func (this *Publisher) PublishBatchRoutingKey(messages []amqp.Publishing, routingKey string) error {
	return this.PublishBatchExchangeRoutingKey(messages, this.exchange, routingKey)
}

func (this *Publisher) PublishBatchExchangeRoutingKey(messages []amqp.Publishing, exchange, routingKey string) error {
	if len(messages) == 0 {
		return nil
	}
	if len(messages) == 1 {
		if err := this.PublishExchangeRoutingKey(messages[0], exchange, routingKey); err != nil {
			return &BatchPublishError{FailedIndexes: []int{0}, Err: err}
		}

		return nil
	}

	var channel = this.channel
	var connector = channel.connection
	if !connector.open || !channel.open {
		return &BatchPublishError{FailedIndexes: allIndexes(len(messages)), Err: errors.New("publisher closed")}
	}

	connector.lock.Lock()
	defer connector.lock.Unlock()

	retries := this.retryAttempts
	if retries < 0 {
		retries = 10
	}
	closedRefunds := 10

	pending := allIndexes(len(messages))
	var err error

	for i := 0; i <= retries; i++ {
		if !connector.open {
			err = errors.New("publisher closed")
			break
		}

		ch, confirm, live := awaitLiveChannel(channel)
		if !live {
			if !connector.open {
				err = errors.New("publisher closed")
			} else {
				err = errors.New("channel retries exceeded")
			}

			return &BatchPublishError{FailedIndexes: pending, Err: err}
		}

		round := this.publishBatchRound(ch, confirm, messages, pending, exchange, routingKey)
		pending = nextPending(pending, round.resolved, round.failedOffsets)
		if len(pending) == 0 {
			return nil
		}

		if round.err != nil {
			err = round.err
		} else if len(round.failedOffsets) > 0 {
			err = errors.New("publish not-ack")
		}

		if round.aborted {
			break
		}

		if round.closed {
			if refreshErr := this.refreshExchange(); refreshErr != nil {
				err = fmt.Errorf("channel closed or cannot confirm publish, binding: %v", refreshErr)
			} else {
				if err == nil {
					err = errors.New("channel closed before publish confirmation")
				}
				if closedRefunds > 0 {
					closedRefunds--
					i--
				}
			}
		} else if round.resolved < round.sent && connector.open {
			channel.requestRefresh(10 * time.Second)
		}
	}

	return &BatchPublishError{FailedIndexes: pending, Err: err}
}

type batchRound struct {
	sent          int
	resolved      int
	failedOffsets []int
	closed        bool
	aborted       bool
	err           error
}

func (this *Publisher) publishBatchRound(ch *amqp.Channel, confirm chan amqp.Confirmation, messages []amqp.Publishing, pending []int, exchange, routingKey string) batchRound {
	channel := this.channel
	timeout := time.Duration(this.timeout) * time.Second
	deadline := time.Now().Add(timeout * time.Duration(1+len(pending)/confirmBufferCap))

	channel.mu.Lock()
	firstTag := channel.deliveryTag + 1
	channel.mu.Unlock()

	confirms := &batchConfirms{channel: channel, firstTag: firstTag, limit: len(pending)}

	sent := 0
	outcome := awaitDone
	var err error

sendLoop:
	for _, index := range pending {
		for sent-confirms.resolved >= confirmBufferCap {
			outcome = confirms.await(confirm, confirms.resolved+1, timeout, deadline)
			if outcome != awaitDone {
				break sendLoop
			}
		}

		if publishErr := ch.PublishWithContext(context.Background(), exchange, routingKey, false, false, messages[index]); publishErr != nil {
			err = publishErr
			break
		}
		sent++

		if !confirms.drain(confirm) {
			outcome = awaitClosed
			break
		}
	}

	if outcome == awaitDone && confirms.resolved < sent {
		outcome = confirms.await(confirm, sent, timeout, deadline)
	}

	switch outcome {
	case awaitTimeout:
		if err == nil {
			err = errors.New("publish timeout")
		}
	case awaitAborted:
		err = errors.New("publisher closed")
	}

	return batchRound{
		sent:          sent,
		resolved:      confirms.resolved,
		failedOffsets: confirms.failedOffsets,
		closed:        confirms.closed,
		aborted:       outcome == awaitAborted,
		err:           err,
	}
}

func awaitLiveChannel(channel *channel) (*amqp.Channel, chan amqp.Confirmation, bool) {
	connector := channel.connection
	for tries := 10; tries > 0; tries-- {
		if !connector.open || !channel.open {
			return nil, nil, false
		}

		ch, confirm := channel.snapshot()
		if ch != nil && !ch.IsClosed() {
			return ch, confirm, true
		}

		<-time.After(time.Second)
	}

	return nil, nil, false
}

type awaitOutcome int

const (
	awaitDone awaitOutcome = iota
	awaitClosed
	awaitTimeout
	awaitAborted
)

type batchConfirms struct {
	channel       *channel
	firstTag      uint64
	limit         int
	resolved      int
	failedOffsets []int
	closed        bool
}

func (this *batchConfirms) handle(confirmation amqp.Confirmation, ok bool) bool {
	if !ok {
		this.closed = true
		return false
	}

	this.channel.mu.Lock()
	if confirmation.DeliveryTag > this.channel.deliveryTag {
		this.channel.deliveryTag = confirmation.DeliveryTag
	}
	this.channel.mu.Unlock()

	if confirmation.DeliveryTag < this.firstTag {
		return true
	}

	offset := int(confirmation.DeliveryTag - this.firstTag)
	if offset >= this.limit {
		return true
	}

	if !confirmation.Ack {
		this.failedOffsets = append(this.failedOffsets, offset)
	}
	this.resolved++

	return true
}

func (this *batchConfirms) drain(confirm chan amqp.Confirmation) bool {
	for {
		select {
		case confirmation, ok := <-confirm:
			if !this.handle(confirmation, ok) {
				return false
			}
		default:
			return true
		}
	}
}

func (this *batchConfirms) await(confirm chan amqp.Confirmation, count int, timeout time.Duration, deadline time.Time) awaitOutcome {
	lastProgress := time.Now()

	for this.resolved < count {
		if !this.channel.connection.open {
			return awaitAborted
		}

		now := time.Now()
		wait := timeout - now.Sub(lastProgress)
		if untilDeadline := deadline.Sub(now); untilDeadline < wait {
			wait = untilDeadline
		}
		if wait <= 0 {
			return awaitTimeout
		}
		if wait > time.Second {
			wait = time.Second
		}

		select {
		case confirmation, ok := <-confirm:
			if !this.handle(confirmation, ok) {
				return awaitClosed
			}
			lastProgress = time.Now()
		case <-time.After(wait):
		}
	}

	return awaitDone
}

func nextPending(pending []int, resolved int, failedOffsets []int) []int {
	next := make([]int, 0, len(failedOffsets)+len(pending)-resolved)
	for _, offset := range failedOffsets {
		next = append(next, pending[offset])
	}

	return append(next, pending[resolved:]...)
}

func allIndexes(count int) []int {
	indexes := make([]int, count)
	for i := range indexes {
		indexes[i] = i
	}

	return indexes
}
