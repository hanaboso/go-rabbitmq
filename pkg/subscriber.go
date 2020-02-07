package pkg

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/streadway/amqp"

	log "github.com/hanaboso/go-log/pkg"
)

// SubscriberWithLogger overrides Logger taken from Connection.
func SubscriberWithLogger(log log.Logger) func(*subscriber) {
	return func(s *subscriber) {
		s.logger = log
	}
}

// SubscriberWithPrefetchLimit sets prefetch limit for subscriber.
// Can't override global limit, stricter limit is applied.
// Limit is set to channel in fact, but that channel will be used for this subscriber only.
func SubscriberWithPrefetchLimit(count int) func(*subscriber) {
	return func(s *subscriber) {
		s.prefetchCount = count
	}
}

// SubscriberAddQueue adds queue into slice.
// Added queues will be auto-declared.
func SubscriberAddQueue(queue Queue) func(*subscriber) {
	return func(subscriber *subscriber) {
		subscriber.queues = append(subscriber.queues, queue)
	}
}

// SubscriberAddExchange adds exchange into slice.
// Added exchange will be auto-declared.
func SubscriberAddExchange(exchange Exchange) func(*subscriber) {
	return func(s *subscriber) {
		s.exchanges = append(s.exchanges, exchange)
	}
}

// Subscriber is interface that provides all necessary methods for subscribing.
type Subscriber interface {
	Subscribe(ctx context.Context, queue *Queue, options ...func(*Subscription)) (<-chan Message, error)
	Close() error
}

type subscriber struct {
	ch              *amqp.Channel
	chM             sync.RWMutex
	closeOnce       sync.Once
	connection      *Connection
	done            chan bool
	exchanges       []Exchange
	logger          log.Logger
	notifyChanClose chan *amqp.Error
	prefetchCount   int
	queues          []Queue
}

// NewSubscriber creates Subscriber that uses provided connection.
// Subscriber reconnect doesn't rely on context.
func NewSubscriber(ctx context.Context, conn *Connection, options ...func(*subscriber)) (Subscriber, error) {
	s := subscriber{
		connection: conn,
		logger:     conn.logger,
		done:       make(chan bool),
	}

	for _, option := range options {
		option(&s)
	}

	if err := s.handleReconnect(ctx); err != nil {
		return nil, err
	}
	go func() {
		for {
			select {
			case <-conn.done:
				s.logger.Debug("connection is done, closing subscriber")
				if err := s.Close(); err != nil {
					s.logger.WithFields(map[string]interface{}{
						"error": err.Error(),
					}).Debug("failed to close subscriber")
				}
			case <-s.done:
				s.logger.Debug("subscriber is done")
				return
			case err := <-s.notifyChanClose:
				if err == nil {
					return
				}
				s.logger.WithFields(map[string]interface{}{
					"error": err.Error(),
				}).Debug("channel closed")
				if err := s.handleReconnect(context.Background()); err != nil {
					conn.logger.WithFields(map[string]interface{}{
						"error": err.Error(),
					}).Debug("reconnect error")
				}
			}
		}
	}()

	// Auto-declare Exchanges
	for _, ex := range s.exchanges {
		err := conn.ExchangeDeclare(ctx, ex)
		if err != nil {
			conn.logger.WithFields(map[string]interface{}{
				"error": err.Error(),
			}).Debug("exchange declare error")
		}
	}

	//Auto-declare Queues
	for i, queue := range s.queues {
		q, err := conn.QueueDeclare(ctx, queue)
		if err != nil {
			conn.logger.WithFields(map[string]interface{}{
				"error": err.Error(),
			}).Debug("queue declare error")
		}
		s.queues[i] = q
	}

	return &s, nil
}

func (s *subscriber) handleReconnect(ctx context.Context) error {
	for {
		ch, err := s.connection.connection().Channel()
		if err != nil {
			s.logger.WithFields(map[string]interface{}{
				"error": err.Error(),
			}).Debug("failed to open channel")
			select {
			case <-s.connection.done:
				s.logger.Debug("connection is done, closing subscriber")
				// closing self cause graceful shutdown by (*subscriber).done channel
				if err := s.Close(); err != nil {
					s.logger.WithFields(map[string]interface{}{
						"error": err.Error(),
					}).Debug("failed to close subscriber")
				}
			case <-s.done:
				s.logger.Debug("subscriber is done")
				return errors.New("subscriber is done")
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(s.connection.reconnectDelay.Duration()):
			}
			continue
		}

		if err := s.presetChannel(ch); err != nil {
			s.logger.WithFields(map[string]interface{}{
				"error": err.Error(),
			}).Debug("failed to preset channel")
			if err := ch.Close(); err != nil {
				s.logger.WithFields(map[string]interface{}{
					"error": err.Error(),
				}).Debug("failed to close channel")
			}
			continue
		}

		s.changeChannel(ch)
		s.connection.reconnectDelay.Reset()
		return nil
	}
}

func (s *subscriber) presetChannel(ch *amqp.Channel) error {
	if s.prefetchCount > 0 {
		if err := ch.Qos(s.prefetchCount, 0, false); err != nil {
			return fmt.Errorf("failed to set QoS: %v", err)
		}
	}

	return nil
}

func (s *subscriber) channel() *amqp.Channel {
	s.chM.RLock()
	defer s.chM.RUnlock()
	return s.ch
}

func (s *subscriber) changeChannel(channel *amqp.Channel) {
	s.chM.Lock()
	defer s.chM.Unlock()

	s.ch = channel
	s.notifyChanClose = make(chan *amqp.Error)
	s.ch.NotifyClose(s.notifyChanClose)
}

func (s *subscriber) Subscribe(ctx context.Context, queue *Queue, options ...func(*Subscription)) (<-chan Message, error) {
	subscription := Subscription{
		Queue: queue.Name,
	}

	for _, option := range options {
		option(&subscription)
	}

	ch := make(chan Message)
	go func() {
		defer close(ch)

		for {
			msgs, err := s.channel().Consume(
				subscription.Queue,            // queue
				subscription.Consumer,         // consumer
				subscription.AutoAck,          // auto-ack
				subscription.Exclusive,        // exclusive
				subscription.NoLocal,          // no-local
				subscription.NoWait,           // no-wait
				amqp.Table(subscription.Args), // args
			)
			if err != nil {
				s.logger.WithFields(map[string]interface{}{
					"error": err.Error(),
				}).Debug("failed to start consuming")
				select {
				case <-ctx.Done():
					s.logger.WithFields(map[string]interface{}{
						"error": ctx.Err(),
					}).Debug("canceled by context")
					return
				case <-s.done:
					s.logger.Debug("subscriber is done")
					return
				case <-time.After(s.connection.subscribeDelay.Duration()):
				}
				continue
			}
			if err := s.consume(ctx, msgs, ch); err != nil {
				s.logger.WithFields(map[string]interface{}{
					"error": err.Error(),
				}).Debug("failed to consume")
				return
			}
		}
	}()

	s.connection.subscribeDelay.Reset()
	return ch, nil
}

func (s *subscriber) consume(ctx context.Context, src <-chan amqp.Delivery, dst chan<- Message) error {
	for {
		select {
		case msg, ok := <-src:
			if !ok {
				// channel is closed, but it will be logged as closed connection
				return nil
			}
			dst <- Message{Delivery: msg}
		case <-ctx.Done():
			return fmt.Errorf("canceled by context: %w", ctx.Err())
		case <-s.done:
			return fmt.Errorf("subscriber is done")
		}
	}
}

func (s *subscriber) Close() error {
	s.closeOnce.Do(func() {
		close(s.done)
	})
	s.connection.reconnectDelay.Reset()
	s.connection.subscribeDelay.Reset()

	return s.ch.Close()
}
