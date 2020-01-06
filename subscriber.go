package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

// SubscriberWithLogger overrides Logger taken from Connection.
func SubscriberWithLogger(log Logger, level LoggingLevel) func(*subscriber) {
	return func(s *subscriber) {
		s.logger = logger{
			Logger: log,
			level:  level,
		}
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

// Subscriber is interface that provides all necessary methods for subscribing.
type Subscriber interface {
	Subscribe(ctx context.Context, queue *Queue, options ...func(*Subscription)) (<-chan Message, error)
	Close() error
}

type subscriber struct {
	connection      *Connection
	ch              *amqp.Channel
	chM             sync.RWMutex
	notifyChanClose chan *amqp.Error
	logger          logger
	done            chan bool
	closeOnce       sync.Once
	reconnectDelay  time.Duration
	prefetchCount   int
}

// NewSubscriber creates Subscriber that uses provided connection.
// Subscriber reconnect doesn't rely on context.
func NewSubscriber(ctx context.Context, conn *Connection, options ...func(*subscriber)) (Subscriber, error) {
	s := subscriber{
		connection:     conn,
		logger:         conn.logger,
		done:           make(chan bool),
		reconnectDelay: 5 * time.Second,
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
					s.logger.Debugf("failed to close subscriber: %v", err)
				}
			case <-s.done:
				s.logger.Debug("subscriber is done")
				return
			case err := <-s.notifyChanClose:
				if err == nil {
					return
				}
				s.logger.Debugf("channel closed: %v", err)
				if err := s.handleReconnect(context.Background()); err != nil {
					conn.logger.Debugf("reconnect error: %v", err)
				}
			}
		}
	}()

	return &s, nil
}

func (s *subscriber) handleReconnect(ctx context.Context) error {
	for {
		ch, err := s.connection.connection().Channel()
		if err != nil {
			s.logger.Debugf("failed to open channel: %v", err)
			select {
			case <-s.connection.done:
				s.logger.Debug("connection is done, closing subscriber")
				// closing self cause graceful shutdown by (*subscriber).done channel
				if err := s.Close(); err != nil {
					s.logger.Debugf("failed to close subscriber: %v", err)
				}
			case <-s.done:
				s.logger.Debug("subscriber is done")
				return errors.New("subscriber is done")
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(s.reconnectDelay):
			}
			continue
		}

		if err := s.presetChannel(ch); err != nil {
			s.logger.Debugf("failed to preset channel: %v", err)
			if err := ch.Close(); err != nil {
				s.logger.Debugf("failed to close channel: %v", err)
			}
			continue
		}

		s.changeChannel(ch)
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

	// TODO use *Queue to recover queue definition + binding

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
				s.logger.Debugf("failed to start consuming: %v", err)
				select {
				case <-ctx.Done():
					s.logger.Debugf("canceled by context: %v", ctx.Err())
					return
				case <-s.done:
					s.logger.Debug("subscriber is done")
					return
				case <-time.After(s.reconnectDelay):
				}
				continue
			}
			if err := s.consume(ctx, msgs, ch); err != nil {
				s.logger.Debugf("failed to consume: %v", err)
				return
			}
		}
	}()

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

	return s.ch.Close()
}
