package rabbitmq

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type Subscriber interface {
	Subscribe(queue string, options ...func(*Subscription)) (<-chan Message, error)
	Close() error
}

type SubscriberCtx interface {
	Subscriber
	SubscribeCtx(ctx context.Context, queue string, options ...func(*Subscription)) (<-chan Message, error)
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
}

func NewSubscriber(conn *Connection, options ...func(*subscriber)) (SubscriberCtx, error) {
	s := subscriber{
		connection:     conn,
		logger:         conn.logger,
		done:           make(chan bool),
		reconnectDelay: 5 * time.Second,
	}
	for _, option := range options {
		option(&s)
	}

	s.handleReconnect()
	go func() {
		for {
			select {
			case <-conn.done:
				s.logger.Debug("connection is done, closing publisher")
				if err := s.Close(); err != nil {
					s.logger.Debugf("failed to close subscriber: %v", err)
				}
			case <-s.done:
				s.logger.Debug("publisher is done")
				return
			case err := <-s.notifyChanClose:
				if err == nil {
					return
				}
				s.logger.Debugf("channel closed: %v", err)
				s.handleReconnect()
			}
		}
	}()

	return &s, nil
}

func (s *subscriber) handleReconnect() {
	for {
		ch, err := s.connection.connection().Channel()
		if err != nil {
			s.logger.Debugf("failed to open channel: %v", err)
			select {
			case <-s.connection.done:
				s.logger.Debug("connection is done, closing publisher")
				if err := s.Close(); err != nil {
					s.logger.Debugf("failed to close subscriber: %v", err)
				}
			case <-s.done:
				s.logger.Debug("subscriber is done")
				return
			case <-time.After(s.reconnectDelay):
			}
			continue
		}

		s.changeChannel(ch)
		break
	}
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

func (s *subscriber) Subscribe(queue string, options ...func(*Subscription)) (<-chan Message, error) {
	return s.SubscribeCtx(context.Background(), queue, options...)
}

func (s *subscriber) SubscribeCtx(ctx context.Context, queue string, options ...func(*Subscription)) (<-chan Message, error) {
	conf := Subscription{
		Queue: queue,
	}
	for _, option := range options {
		option(&conf)
	}

	ch := make(chan Message)
	go func() {
		defer close(ch)

		for {
			msgs, err := s.channel().Consume(
				conf.Queue,            // queue
				conf.Consumer,         // consumer
				conf.AutoAck,          // auto-ack
				conf.Exclusive,        // exclusive
				conf.NoLocal,          // no-local
				conf.NoWait,           // no-wait
				amqp.Table(conf.Args), // args
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
