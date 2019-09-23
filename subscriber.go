package rabbitmq

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type Subscriber interface {
	Subscribe(conf QueueConfig) (<-chan Message, error)
	Close() error
}

type SubscriberCtx interface {
	Subscriber
	SubscribeCtx(ctx context.Context, conf QueueConfig) (<-chan Message, error)
}

type subscriber struct {
	connection      *Connection
	ch              *amqp.Channel
	chM             sync.RWMutex
	notifyChanClose chan *amqp.Error
	logger          logger
	done            chan bool
	reconnectDelay  time.Duration
}

func NewSubscriber(conn *Connection, opts ...func(*subscriber)) (SubscriberCtx, error) {
	s := subscriber{
		connection:     conn,
		logger:         conn.logger,
		done:           make(chan bool),
		reconnectDelay: 5 * time.Second,
	}
	for _, opt := range opts {
		opt(&s)
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

type QueueConfig struct {
	Name string
	// setup for binding
	// ex.
	// routing := map[string][]string{
	//     "exchange-one": {
	//         "routing.key.one",
	//         "routing.key.two",
	//     },
	//     "exchange-two": {
	//         "routing.key.one",
	//         "routing.key.two",
	//     },
	// }
	RoutingKeys map[string][]string
	Consumer    string
	Durable     bool
	AutoDelete  bool
	Exclusive   bool
	AutoAck     bool
	NoLocal     bool
}

func (s *subscriber) Subscribe(conf QueueConfig) (<-chan Message, error) {
	return s.SubscribeCtx(context.Background(), conf)
}

func (s *subscriber) SubscribeCtx(ctx context.Context, conf QueueConfig) (<-chan Message, error) {
	if err := s.declareExchange(conf); err != nil {
		return nil, err
	}
	if err := s.declareQueue(conf); err != nil {
		return nil, err
	}
	if err := s.bindQueue(conf); err != nil {
		return nil, err
	}
	ch := make(chan Message)
	go func() {
		defer close(ch)

		for {
			msgs, err := s.channel().Consume(
				conf.Name,      // queue
				conf.Consumer,  // consumer
				conf.AutoAck,   // auto-ack
				conf.Exclusive, // exclusive
				conf.NoLocal,   // no-local
				false,          // no-wait
				nil,            // args
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
			dst <- Message{delivery: msg}
		case <-ctx.Done():
			return fmt.Errorf("canceled by context: %w", ctx.Err())
		case <-s.done:
			return fmt.Errorf("subscriber is done")
		}
	}
}

func (s *subscriber) declareExchange(conf QueueConfig) error {
	for exchange, _ := range conf.RoutingKeys {
		if len(exchange) == 0 {
			continue
		}
		if err := s.channel().ExchangeDeclare(
			exchange,           // name
			amqp.ExchangeTopic, // type
			conf.Durable,       // durable
			conf.AutoDelete,    // auto-deleted
			false,              // internal
			false,              // no-wait
			nil,                // arguments
		); err != nil {
			return err
		}
	}

	return nil
}

func (s *subscriber) declareQueue(conf QueueConfig) error {
	_, err := s.channel().QueueDeclare(
		conf.Name,       // name
		conf.Durable,    // durable
		conf.AutoDelete, // delete when unused
		conf.Exclusive,  // exclusive
		false,           // no-wait
		nil,             // arguments
	)
	return err
}

func (s *subscriber) bindQueue(conf QueueConfig) error {
	for exchange, keys := range conf.RoutingKeys {
		if len(exchange) == 0 {
			continue
		}
		for _, key := range keys {
			if err := s.channel().QueueBind(
				conf.Name, // queue name
				key,       // routing key
				exchange,  // exchange
				false,
				nil,
			); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *subscriber) Close() error {
	// TODO can't be closed twice
	close(s.done)
	return s.ch.Close()
}
