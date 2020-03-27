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

// PublisherWithPublishAck allow change ack mode.
// Publisher will not wait for ack by default.
func PublisherWithPublishAck() func(*publisher) {
	return func(p *publisher) {
		p.ack = true
	}
}

// PublisherAddQueue adds queue into slice.
// Added queues will be auto-declared.
func PublisherAddQueue(queue Queue) func(*publisher) {
	return func(p *publisher) {
		p.queues = append(p.queues, queue)
	}
}

// PublisherAddExchange adds exchange into slice.
// Added exchange will be auto-declared.
func PublisherAddExchange(exchange Exchange) func(*publisher) {
	return func(p *publisher) {
		p.exchanges = append(p.exchanges, exchange)
	}
}

// Publisher is interface that provides all necessary methods for publishing.
type Publisher interface {
	Publish(ctx context.Context, exchange, key string, data []byte, options ...func(*Publishing)) error
	Close() error
}

type publisher struct {
	ack             bool
	ch              *amqp.Channel
	chM             sync.RWMutex
	closeOnce       sync.Once
	connection      *Connection
	done            chan bool
	exchanges       []Exchange
	logger          log.Logger
	notifyChanClose chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation
	queues          []Queue
}

// NewPublisher creates Publisher that uses provided connection.
// Publisher reconnect doesn't rely on context.
func NewPublisher(ctx context.Context, conn *Connection, options ...func(*publisher)) (Publisher, error) {
	p := publisher{
		connection: conn,
		logger:     conn.logger,
		ack:        false,
		done:       make(chan bool),
	}
	for _, option := range options {
		option(&p)
	}

	if err := p.handleReconnect(ctx); err != nil {
		return nil, err
	}
	go func() {
		for {
			select {
			case <-conn.done:
				p.logger.Debug("connection is done, closing publisher")
				if err := p.Close(); err != nil {
					p.logger.WithFields(map[string]interface{}{
						"error": err.Error(),
					}).Debug("failed to close publisher")
				}
			case <-p.done:
				p.logger.Debug("publisher is done")
				return
			case err := <-p.notifyChanClose:
				if err == nil {
					return
				}
				p.logger.WithFields(map[string]interface{}{
					"error": err.Error(),
				}).Debug("channel closed")
				if err := p.handleReconnect(context.Background()); err != nil {
					p.logger.WithFields(map[string]interface{}{
						"error": err.Error(),
					}).Debug("reconnect")
				}
			}
		}
	}()

	// Auto declare Exchanges
	for _, ex := range p.exchanges {
		err := conn.ExchangeDeclare(nil, ex)
		if err != nil {
			p.logger.WithFields(map[string]interface{}{
				"error": err.Error(),
			}).Debug("exchange declare")
		}
	}

	// Auto declare Queues
	for i, queue := range p.queues {
		q, err := conn.QueueDeclare(ctx, queue)
		if err != nil {
			p.logger.WithFields(map[string]interface{}{
				"error": err.Error(),
			}).Debug("queue declare")
		}
		p.queues[i] = q
	}

	return &p, nil
}

func (p *publisher) handleReconnect(ctx context.Context) error {
	for {
		ch, err := p.connection.connection().Channel()
		if err != nil {
			p.logger.WithFields(map[string]interface{}{
				"error": err.Error(),
			}).Debug("failed to open channel")
			select {
			case <-p.connection.done:
				p.logger.Debug("connection is done, closing publisher")
				// closing self cause graceful shutdown by (*publisher).done channel
				if err := p.Close(); err != nil {
					p.logger.WithFields(map[string]interface{}{
						"error": err.Error(),
					}).Debug("failed to close publisher")
				}
			case <-p.done:
				p.logger.WithFields(map[string]interface{}{
					"error": err.Error(),
				}).Debug("publisher is done")
				return errors.New("subscriber is done")
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(p.connection.reconnectDelay.Duration()):
			}
			continue
		}

		if err := ch.Confirm(false); err != nil {
			p.logger.WithFields(map[string]interface{}{
				"error": err.Error(),
			}).Debug("failed to set to confirm mode")
			continue
		}

		p.changeChannel(ch)
		p.connection.reconnectDelay.Reset()
		return nil
	}
}

func (p *publisher) channel() *amqp.Channel {
	p.chM.RLock()
	defer p.chM.RUnlock()
	return p.ch
}

func (p *publisher) changeChannel(channel *amqp.Channel) {
	p.chM.Lock()
	defer p.chM.Unlock()

	p.ch = channel
	p.notifyChanClose = make(chan *amqp.Error)
	p.notifyConfirm = make(chan amqp.Confirmation, 1)
	p.ch.NotifyClose(p.notifyChanClose)
	p.ch.NotifyPublish(p.notifyConfirm)
}

func (p *publisher) Publish(ctx context.Context, exchange, key string, data []byte, options ...func(*Publishing)) error {
	for {
		if err := p.publish(exchange, key, data, options...); err != nil {
			p.logger.WithFields(map[string]interface{}{
				"error": err.Error(),
			}).Debug("Push failed")
			if !errors.Is(err, &amqp.Error{}) {
				return err
			}
			select {
			case <-ctx.Done():
				p.logger.Debug("publish canceled by context")
				return ctx.Err()
			case <-p.done:
				p.logger.Debug("publisher is done")
				return fmt.Errorf("publisher is done")
			case <-time.After(p.connection.publishDelay.Duration()):
			}
			continue
		}

		if p.ack {
			select {
			case confirm := <-p.notifyConfirm:
				if confirm.Ack {
					p.logger.Debug("Push confirmed!")
					p.connection.publishDelay.Reset()
					return nil
				}

				p.logger.Debug("publish confirm not acked")

				return errors.New("publish confirm not acked")
			case <-p.done:
				p.logger.Debug("publisher is done")
				return fmt.Errorf("publisher is done")
			case <-ctx.Done():
				p.logger.Debug("publish canceled by context")
				return ctx.Err()
			case <-time.After(p.connection.publishDelay.Duration()):
				p.logger.Debug("publish delay exceeded")
				return fmt.Errorf("publish delay exceeded")
			}
		}
		return nil
	}
}

func (p *publisher) publish(exchange, key string, data []byte, options ...func(*Publishing)) error {
	pub := Publishing{
		ContentType:  "text/plain",
		Timestamp:    time.Now().UTC(),
		Exchange:     exchange,
		RoutingKey:   key,
		Body:         data,
		DeliveryMode: Persistent,
	}

	for _, option := range options {
		option(&pub)
	}

	return p.channel().Publish(
		pub.Exchange,
		pub.RoutingKey,
		pub.Mandatory,
		pub.Immediate,
		amqp.Publishing{
			Headers:         amqp.Table(pub.Headers),
			ContentType:     pub.ContentType,
			ContentEncoding: pub.ContentEncoding,
			DeliveryMode:    uint8(pub.DeliveryMode),
			Priority:        pub.Priority,
			CorrelationId:   pub.CorrelationID,
			ReplyTo:         pub.ReplyTo,
			Expiration:      pub.Expiration,
			MessageId:       pub.MessageID,
			Timestamp:       pub.Timestamp,
			Type:            pub.Type,
			UserId:          pub.UserID,
			AppId:           pub.AppID,
			Body:            pub.Body,
		})
}

func (p *publisher) Close() error {
	p.closeOnce.Do(func() {
		close(p.done)
	})
	p.connection.reconnectDelay.Reset()
	p.connection.publishDelay.Reset()

	return p.ch.Close()
}
