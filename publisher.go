package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

// Publisher is interface that provides all necessary methods for publishing.
type Publisher interface {
	Publish(ctx context.Context, exchange, key string, data []byte, options ...func(*Publishing)) error
	Close() error
}

type publisher struct {
	connection      *Connection
	ch              *amqp.Channel
	chM             sync.RWMutex
	notifyChanClose chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation
	logger          logger
	done            chan bool
	closeOnce       sync.Once
	reconnectDelay  time.Duration
}

// NewPublisher creates Publisher that uses provided connection.
// Publisher reconnect doesn't rely on context.
func NewPublisher(ctx context.Context, conn *Connection, options ...func(*publisher)) (Publisher, error) {
	p := publisher{
		connection:     conn,
		logger:         conn.logger,
		done:           make(chan bool),
		reconnectDelay: time.Second,
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
					p.logger.Debugf("failed to close publisher: %v", err)
				}
			case <-p.done:
				p.logger.Debug("publisher is done")
				return
			case err := <-p.notifyChanClose:
				if err == nil {
					return
				}
				p.logger.Debugf("channel closed: %v", err)
				if err := p.handleReconnect(context.Background()); err != nil {
					conn.logger.Debugf("reconnect error: %v", err)
				}
			}
		}
	}()

	return &p, nil
}

func (p *publisher) handleReconnect(ctx context.Context) error {
	for {
		ch, err := p.connection.connection().Channel()
		if err != nil {
			p.logger.Debugf("failed to open channel: %v", err)
			select {
			case <-p.connection.done:
				p.logger.Debug("connection is done, closing publisher")
				// closing self cause graceful shutdown by (*publisher).done channel
				if err := p.Close(); err != nil {
					p.logger.Debugf("failed to close publisher: %v", err)
				}
			case <-p.done:
				p.logger.Debug("publisher is done")
				return errors.New("subscriber is done")
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(p.reconnectDelay):
			}
			continue
		}

		if err := ch.Confirm(false); err != nil {
			p.logger.Debugf("failed to set to confirm mode: %v", err)
			continue
		}

		p.changeChannel(ch)
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
			p.logger.Debugf("Push failed: %v", err)
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
			case <-time.After(p.reconnectDelay):
			}
			continue
		}
		select {
		case confirm := <-p.notifyConfirm:
			if confirm.Ack {
				p.logger.Debug("Push confirmed!")
				return nil
			}
		case <-time.After(p.reconnectDelay):
		}
		p.logger.Debug("Push didn't confirm. Retrying...")
	}
}

func (p *publisher) publish(exchange, key string, data []byte, options ...func(*Publishing)) error {
	pub := Publishing{
		ContentType: "text/plain",
		Timestamp:   time.Now().UTC(),
		Exchange:    exchange,
		RoutingKey:  key,
		Body:        data,
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

	return p.ch.Close()
}
