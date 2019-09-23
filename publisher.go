package rabbitmq

import (
	"context"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type DeliveryMode uint8

const (
	Transient  DeliveryMode = 1
	Persistent DeliveryMode = 2
)

func SetDeliveryMode(mode DeliveryMode) func(*publisher) {
	return func(p *publisher) {
		p.deliveryMode = mode
	}
}

type Publisher interface {
	Publish(exchange, key string, data []byte, opts ...func(*amqp.Publishing)) error
	Close() error
}

type PublisherCtx interface {
	Publisher
	PublishCtx(ctx context.Context, exchange, key string, data []byte, opts ...func(*amqp.Publishing)) error
}

type publisher struct {
	connection      *Connection
	ch              *amqp.Channel
	chM             sync.RWMutex
	notifyChanClose chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation
	logger          logger
	deliveryMode    DeliveryMode
	resendDelay     time.Duration
	done            chan bool
	reconnectDelay  time.Duration
}

func NewPublisher(conn *Connection, opts ...func(*publisher)) (PublisherCtx, error) {
	p := publisher{
		connection:     conn,
		resendDelay:    time.Second,
		logger:         conn.logger,
		done:           make(chan bool),
		reconnectDelay: 5 * time.Second,
	}
	for _, opt := range opts {
		opt(&p)
	}

	p.handleReconnect()
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
				p.handleReconnect()
			}
		}
	}()

	return &p, nil
}

func (p *publisher) handleReconnect() {
	for {
		ch, err := p.connection.connection().Channel()
		if err != nil {
			p.logger.Debugf("failed to open channel: %v", err)
			select {
			case <-p.connection.done:
				p.logger.Debug("connection is done, closing publisher")
				if err := p.Close(); err != nil {
					p.logger.Debugf("failed to close publisher: %v", err)
				}
			case <-p.done:
				p.logger.Debug("publisher is done")
				return
			case <-time.After(p.reconnectDelay):
			}
			continue
		}

		if err := ch.Confirm(false); err != nil {
			p.logger.Debugf("failed to set to confirm mode: %v", err)
			continue
		}

		p.changeChannel(ch)
		break
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

func (p *publisher) Publish(exchange, key string, data []byte, opts ...func(*amqp.Publishing) /*TODO wrapper, maybe config struct*/) error {
	return p.PublishCtx(context.Background(), exchange, key, data, opts...)
}

func (p *publisher) PublishCtx(ctx context.Context, exchange, key string, data []byte, opts ...func(*amqp.Publishing) /*TODO wrapper, maybe config struct*/) error {
	for {
		if err := p.publish(exchange, key, data, opts...); err != nil {
			p.logger.Debugf("Push failed: %v", err)
			select {
			case <-ctx.Done():
				p.logger.Debug("publish canceled by context")
				return ctx.Err()
			case <-p.done:
				p.logger.Debug("publisher is done")
				return nil // TODO should be error. Publish wasn't successful!!!
			case <-time.After(p.resendDelay):
			}
			continue
		}
		select {
		case confirm := <-p.notifyConfirm:
			if confirm.Ack {
				p.logger.Debug("Push confirmed!")
				return nil
			}
		case <-time.After(p.resendDelay):
		}
		p.logger.Debug("Push didn't confirm. Retrying...")
	}
}

func (p *publisher) publish(exchange, key string, data []byte, opts ...func(*amqp.Publishing) /*TODO wrapper, maybe config struct*/) error {
	pub := amqp.Publishing{
		DeliveryMode: uint8(p.deliveryMode),
		Timestamp:    time.Now(),
		ContentType:  "text/plain",
		Body:         data,
	}

	for _, opt := range opts {
		opt(&pub)
	}

	return p.channel().Publish(exchange, key, false, false, pub)
}

func (p *publisher) Close() error {
	close(p.done)
	return p.ch.Close()
}
