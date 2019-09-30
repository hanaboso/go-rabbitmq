package rabbitmq

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type Publisher interface {
	Publish(exchange, key string, data []byte, options ...func(*Publishing)) error
	Close() error
}

type PublisherCtx interface {
	Publisher
	PublishCtx(ctx context.Context, exchange, key string, data []byte, options ...func(*Publishing)) error
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
	// TODO why two delays?
	reconnectDelay time.Duration
	resendDelay    time.Duration
}

func NewPublisher(conn *Connection, options ...func(*publisher)) (PublisherCtx, error) {
	p := publisher{
		connection:     conn,
		resendDelay:    time.Second,
		logger:         conn.logger,
		done:           make(chan bool),
		reconnectDelay: 5 * time.Second,
	}
	for _, option := range options {
		option(&p)
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

func (p *publisher) Publish(exchange, key string, data []byte, options ...func(*Publishing) /*TODO wrapper, maybe config struct*/) error {
	return p.PublishCtx(context.Background(), exchange, key, data, options...)
}

func (p *publisher) PublishCtx(ctx context.Context, exchange, key string, data []byte, options ...func(*Publishing) /*TODO wrapper, maybe config struct*/) error {
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

func (p *publisher) publish(exchange, key string, data []byte, options ...func(*Publishing) /*TODO wrapper, maybe config struct*/) error {
	pub := Publishing{
		ContentType: "text/plain",
		Timestamp:   time.Now(),
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
			CorrelationId:   pub.CorrelationId,
			ReplyTo:         pub.ReplyTo,
			Expiration:      pub.Expiration,
			MessageId:       pub.MessageId,
			Timestamp:       pub.Timestamp,
			Type:            pub.Type,
			UserId:          pub.UserId,
			AppId:           pub.AppId,
			Body:            pub.Body,
		})
}

func (p *publisher) Close() error {
	p.closeOnce.Do(func() {
		close(p.done)
	})

	return p.ch.Close()
}
