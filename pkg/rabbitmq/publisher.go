package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	log "github.com/hanaboso/go-log/pkg"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

type Publisher struct {
	channel       *channel
	timeout       int
	retryAttempts int
	exchange      string
	routingKey    string
}

func (this *Publisher) Publish(message amqp.Publishing) error {
	return this.PublishExchangeRoutingKey(message, this.exchange, this.routingKey)
}

func (this *Publisher) PublishRoutingKey(message amqp.Publishing, routingKey string) error {
	return this.PublishExchangeRoutingKey(message, this.exchange, routingKey)
}

func (this *Publisher) PublishExchangeRoutingKey(message amqp.Publishing, exchange, routingKey string) error {
	var err error
	var channel = this.channel
	var connector = channel.connection
	if !connector.open || !channel.open {
		return errors.New("published closed")
	}

	connector.lock.Lock()
	defer connector.lock.Unlock()

	retries := this.retryAttempts
	if retries < 0 {
		retries = 10
	}

	for i := 0; i <= retries; i++ {
		if !connector.open {
			return errors.New("publisher closed")
		}

		var ch *amqp.Channel
		var confirm chan amqp.Confirmation
		channelTries := 10
		for {
			ch = channel.channel
			confirm = channel.confirm
			if ch != nil {
				break
			}

			if channelTries <= 0 {
				return errors.New("channel retries exceeded")
			}
			channelTries--

			<-time.After(time.Second)
		}

		if connector.connection.IsClosed() || ch.IsClosed() {
			err = errors.New("disconnected")
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(this.timeout)*time.Second)
		if err := ch.PublishWithContext(ctx, exchange, routingKey, false, false, message); err != nil {
			cancel()

			return err
		}

		select {
		case confirmM, ok := <-confirm:
			cancel()
			if !ok {
				if refreshErr := this.refreshExchange(); refreshErr != nil {
					err = fmt.Errorf("channel closed or cannot confirm publish, binding: %v", refreshErr)
				} else {
					i--
				}
				continue
			}

			channel.mu.Lock()
			if confirmM.DeliveryTag < channel.deliveryTag+1 {
				channel.mu.Unlock()
				err = fmt.Errorf("received unexpected delivery tag [want=%d, got=%d]", channel.deliveryTag+1, confirmM.DeliveryTag)
				continue
			}
			channel.deliveryTag = confirmM.DeliveryTag
			channel.mu.Unlock()

			if !confirmM.Ack {
				err = fmt.Errorf("publish not-ack")
				continue
			}

			return nil
		case <-ctx.Done():
			cancel()
			err = fmt.Errorf("publish timeout")
			continue
		}
	}

	return err
}

func (this *Publisher) refreshExchange() error {
	channelContainer := this.channel
	connector := channelContainer.connection
	channel := channelContainer.channel
	logger := connector.logger
	connection := connector.connection
	client := connector.client

	if !connector.open || connection == nil || connection.IsClosed() {
		this.log(logger).Debug("connection closed / not opened")
	}

	for {
		if channel == nil || channel.IsClosed() {
			channel = channelContainer.channel
			time.Sleep(100 * time.Millisecond)
			continue
		}
		break
	}

	exchange, ok := client.exchanges[this.exchange]
	if !ok {
		return fmt.Errorf("exchange is not setted")
	}

	if err := client.DeclareExchange(exchange); err != nil {
		return fmt.Errorf("declare exchange: %v", err)
	}

	return nil
}

func (this *Publisher) Close() {
	this.channel.close()
}

func (this *Publisher) log(log log.Logger) log.Logger {
	return log.WithFields(map[string]interface{}{
		"exchange": this.exchange,
	})
}
