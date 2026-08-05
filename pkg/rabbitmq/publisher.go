package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"time"

	log "github.com/hanaboso/go-log/pkg"
	amqp "github.com/rabbitmq/amqp091-go"
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
	closedRefunds := 10

	for i := 0; i <= retries; i++ {
		if !connector.open {
			return errors.New("publisher closed")
		}

		ch, confirm, live := awaitLiveChannel(channel)
		if !live {
			if !connector.open {
				return errors.New("publisher closed")
			}

			return errors.New("channel retries exceeded")
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
				err = errors.New("channel closed before publish confirmation")
				if refreshErr := this.refreshExchange(); refreshErr != nil {
					err = fmt.Errorf("channel closed or cannot confirm publish, binding: %v", refreshErr)
				} else if closedRefunds > 0 {
					closedRefunds--
					i--
				}
				continue
			}

			channel.mu.Lock()
			if confirmM.DeliveryTag < channel.deliveryTag+1 {
				channel.mu.Unlock()
				err = fmt.Errorf("received unexpected delivery tag [want=%d, got=%d]", channel.deliveryTag+1, confirmM.DeliveryTag)
				channel.requestRefresh(10 * time.Second)
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
			channel.requestRefresh(10 * time.Second)
			continue
		}
	}

	return err
}

func (this *Publisher) refreshExchange() error {
	channelContainer := this.channel
	client := channelContainer.connection.client

	if _, _, live := awaitLiveChannel(channelContainer); !live {
		return errors.New("channel is not available")
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
