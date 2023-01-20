package rabbitmq

import (
	"context"
	"fmt"
	"github.com/hanaboso/go-utils/pkg/intx"
	"time"
)

const (
	publishTimeout = 10
)

func (client *Client) DeclareQueue(queue Queue) error {
	channel, err := client.getConsumerConnection().connection.Channel()
	if err != nil {
		return err
	}

	_, err = channel.QueueDeclare(
		queue.Name,
		queue.Options.Durable,
		queue.Options.AutoDelete,
		queue.Options.Exclusive,
		queue.Options.NoWait,
		queue.Options.Args,
	)

	return err
}

func (client *Client) DeleteQueue(queue Queue) error {
	channel, err := client.getConsumerConnection().connection.Channel()
	if err != nil {
		return err
	}

	_, err = channel.QueueDelete(queue.Name, false, false, false)
	return err
}

func (client *Client) DeclareExchange(exchange Exchange) error {
	channel, err := client.getConsumerConnection().connection.Channel()
	if err != nil {
		return err
	}

	// Because go lib has no error or timeout on missing plugin -> only hangs up
	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Duration(intx.Default(exchange.Options.ConnectionTimeout, publishTimeout))*time.Second,
	)

	go func() {
		<-ctx.Done()
		if err := ctx.Err(); err == context.DeadlineExceeded {
			client.logger.Fatal(err)
		}
	}()

	err = channel.ExchangeDeclare(
		exchange.Name,
		exchange.Kind,
		exchange.Options.Durable,
		exchange.Options.AutoDelete,
		exchange.Options.Internal,
		exchange.Options.NoWait,
		exchange.Options.Args,
	)
	cancel()
	if err != nil {
		return err
	}

	for _, binding := range exchange.Bindings {
		if err := client.BindQueue(exchange.Name, binding); err != nil {
			return fmt.Errorf("bind exchange: %v", err)
		}
	}

	return nil
}

func (client *Client) DeleteExchange(exchange Exchange) error {
	channel, err := client.getPublisherConnection().connection.Channel()
	if err != nil {
		return err
	}

	return channel.ExchangeDelete(exchange.Name, true, false)
}

func (client *Client) BindQueue(exchange string, binding BindOptions) error {
	channel, err := client.getPublisherConnection().connection.Channel()
	if err != nil {
		return err
	}

	return channel.QueueBind(binding.Queue, binding.Key, exchange, binding.NoWait, binding.Args)
}

func (client *Client) UnbindQueue(exchange string, binding BindOptions) error {
	channel, err := client.getPublisherConnection().connection.Channel()
	if err != nil {
		return err
	}

	return channel.QueueUnbind(binding.Queue, binding.Key, exchange, binding.Args)
}

func (client *Client) BindExchange(source, destination Exchange, binding ExchangeBindOptions) error {
	channel, err := client.getPublisherConnection().connection.Channel()
	if err != nil {
		return err
	}

	return channel.ExchangeBind(destination.Name, binding.Key, source.Name, false, binding.Args)
}

func (client *Client) UnbindExchange(source, destination Exchange, binding ExchangeBindOptions) error {
	channel, err := client.getPublisherConnection().connection.Channel()
	if err != nil {
		return err
	}

	return channel.ExchangeUnbind(destination.Name, binding.Key, source.Name, false, binding.Args)
}
