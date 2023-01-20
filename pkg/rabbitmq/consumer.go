package rabbitmq

import (
	"context"
	"fmt"
	log "github.com/hanaboso/go-log/pkg"
	"github.com/hanaboso/go-utils/pkg/jsonx"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

type Consumer struct {
	channel  *channel
	queue    string
	prefetch int
}

type JsonConsumerCallback[T any] func(content *T, headers map[string]interface{}) Acked
type StringConsumerCallback func(content string, headers map[string]interface{}) Acked

type Acked int

const (
	Ack Acked = iota
	Nack
	Reject
)

type JsonConsumer[T any] struct {
	*Consumer
}

type StringConsumer struct {
	*Consumer
}

/*
	Due to Go's dumb limitations there's a HUGE copy-paste of String/Json consumers
	only message Body is handled differently so update fix both
*/

func (this *StringConsumer) Consume(callback StringConsumerCallback) {
	connector := this.channel.connection
	logger := connector.logger
	for connector.open {
		input := this.connect(false)
		if input == nil {
			time.Sleep(time.Second)
			continue
		}

		consume := true
		for consume {
			select {
			case message, ok := <-input:
				if ok {
					switch callback(string(message.Body), message.Headers) {
					case Ack:
						if err := message.Ack(true); err != nil {
							this.log(logger).Error(fmt.Errorf("reject rabbitMq message: %v", err))
						}
					case Nack:
						if err := message.Nack(true, true); err != nil {
							this.log(logger).Error(fmt.Errorf("reject rabbitMq message: %v", err))
						}
					case Reject:
						if err := message.Reject(true); err != nil {
							this.log(logger).Error(fmt.Errorf("reject rabbitMq message: %v", err))
						}
					default:
						this.log(logger).Fatal(fmt.Errorf("invalid consumer callback result"))
					}
				} else {
					consume = false
				}
			}
		}
	}
}

func (this *JsonConsumer[T]) Consume(callback JsonConsumerCallback[T]) {
	connector := this.channel.connection
	logger := connector.logger
	for connector.open {
		input := this.connect(false)
		if input == nil {
			time.Sleep(time.Second)
			continue
		}

		consume := true
		for consume {
			select {
			case <-this.channel.refreshed:
				consume = false
			case message, ok := <-input:
				if ok {
					content, err := jsonx.UnmarshalBytes[T](message.Body)
					if err != nil {
						this.log(logger).Fatal(fmt.Errorf("cannot parse rabbitMq message: %v", err))
					}
					switch callback(content, message.Headers) {
					case Ack:
						if err := message.Ack(true); err != nil {
							this.log(logger).Error(fmt.Errorf("reject rabbitMq message: %v", err))
						}
					case Nack:
						if err := message.Nack(true, true); err != nil {
							this.log(logger).Error(fmt.Errorf("reject rabbitMq message: %v", err))
						}
					case Reject:
						if err := message.Reject(true); err != nil {
							this.log(logger).Error(fmt.Errorf("reject rabbitMq message: %v", err))
						}
					default:
						this.log(logger).Fatal(fmt.Errorf("invalid consumer callback result"))
					}
				} else {
					consume = false
				}
			}
		}
	}
}

func (this *Consumer) Consume(autoAck bool) <-chan amqp.Delivery {
	output := make(chan amqp.Delivery)

	go func() {
		connector := this.channel.connection
		for connector.open {
			input := this.connect(autoAck)
			if input == nil {
				time.Sleep(time.Second)
				continue
			}

			consume := true
			for consume {
				select {
				case message, ok := <-input:
					if ok {
						output <- message
					} else {
						consume = false
					}
				}
			}
		}
		close(output)
	}()

	return output
}

func (this *Consumer) connect(autoAck bool) <-chan amqp.Delivery {
	channelContainer := this.channel
	connector := channelContainer.connection
	channel := channelContainer.channel
	logger := connector.logger
	connection := connector.connection
	client := connector.client

	if !connector.open || connection == nil || connection.IsClosed() || channel == nil || channel.IsClosed() {
		this.log(logger).Debug("connection closed / not opened")
		return nil
	}

	queue, ok := client.queues[this.queue]
	if !ok {
		this.log(logger).Fatal(fmt.Errorf("missing [%s] queue definition", this.queue))
	}

	if err := client.DeclareQueue(queue); err != nil {
		this.log(logger).Error(fmt.Errorf("declare queue: %v", err))
		return nil
	}

	if err := channel.Qos(this.prefetch, 0, false); err != nil {
		this.log(logger).Error(fmt.Errorf("declare prefetch: %v", err))
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	go func() {
		<-ctx.Done()
		if err := ctx.Err(); err == context.DeadlineExceeded {
			// This is hack to work around Go lang's rabbitMq shortcomings of amqp unresponsiveness
			// For example ex-declare with missing hash-ex plugin will do nothing (no error, timeout, nada)
			panic(err)
		}
	}()

	delivery, err := channel.Consume(
		this.queue,
		"",
		autoAck,
		queue.Options.Exclusive,
		false,
		queue.Options.NoWait,
		queue.Options.Args,
	)
	if err != nil {
		this.log(logger).Error(err)
		cancel()
		return nil
	}

	cancel()
	return delivery
}

func (this *Consumer) log(log log.Logger) log.Logger {
	return log.WithFields(map[string]interface{}{
		"exchange": this.queue,
	})
}
