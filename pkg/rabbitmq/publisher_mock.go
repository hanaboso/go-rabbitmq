package rabbitmq

import amqp "github.com/rabbitmq/amqp091-go"

type IPublisher interface {
	Publish(message amqp.Publishing) error
	PublishRoutingKey(message amqp.Publishing, routingKey string) error
	PublishExchangeRoutingKey(message amqp.Publishing, exchange, routingKey string) error
	Close()
}

type PublisherMock struct {
	Messages        chan amqp.Publishing
	ReturnError     error
	ReturnErrorOnce error
}

func (this *PublisherMock) Publish(message amqp.Publishing) error {
	if this.Messages != nil {
		this.Messages <- message
	}

	if this.ReturnErrorOnce != nil {
		this.ReturnErrorOnce = nil
		return this.ReturnErrorOnce
	}

	return this.ReturnError
}

func (this *PublisherMock) PublishRoutingKey(message amqp.Publishing, _routingKey string) error {
	return this.Publish(message)
}

func (this *PublisherMock) PublishExchangeRoutingKey(message amqp.Publishing, _exchange, _routingKey string) error {
	return this.Publish(message)
}

func (this *PublisherMock) Close() {
	close(this.Messages)
}
