package rabbitmq

import (
	log "github.com/hanaboso/go-log/pkg"
	"github.com/hanaboso/go-utils/pkg/intx"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
)

type Client struct {
	address          string
	singleConnection bool

	subConnection *connection
	pubConnection *connection

	wg     *sync.WaitGroup
	logger log.Logger

	queues    map[string]Queue
	exchanges map[string]Exchange
}

type PublisherOptions struct {
	RetryAttempts int
	Timeout       int
}

var DefaultPublisherOptions = PublisherOptions{
	RetryAttempts: 0,
	Timeout:       publishTimeout,
}

func (this *Client) AddExchange(exchange Exchange) {
	this.exchanges[exchange.Name] = exchange
}

func (this *Client) AddQueue(queue Queue) {
	this.queues[queue.Name] = queue
}

func (this *Client) AddExchanges(exchanges []Exchange) {
	for _, exchange := range exchanges {
		this.exchanges[exchange.Name] = exchange
	}
}

func (this *Client) AddQueues(queues []Queue) {
	for _, queue := range queues {
		this.queues[queue.Name] = queue
	}
}

func (this *Client) InitializeQueuesExchanges() error {
	for _, queue := range this.queues {
		if err := this.DeclareQueue(queue); err != nil {
			return err
		}
	}
	for _, exchange := range this.exchanges {
		if err := this.DeclareExchange(exchange); err != nil {
			return err
		}
	}

	return nil
}

func (this *Client) Close() {
	closeFn := func(connection *connection) {
		if connection != nil {
			connection.open = false
			connection.lock.Lock()
			_ = connection.connection.Close()
			connection.lock.Unlock()
		}
	}
	closeFn(this.subConnection)
	closeFn(this.pubConnection)
}

func (this *Client) Destroy() {
	cleanUpConn := newConnection(this, this.address, this.logger)
	cleanUpConn.awaitConnection()

	channelObj := newChannel(cleanUpConn)
	go channelObj.connect()
	channelObj.awaitConnection()

	channel := channelObj.channel
	this.Close()

	for _, exchange := range this.exchanges {
		for _, binding := range exchange.Bindings {
			_ = channel.QueueUnbind(binding.Queue, binding.Key, exchange.Name, binding.Args)
		}
		_ = channel.ExchangeDelete(exchange.Name, false, true)
	}
	for _, queue := range this.queues {
		_, _ = channel.QueueDelete(queue.Name, false, false, true)
	}
	_ = channel.Close()
	_ = cleanUpConn.connection.Close()
}

/*
Exposes default amqp.Connection struct for edge cases
Reconnect is not handled for this connection by itself as it gets replaces within a Client
WARNING Closing connection will cause a deadlock
Do try to avoid this - use it for tests or other unique cases
*/
func (this *Client) RawConnection() *amqp.Connection {
	return this.getConsumerConnection().connection
}

/*
Blocking method until connection are successfully established
*/
func (this *Client) AwaitConnect() {
	this.getConsumerConnection().awaitConnection()
	this.getPublisherConnection().awaitConnection()
}

/*
Without registering Exchange object into Client publisher can't re-create it should it be deleted / non-existent
Uses default options
*/
func (this *Client) NewPublisher(exchange string, routingKey string) *Publisher {
	return this.NewPublisherOpt(exchange, routingKey, DefaultPublisherOptions)
}

/*
Without registering Exchange object into Client publisher can't re-create it should it be deleted / non-existent
*/
func (this *Client) NewPublisherOpt(exchange string, routingKey string, options PublisherOptions) *Publisher {
	channel := newChannel(this.getPublisherConnection())
	go channel.connect()
	channel.awaitConnection()

	return &Publisher{
		channel:       channel,
		timeout:       intx.Default(options.Timeout, publishTimeout),
		retryAttempts: options.RetryAttempts,
		exchange:      exchange,
		routingKey:    routingKey,
	}
}

/*
Before calling Consume() make sure to register given Queue into Client,
otherwise it will not be able to successfully connect
Uses default options
*/
func (this *Client) NewConsumer(queue string, prefetch int) *Consumer {
	channel := newChannel(this.getConsumerConnection())
	go channel.connect()
	channel.awaitConnection()

	return &Consumer{
		channel:  channel,
		queue:    queue,
		prefetch: prefetch,
		wg:       &sync.WaitGroup{},
		toClose:  make(chan struct{}),
		open:     true,
	}
}

/*
singleConnection - for heavy load it splits subscribers & publisher into two different client
for minimalistic usage with low traffic you can use single client for both
*/
func NewClient(address string, logger log.Logger, singleConnection bool) *Client {
	c := &Client{
		address:          address,
		logger:           logger,
		singleConnection: singleConnection,
		queues:           map[string]Queue{},
		exchanges:        map[string]Exchange{},
	}
	c.AwaitConnect()

	return c
}

func (this *Client) getConsumerConnection() *connection {
	if this.subConnection == nil {
		this.subConnection = newConnection(this, this.address, this.logger)
	}

	return this.subConnection
}

func (this *Client) getPublisherConnection() *connection {
	if this.singleConnection {
		return this.getConsumerConnection()
	}

	if this.pubConnection == nil {
		this.pubConnection = newConnection(this, this.address, this.logger)
	}

	return this.pubConnection
}
