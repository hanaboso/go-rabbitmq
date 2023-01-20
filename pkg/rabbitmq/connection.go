package rabbitmq

import (
	"fmt"
	log "github.com/hanaboso/go-log/pkg"
	"github.com/hanaboso/go-utils/pkg/intx"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
	"time"
)

type connection struct {
	client     *Client
	connection *amqp.Connection
	channels   []*channel
	lock       *sync.Mutex
	logger     log.Logger
	open       bool
}

/*
Creates a new connection with single channel keeping it open in endless reconnect loop
To stop it, call Close() method on client which sets open=false
*/
func (this *connection) connect(address string) {
	var retryCount int
	var connectionCheck chan *amqp.Error

	for this.open {
		{
			this.logger.Info("connecting to rabbitMQ: %s", address)
			connection, err := amqp.Dial(address)
			if err != nil {
				this.logger.Error(fmt.Errorf("connecting to rabbitMQ: %v", err))
				goto RETRY
			}

			this.connection = connection
			connectionCheck = connection.NotifyClose(make(chan *amqp.Error))
			retryCount = 0

			err = <-connectionCheck
			if err != nil {
				this.logger.Debug(err.Error())
				goto RETRY
			}

			this.logger.Debug("rabbitMq client closed")
			return
		}

	RETRY:
		retryCount++
		<-time.After(time.Duration(2*intx.Min(retryCount, 30)) * time.Second)
	}
}

func (this *connection) awaitConnection() {
	for {
		if this.connection != nil && !this.connection.IsClosed() {
			for _, channel := range this.channels {
				channel.awaitConnection()
			}
			return
		}
		<-time.After(100 * time.Millisecond)
	}
}

func newConnection(client *Client, address string, logger log.Logger) *connection {
	connection := &connection{
		client: client,
		logger: logger,
		open:   true,
		lock:   &sync.Mutex{},
	}
	go connection.connect(address)

	return connection
}
