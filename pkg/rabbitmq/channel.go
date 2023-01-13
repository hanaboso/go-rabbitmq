package rabbitmq

import (
	"fmt"
	"github.com/hanaboso/go-utils/pkg/intx"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

type channel struct {
	channel     *amqp.Channel
	connection  *connection
	confirm     chan amqp.Confirmation
	deliveryTag uint64
	refreshed   chan struct{}
}

func (this *channel) connect() {
	var retryCount int
	var channelCheck chan *amqp.Error
	var channelCancelCheck chan string

	for this.connection.open {
		close(this.refreshed)
		this.refreshed = make(chan struct{})
		connection := this.connection.connection
		logger := this.connection.logger

		channel, err := connection.Channel()
		if err != nil {
			logger.Error(fmt.Errorf("creating client channel: %v", err))
			goto RETRY
		}
		err = channel.Confirm(false)
		if err != nil {
			logger.Error(fmt.Errorf("confirm channel: %v", err))
			goto RETRY
		}

		channelCheck = channel.NotifyClose(make(chan *amqp.Error))
		channelCancelCheck = channel.NotifyCancel(make(chan string))
		this.confirm = channel.NotifyPublish(make(chan amqp.Confirmation, 99))
		this.channel = channel
		this.deliveryTag = 0
		retryCount = 0

		select {
		case err := <-channelCheck:
			if err != nil {
				logger.Debug(err.Error())
				goto RETRY
			}
		case <-channelCancelCheck:
			goto RETRY
		}

		return

	RETRY:
		this.channel.Close()
		retryCount++
		<-time.After(time.Duration(2*intx.Min(retryCount, 30)) * time.Second)
	}
}

func (this *channel) awaitConnection() {
	for {
		if this.channel != nil && !this.channel.IsClosed() {
			return
		}
		<-time.After(100 * time.Millisecond)
	}
}

func newChannel(connection *connection) *channel {
	return &channel{
		connection: connection,
		refreshed:  make(chan struct{}),
	}
}
