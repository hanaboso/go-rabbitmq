package rabbitmq

import (
	"fmt"
	"github.com/hanaboso/go-utils/pkg/intx"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
	"time"
)

type channel struct {
	channel     *amqp.Channel
	connection  *connection
	confirm     chan amqp.Confirmation
	deliveryTag uint64
	refreshed   chan struct{}
	open        bool
	id          int
	mu          sync.Mutex
}

func (this *channel) connect() {
	var retryCount int
	var channelCheck chan *amqp.Error
	var channelCancelCheck chan string

	for this.open && this.connection.open {
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

func (this *channel) close() {
	this.open = false
	_ = this.channel.Close()
	this.connection.removeChannel(this.id)
}

func (this *channel) awaitConnection() {
	for {
		if this.channel != nil && !this.channel.IsClosed() {
			return
		}
		<-time.After(100 * time.Millisecond)
	}
}

var (
	// Unique identifier of channels
	channelId = 0
	mutex     = &sync.Mutex{}
)

func newChannel(connection *connection) *channel {
	mutex.Lock()
	currentId := channelId
	channelId += 1
	mutex.Unlock()

	return &channel{
		connection: connection,
		refreshed:  make(chan struct{}),
		id:         currentId,
		open:       true,
	}
}
