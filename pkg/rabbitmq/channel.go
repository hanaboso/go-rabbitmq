package rabbitmq

import (
	"fmt"
	"sync"
	"time"

	"github.com/hanaboso/go-utils/pkg/intx"
	amqp "github.com/rabbitmq/amqp091-go"
)

const confirmBufferCap = 99

type channel struct {
	channel     *amqp.Channel
	connection  *connection
	confirm     chan amqp.Confirmation
	deliveryTag uint64
	refreshed   chan struct{}
	refreshReq  chan struct{}
	open        bool
	id          int
	mu          sync.Mutex
}

func (this *channel) connect() {
	var retryCount int
	var channelCheck chan *amqp.Error
	var channelCancelCheck chan string
	var confirm chan amqp.Confirmation

	for this.open && this.connection.open {
		this.mu.Lock()
		close(this.refreshed)
		this.refreshed = make(chan struct{})
		this.mu.Unlock()
		select {
		case <-this.refreshReq:
		default:
		}
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
		confirm = channel.NotifyPublish(make(chan amqp.Confirmation, confirmBufferCap))
		this.mu.Lock()
		this.confirm = confirm
		this.deliveryTag = 0
		this.channel = channel
		this.mu.Unlock()
		retryCount = 0

		select {
		case err := <-channelCheck:
			if err != nil {
				logger.Debug(err.Error())
				goto RETRY
			}
		case <-channelCancelCheck:
			goto RETRY
		case <-this.refreshReq:
			_ = this.channel.Close()
			continue
		}

		return

	RETRY:
		this.channel.Close()
		retryCount++
		<-time.After(time.Duration(2*intx.Min(retryCount, 30)) * time.Second)
	}
}

func (this *channel) requestRefresh(bound time.Duration) {
	this.mu.Lock()
	refreshed := this.refreshed
	this.mu.Unlock()
	select {
	case this.refreshReq <- struct{}{}:
	default:
	}

	select {
	case <-refreshed:
	case <-time.After(bound):
	}
}

func (this *channel) close() {
	this.open = false
	channel, _ := this.snapshot()
	if channel != nil {
		_ = channel.Close()
	}
	this.connection.removeChannel(this.id)
}

func (this *channel) awaitConnection() {
	for {
		if channel, _ := this.snapshot(); channel != nil && !channel.IsClosed() {
			return
		}
		<-time.After(100 * time.Millisecond)
	}
}

func (this *channel) snapshot() (*amqp.Channel, chan amqp.Confirmation) {
	this.mu.Lock()
	defer this.mu.Unlock()

	return this.channel, this.confirm
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
		refreshReq: make(chan struct{}, 1),
		id:         currentId,
		open:       true,
	}
}
