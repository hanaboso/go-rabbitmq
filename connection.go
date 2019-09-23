package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

func SetLogger(log Logger, level LoggingLevel) func(*Connection) {
	return func(connection *Connection) {
		connection.logger = logger{
			Logger: log,
			level:  level,
		}
	}
}

type Connection struct {
	conn            *amqp.Connection
	connM           sync.RWMutex
	logger          logger
	notifyConnClose chan *amqp.Error
	reconnectDelay  time.Duration
	done            chan bool
}

func Connect(dsn string, opts ...func(*Connection)) (*Connection, error) {
	return ConnectCtx(context.Background(), dsn, opts...)
}

func ConnectCtx(ctx context.Context, dsn string, opts ...func(*Connection)) (*Connection, error) {
	conn := Connection{
		logger:         logger{Logger: DeafLogger()},
		done:           make(chan bool),
		reconnectDelay: 5 * time.Second,
	}

	for _, opt := range opts {
		opt(&conn)
	}

	if err := conn.handleReconnect(ctx, dsn); err != nil {
		return nil, err
	}
	go func() {
		for {
			select {
			case err, ok := <-conn.notifyConnClose:
				if !ok {
					// graceful shutdown
					return
				}
				conn.logger.Debugf("connection closed: %v", err)
				if err := conn.handleReconnect(context.Background(), dsn); err != nil {
					conn.logger.Debugf("reconnect error: %v", err)
				}
			case <-conn.done:
				return
			}
		}
	}()

	return &conn, nil
}

func (c *Connection) Close() error {
	// TODO can't be closed twice
	close(c.done)

	c.connM.Lock()
	defer c.connM.Unlock()

	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *Connection) ExchangeDeclare(name, kind string, durable, autoDelete bool) error {
	ch, err := c.connection().Channel()
	if err != nil {
		return err
	}
	return ch.ExchangeDeclare(
		name,               // name
		amqp.ExchangeTopic, // type
		durable,            // durable
		autoDelete,         // auto-deleted
		false,              // internal
		false,              // no-wait
		nil,                // arguments
	)
}

func (c *Connection) handleReconnect(ctx context.Context, dsn string) error {
	for {
		conn, err := amqp.Dial(dsn)
		if err != nil {
			var aErr *amqp.Error
			if errors.As(err, &aErr) && aErr.Code == amqp.AccessRefused {
				return err
			}

			c.logger.Debugf("failed to dial connection: %v", err)

			select {
			case <-c.done:
				return fmt.Errorf("connection is closed")
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.reconnectDelay):
				c.logger.Debug("connection reconnect")
				continue
			}
		}

		c.changeConnection(conn)
		return nil
	}
}

func (c *Connection) connection() *amqp.Connection {
	c.connM.RLock()
	defer c.connM.RUnlock()
	return c.conn
}

func (c *Connection) changeConnection(connection *amqp.Connection) {
	c.connM.Lock()
	defer c.connM.Unlock()

	c.conn = connection
	c.notifyConnClose = make(chan *amqp.Error)
	c.conn.NotifyClose(c.notifyConnClose)
}
