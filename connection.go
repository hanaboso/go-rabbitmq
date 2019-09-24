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

// TODO define more options

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

type Exchange struct {
	Name       string
	Type       ExchangeType
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       ArgumentsTable
}

func SetExchangeDurability(durable bool) func(*Exchange) {
	return func(ex *Exchange) {
		ex.Durable = durable
	}
}

// TODO define more options

func (c *Connection) ExchangeDeclare(name string, kind ExchangeType, opts ...func(*Exchange)) error {
	return c.ExchangeDeclareCtx(context.Background(), name, kind, opts...)
}

func (c *Connection) ExchangeDeclareCtx(ctx context.Context, name string, kind ExchangeType, opts ...func(*Exchange)) error {
	exchange := Exchange{
		Name: name,
		Type: kind,
	}
	for _, opt := range opts {
		opt(&exchange)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.reconnectDelay):
			if err := c.exchangeDeclare(exchange); err != nil {
				c.logger.Debug(err)
				continue
			}
		}
		return nil
	}
}

func (c *Connection) exchangeDeclare(exchange Exchange) error {
	ch, err := c.connection().Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	return ch.ExchangeDeclare(
		exchange.Name,             // name
		string(exchange.Type),     // type
		exchange.Durable,          // durable
		exchange.AutoDelete,       // auto-deleted
		exchange.Internal,         // internal
		exchange.NoWait,           // no-wait
		amqp.Table(exchange.Args), // arguments
	)
}

type Queue struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       ArgumentsTable
}

func SetQueueDurability(durable bool) func(*Queue) {
	return func(config *Queue) {
		config.Durable = durable
	}
}

func SetQueueAutoDelete(autoDelete bool) func(*Queue) {
	return func(config *Queue) {
		config.AutoDelete = autoDelete
	}
}

// TODO define more options

func (c *Connection) QueueDeclare(name string, opts ...func(*Queue)) (Queue, error) {
	return c.QueueDeclareCtx(context.Background(), name, opts...)
}

func (c *Connection) QueueDeclareCtx(ctx context.Context, name string, opts ...func(*Queue)) (Queue, error) {
	queue := Queue{
		Name: name,
	}
	for _, opt := range opts {
		opt(&queue)
	}

	for {
		select {
		case <-c.done:
			return Queue{}, fmt.Errorf("")
		case <-ctx.Done():
			return Queue{}, ctx.Err()
		case <-time.After(c.reconnectDelay):
			q, err := c.queueDeclare(queue)
			if err != nil {
				c.logger.Debug(err)
				continue
			}
			return q, nil
		}
	}
}

func (c *Connection) queueDeclare(queue Queue) (Queue, error) {
	ch, err := c.connection().Channel()
	if err != nil {
		return Queue{}, err
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		queue.Name,             // name
		queue.Durable,          // durable
		queue.AutoDelete,       // delete when unused
		queue.Exclusive,        // exclusive
		queue.NoWait,           // no-wait
		amqp.Table(queue.Args), // arguments
	)
	if err != nil {
		return Queue{}, err
	}
	queue.Name = q.Name
	return queue, nil
}

// TODO add config for `noWait` and `args`

func (c *Connection) QueueBind(name, key, exchange string) error {
	return c.QueueBindCtx(context.Background(), name, key, exchange)
}

func (c *Connection) QueueBindCtx(ctx context.Context, name, key, exchange string) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.reconnectDelay):
			if err := c.queueBind(name, key, exchange); err != nil {
				c.logger.Debug(err)
				continue
			}
		}
		return nil
	}
}

func (c *Connection) queueBind(name, key, exchange string) error {
	ch, err := c.connection().Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	return ch.QueueBind(
		name,     // queue name
		key,      // routing key
		exchange, // exchange
		false,
		nil,
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
