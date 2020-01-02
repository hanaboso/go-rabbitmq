package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

// ConnectionWithLogger provides connection with logger.
func ConnectionWithLogger(log Logger, level LoggingLevel) func(*Connection) {
	return func(connection *Connection) {
		connection.logger = logger{
			Logger: log,
			level:  level,
		}
	}
}

// ConnectionWithConfig provides connection with advanced config from underlying library.
func ConnectionWithConfig(config amqp.Config) func(*Connection) {
	return func(connection *Connection) {
		connection.Config = config
	}
}

// TODO define more options

// Connection is wrapper over (*amqp.Connection) with ability to reconnect.
type Connection struct {
	conn            *amqp.Connection
	connM           sync.RWMutex
	logger          logger
	notifyConnClose chan *amqp.Error
	reconnectDelay  time.Duration
	done            chan bool
	closeOnce       sync.Once
	Config          amqp.Config
}

// Connect connects to provided DSN with context and returns Connection with started reconnect goroutine.
// Connection reconnect doesn't rely on context.
func Connect(ctx context.Context, dsn string, options ...func(*Connection)) (*Connection, error) {
	conn := Connection{
		logger:         logger{Logger: DeafLogger()},
		done:           make(chan bool),
		reconnectDelay: 5 * time.Second,
		Config: amqp.Config{
			Heartbeat: 10 * time.Second, // amqp.defaultHeartbeat
			Locale:    "en_US",          // amqp.defaultLocale
		},
	}

	for _, option := range options {
		option(&conn)
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

// Close closes connection
func (c *Connection) Close() error {
	c.closeOnce.Do(func() {
		close(c.done)
	})

	c.connM.Lock()
	defer c.connM.Unlock()

	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// ExchangeDeclare declares exchange with context
func (c *Connection) ExchangeDeclare(ctx context.Context, name string, kind ExchangeType, options ...func(*Exchange)) error {
	exchange := Exchange{
		Name: name,
		Type: kind,
	}
	for _, option := range options {
		option(&exchange)
	}

	for {
		if err := c.exchangeDeclare(exchange); err != nil {
			var aErr *amqp.Error
			if errors.As(err, &aErr) {
				switch aErr.Code {
				case amqp.AccessRefused:
					return err
				case amqp.PreconditionFailed:
					return err
				}
			}

			c.logger.Info(err)

			select {
			case <-c.done:
				return errors.New("connection is done")
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.reconnectDelay):
			}
			continue
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

// QueueDeclare declares queue with context
func (c *Connection) QueueDeclare(ctx context.Context, name string, options ...func(*Queue)) (Queue, error) {
	queue := Queue{
		Name: name,
	}
	for _, option := range options {
		option(&queue)
	}

	for {
		q, err := c.queueDeclare(queue)
		if err != nil {
			var aErr *amqp.Error
			if errors.As(err, &aErr) {
				switch aErr.Code {
				case amqp.AccessRefused:
					return Queue{}, err
				case amqp.PreconditionFailed:
					return Queue{}, err
				}
			}

			c.logger.Info(err)

			select {
			case <-c.done:
				return Queue{}, errors.New("connection is done")
			case <-ctx.Done():
				return Queue{}, ctx.Err()
			case <-time.After(c.reconnectDelay):
			}
			continue
		}
		return q, nil
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

// QueueBind binds queue on exchange with provided routing key with context.
func (c *Connection) QueueBind(ctx context.Context, queue *Queue, key, exchange string, options ...func(*Binding)) error {
	if queue == nil {
		return errors.New("queue can't be nil")
	}

	binding := Binding{
		Name:       queue.Name,
		RoutingKey: key,
		Exchange:   exchange,
	}

	for _, option := range options {
		option(&binding)
	}

	for {
		if err := c.queueBind(binding); err != nil {
			var aErr *amqp.Error
			if errors.As(err, &aErr) {
				switch aErr.Code {
				case amqp.AccessRefused:
					return err
				case amqp.PreconditionFailed:
					return err
				}
			}

			c.logger.Debug(err)

			select {
			case <-c.done:
				return errors.New("connection is done")
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.reconnectDelay):
			}
			continue
		}
		break
	}

	queue.bindings = append(queue.bindings, binding)
	return nil
}

func (c *Connection) queueBind(binding Binding) error {
	ch, err := c.connection().Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	return ch.QueueBind(
		binding.Name,       // queue name
		binding.RoutingKey, // routing key
		binding.Exchange,   // exchange
		binding.NoWait,
		amqp.Table(binding.Args),
	)
}

func (c *Connection) handleReconnect(ctx context.Context, dsn string) error {
	for {
		conn, err := amqp.DialConfig(dsn, c.Config)
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
