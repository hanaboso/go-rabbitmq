package pkg

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hanaboso/go-log/pkg/null"
	"github.com/jpillora/backoff"
	"github.com/streadway/amqp"

	log "github.com/hanaboso/go-log/pkg"
)

// ConnectionWithLogger provides connection with logger.
func ConnectionWithLogger(log log.Logger) func(*Connection) {
	return func(connection *Connection) {
		connection.logger = log
	}
}

// ConnectionWithConfig provides connection with advanced config from underlying library.
func ConnectionWithConfig(config amqp.Config) func(*Connection) {
	return func(connection *Connection) {
		connection.Config = config
	}
}

// ConnectionWithPrefetchLimit sets prefetch limit for every consumer.
// This rule can't be overridden by local limit, stricter limit is applied.
func ConnectionWithPrefetchLimit(count int) func(*Connection) {
	return func(connection *Connection) {
		connection.prefetchCount = count
	}
}

// ConnectionWithCustomBackOff sets custom back-off interval.
func ConnectionWithCustomBackOff(min time.Duration, max time.Duration, factor float64, jitter bool) func(*Connection) {
	return func(connection *Connection) {
		connection.reconnectDelay = createBackOff(min, max, factor, jitter)
		connection.publishDelay = createBackOff(min, max, factor, jitter)
		connection.subscribeDelay = createBackOff(min, max, factor, jitter)
	}
}

// Connection is wrapper over (*amqp.Connection) with ability to reconnect.
type Connection struct {
	Config          amqp.Config
	closeOnce       sync.Once
	conn            *amqp.Connection
	connM           sync.RWMutex
	done            chan bool
	logger          log.Logger
	notifyConnClose chan *amqp.Error
	prefetchCount   int
	publishDelay    *backoff.Backoff
	reconnectDelay  *backoff.Backoff
	subscribeDelay  *backoff.Backoff
}

// Connect connects to provided DSN with context and returns Connection with started reconnect goroutine.
// Connection reconnect doesn't rely on context.
func Connect(ctx context.Context, dsn string, options ...func(*Connection)) (*Connection, error) {
	conn := Connection{
		done:           make(chan bool),
		logger:         null.NewLogger(),
		publishDelay:   defaultBackOff(),
		subscribeDelay: defaultBackOff(),
		reconnectDelay: defaultBackOff(),
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
				conn.logger.WithFields(map[string]interface{}{
					"error": err.Error(),
				}).Debug("connection closed")
				if err := conn.handleReconnect(context.Background(), dsn); err != nil {
					conn.logger.WithFields(map[string]interface{}{
						"error": err.Error(),
					}).Debug("reconnect")
				}
			case <-conn.done:
				return
			}
		}
	}()

	return &conn, nil
}

// Close closes connection.
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

// NewExchange returns new exchange.
func NewExchange(name string, kind ExchangeType, options ...func(exchange *Exchange)) Exchange {
	exchange := Exchange{
		Name: name,
		Type: kind,
	}
	for _, option := range options {
		option(&exchange)
	}

	return exchange
}

// ExchangeDeclare declares exchange with context.
func (c *Connection) ExchangeDeclare(ctx context.Context, exchange Exchange) error {
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

			c.logger.Info(err.Error())

			select {
			case <-c.done:
				return errors.New("connection is done")
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.reconnectDelay.Duration()):
			}
			continue
		}

		c.reconnectDelay.Reset()
		return nil
	}
}

func (c *Connection) exchangeDeclare(exchange Exchange) error {
	ch, err := c.connection().Channel()
	if err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()

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

// NewQueue returns new Queue.
func NewQueue(name string, options ...func(*Queue)) Queue {
	queue := Queue{
		Name: name,
		Args: Arguments{},
	}
	for _, option := range options {
		option(&queue)
	}

	return queue
}

// QueueDeclare declares queue with context.
func (c *Connection) QueueDeclare(ctx context.Context, queue Queue) (Queue, error) {
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

			c.logger.Info(err.Error())

			select {
			case <-c.done:
				return Queue{}, errors.New("connection is done")
			case <-ctx.Done():
				return Queue{}, ctx.Err()
			case <-time.After(c.reconnectDelay.Duration()):
			}
			continue
		}
		c.reconnectDelay.Reset()

		return q, nil
	}
}

func (c *Connection) queueDeclare(queue Queue) (Queue, error) {
	ch, err := c.connection().Channel()
	if err != nil {
		return Queue{}, err
	}
	defer func() { _ = ch.Close() }()

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

			c.logger.Debug(err.Error())

			select {
			case <-c.done:
				return errors.New("connection is done")
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.reconnectDelay.Duration()):
			}
			continue
		}
		break
	}

	c.reconnectDelay.Reset()
	queue.bindings = append(queue.bindings, binding)
	return nil
}

func (c *Connection) queueBind(binding Binding) error {
	ch, err := c.connection().Channel()
	if err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()

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

			c.logger.WithFields(map[string]interface{}{
				"error": err.Error(),
			}).Debug("dial connection")

			select {
			case <-c.done:
				return fmt.Errorf("connection is closed")
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.reconnectDelay.Duration()):
				c.logger.Debug("connection reconnect")
				continue
			}
		}

		if err := c.presetConnection(conn); err != nil {
			c.logger.WithFields(map[string]interface{}{
				"error": err.Error(),
			}).Debug("preset connection")
			if err := conn.Close(); err != nil {
				c.logger.WithFields(map[string]interface{}{
					"error": err.Error(),
				}).Debug("close connection")
			}
			continue
		}

		c.changeConnection(conn)
		c.reconnectDelay.Reset()
		return nil
	}
}

func (c *Connection) presetConnection(conn *amqp.Connection) error {
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %v", err)
	}
	defer func() { _ = ch.Close() }()

	if c.prefetchCount > 0 {
		if err := ch.Qos(c.prefetchCount, 0, true); err != nil {
			return fmt.Errorf("failed to set QoS: %v", err)
		}
	}

	return nil
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
