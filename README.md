# Rabbit MQ

## github.com/hanaboso/go-rabbitmq

Auto-reconnecting RabbitMQ client library build on top of [github.com/streadway/amqp](https://github.com/streadway/amqp)

## Usage
```
import rabbitmq "github.com/hanaboso/go-rabbitmq/pkg"

conn, err := rabbitmq.Connect(ctx, dataSourceName,
    rabbitmq.ConnectionWithLogger(logger),
    rabbitmq.ConnectionWithConfig(amqp.Config{}),
    rabbitmq.ConnectionWithCustomBackOff(),
)

// Publisher
pub, err := rabbitmq.NewPublisher(ctx, conn)
defer func() { _ = pub.Close() }()

err := pub.Publish(ctx, "exchange", "routing.key", buff.Bytes(),
    rabbitmq.PublishingWithDeliveryMode(rabbitmq.Persistent),
)

// Subscriber
sub, err := rabbitmq.NewSubscriber(ctx, conn)
defer func() { _ = sub.Close() }()

q, err := conn.QueueDeclare(ctx, rabbitmq.NewQueue("routing.key", rabbitmq.QueueWithDurability(true)))

msgs, err := sub.Subscribe(ctx, &q)
```

More detailed examples can be found in `/pkg/example_test.go`

## Configuration
- RABBIT_DSN
    - Required
    - RabbitMq DSN
    - Example: `amqp://guest:guest@rabbitmq:5672/`

- APP_DEBUG
    - Optional (default `false`)
    - Controls logger level
    - Example: `true` | `false`

## Development
- `make init-dev` - Runs `docker-compose.yml`
- `make test` - Runs tests
- `http://127.0.0.22:15672` - RabbitMq

## Technologies
- Go 1.13+

## Dependencies
- RabbitMq
- GoLogger `github.com/hanaboso/go-log`
