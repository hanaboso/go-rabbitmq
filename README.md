# github.com/hanaboso/go-rabbitmq

Auto-reconnecting RabbitMQ client library build on top of [github.com/streadway/amqp](https://github.com/streadway/amqp)

# TODOs
- redeclare exchange for publish (direct exchange, declare queue??)
- redeclare queue and binding for subscribe.
- persistent messages
- quorum queues
- ack after publish - volitelně
- vždy queue durable (default)
- handling pro redelivered flag