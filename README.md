# github.com/hanaboso/go-rabbitmq

Auto-reconnecting RabbitMQ client library build on top of [github.com/streadway/amqp](https://github.com/streadway/amqp)

# TODOs
- redeclare exchange for publish (direct exchange, declare queue??)
- redeclare queue anb binding for subscribe.
- persistent queue/messages
- quorum queues
- ack after publish