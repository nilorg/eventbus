module rabbitmq_retry_example

go 1.14

replace github.com/nilorg/eventbus => ../..

require (
	github.com/isayme/go-amqp-reconnect v0.0.0-20210303120416-fc811b0bcda2
	github.com/nilorg/eventbus v0.0.0-00010101000000-000000000000
)
