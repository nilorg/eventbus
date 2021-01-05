package main

import (
	"context"
	"fmt"
	"time"

	"github.com/nilorg/eventbus"
	"github.com/streadway/amqp"
)

func main() {
	var err error
	var conn *amqp.Connection
	conn, err = amqp.Dial("amqp://root:test123@localhost:5672/")
	if err != nil {
		panic(err)
	}
	var bus eventbus.EventBus
	ops := eventbus.DefaultRabbitMQOptions
	ops.PoolMinOpen = 1
	ops.PoolMaxOpen = 100
	bus, err = eventbus.NewRabbitMQ(conn, &ops)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()

	topic := "eventbus.examples.rabbitmq.async"
	ctxGroup1 := eventbus.NewGroupIDContext(ctx, "group1")
	err = bus.SubscribeAsync(ctxGroup1, topic, func(ctx context.Context, msg *eventbus.Message) error {
		fmt.Printf("group1 %s: %+v\n", topic, msg)
		return nil
	},
	)
	if err != nil {
		panic(err)
	}
	ctxGroup2 := eventbus.NewGroupIDContext(ctx, "group2")
	err = bus.SubscribeAsync(ctxGroup2, topic,
		func(ctx context.Context, msg *eventbus.Message) error {
			fmt.Printf("group2 %s: %+v\n", topic, msg)
			return nil
		},
	)
	if err != nil {
		panic(err)
	}
	time.Sleep(1 * time.Second)
	for i := 0; i < 5000; i++ {
		go func() {
			err = bus.PublishAsync(ctx, topic, fmt.Sprintf("async message: %d", i))
			if err != nil {
				panic(err)
			}
		}()
		time.Sleep(10 * time.Millisecond)
	}
}
