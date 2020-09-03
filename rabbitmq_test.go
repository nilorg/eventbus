package eventbus

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/streadway/amqp"
)

func newTestEventBus(t *testing.T) (bus EventBus) {
	var err error
	var conn *amqp.Connection
	conn, err = amqp.Dial("amqp://root:test123@localhost:5672/")
	if err != nil {
		t.Error(err)
		return
	}
	bus, err = NewRabbitMQ(conn, nil)
	if err != nil {
		t.Error(err)
		return
	}
	return
}

func TestRabbitMQEventBusSync(t *testing.T) {
	ctx := context.Background()
	bus := newTestEventBus(t)
	var err error
	topic := "order.create.success"
	ctxGroup1 := NewGroupIDContext(ctx, "nilorg.events.sync.group1")
	go func() {
		err = bus.Subscribe(ctxGroup1, topic, func(ctx context.Context, msg *Message) error {
			fmt.Printf("group1 %s: %+v\n", topic, msg)
			return nil
		})
		if err != nil {
			t.Error(err)
			return
		}
	}()
	go func() {
		err = bus.Subscribe(ctxGroup1, topic, func(ctx context.Context, msg *Message) error {
			fmt.Printf("group1(copy) %s: %+v\n", topic, msg)
			return nil
		})
		if err != nil {
			t.Error(err)
			return
		}
	}()
	ctxGroup2 := NewGroupIDContext(ctx, "nilorg.events.sync.group2")
	go func() {
		err = bus.Subscribe(ctxGroup2, topic, func(ctx context.Context, msg *Message) error {
			fmt.Printf("group2 %s: %+v\n", topic, msg)
			return nil
		})
		if err != nil {
			t.Error(err)
			return
		}
	}()
	time.Sleep(1 * time.Second)
	for i := 0; i < 10; i++ {
		err = bus.Publish(ctx, topic, "sync message")
		if err != nil {
			t.Error(err)
			return
		}
		fmt.Println("Publish sync success")
		time.Sleep(3 * time.Second)
	}
	time.Sleep(time.Second * 5)
}

func TestRabbitMQEventBusASync(t *testing.T) {
	ctx := context.Background()
	bus := newTestEventBus(t)
	var err error
	topic := "order.create.success"
	ctxGroup1 := NewGroupIDContext(ctx, "nilorg.events.async.group1")
	err = bus.SubscribeAsync(ctxGroup1, topic, func(ctx context.Context, msg *Message) error {
		fmt.Printf("group1 %s: %+v\n", topic, msg)
		return nil
	},
	)
	if err != nil {
		t.Error(err)
		return
	}
	err = bus.SubscribeAsync(ctxGroup1, topic, func(ctx context.Context, msg *Message) error {
		fmt.Printf("group1 %s(copy): %+v\n", topic, msg)
		return nil
	},
	)
	if err != nil {
		t.Error(err)
		return
	}
	ctxGroup2 := NewGroupIDContext(ctx, "nilorg.events.async.group2")
	err = bus.SubscribeAsync(ctxGroup2, topic,
		func(ctx context.Context, msg *Message) error {
			fmt.Printf("group2 %s: %+v\n", topic, msg)
			return nil
		},
	)
	if err != nil {
		t.Error(err)
		return
	}
	time.Sleep(1 * time.Second)
	for i := 0; i < 10; i++ {
		err = bus.PublishAsync(ctx, topic, "async message", "")
		if err != nil {
			t.Error(err)
			return
		}
		fmt.Println("Publish async success")
		time.Sleep(3 * time.Second)
	}
	time.Sleep(time.Second * 5)
}
