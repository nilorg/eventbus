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
	topic := "test.message"
	ctx = NewRabbitMQRoutingKeyContext(ctx, topic)
	go func() {
		err = bus.Subscribe(ctx, topic, testSubscribeHandel)
		if err != nil {
			t.Error(err)
			return
		}
	}()
	for i := 0; i < 10; i++ {
		err = bus.Publish(ctx, topic, "test.callback")
		if err != nil {
			t.Error(err)
			return
		}
		fmt.Println("Publish test.message")
		time.Sleep(3 * time.Second)
	}
	err = bus.Close()
	if err != nil {
		t.Error(err)
	}
	time.Sleep(time.Second * 5)
}

func TestRabbitMQEventBusASync(t *testing.T) {
	ctx := context.Background()
	bus := newTestEventBus(t)
	var err error
	topic := "test.message"
	ctx = NewRabbitMQRoutingKeyContext(ctx, topic)

	err = bus.SubscribeAsync(ctx, "test1", func(ctx context.Context, msg *Message) error {
		fmt.Printf("11111111111%+v\n", msg)
		return nil
	},
	)
	if err != nil {
		t.Error(err)
		return
	}
	err = bus.SubscribeAsync(ctx, "test1", func(ctx context.Context, msg *Message) error {
		fmt.Printf("11111111111=0---------a%+v\n", msg)
		return nil
	},
	)
	if err != nil {
		t.Error(err)
		return
	}
	err = bus.SubscribeAsync(ctx, "test2",
		func(ctx context.Context, msg *Message) error {
			fmt.Printf("222222222%+v\n", msg)
			return nil
		},
	)
	if err != nil {
		t.Error(err)
		return
	}
	for i := 0; i < 10; i++ {
		err = bus.Publish(ctx, topic, "test.callback")
		if err != nil {
			t.Error(err)
			return
		}
		fmt.Println("Publish test.message")
		time.Sleep(3 * time.Second)
	}
	err = bus.Close()
	if err != nil {
		t.Error(err)
	}
	time.Sleep(time.Second * 5)
}

func testSubscribeHandel(ctx context.Context, msg *Message) error {
	fmt.Printf("%+v\n", msg)
	if msg.ISCallback() {
		fmt.Println("进入回调")
	}
	return nil
}
