package eventbus

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/streadway/amqp"
)

func TestRabbitMQEventBus(t *testing.T) {
	var err error
	var conn *amqp.Connection
	conn, err = amqp.Dial("amqp://root:test123@localhost:5672/")
	if err != nil {
		t.Error(err)
		return
	}
	var bus EventBus
	bus, err = NewRabbitMQ(conn, nil)
	if err != nil {
		t.Error(err)
		return
	}
	ctx := context.Background()
	topic := "test.message"
	// ctx = NewRabbitMQRoutingKeyContext(ctx, topic)
	// go func() {
	// 	err = bus.Subscribe(ctx, topic, testSubscribeHandel)
	// 	if err != nil {
	// 		t.Error(err)
	// 		return
	// 	}
	// }()
	err = bus.SubscribeAsync(ctx, topic, testSubscribeHandel)
	if err != nil {
		t.Error(err)
		return
	}
	err = bus.SubscribeAsync(ctx, topic,
		func(ctx context.Context, msg *Message) error {
			fmt.Printf("222222222%+v\n", msg)
			return nil
		},
	)
	if err != nil {
		t.Error(err)
		return
	}
	for {
		err = bus.PublishAsync(ctx, topic, "", "hhhhhh")
		if err != nil {
			t.Error(err)
			return
		}
		fmt.Println("Publish test.message")
		time.Sleep(3 * time.Second)
	}
}

func testSubscribeHandel(ctx context.Context, msg *Message) error {
	fmt.Printf("%+v\n", msg)
	return nil
}
