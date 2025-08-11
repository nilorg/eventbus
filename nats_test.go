package eventbus

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestNATSEventBus(t *testing.T) {
	// 这个测试需要运行中的NATS服务器
	// 可以使用 docker run -p 4222:4222 nats:latest 启动

	nc, err := nats.Connect("nats://localhost:4222")
	if err != nil {
		t.Skipf("Skipping test - NATS server not available: %v", err)
		return
	}
	defer nc.Close()

	bus, err := NewNATS(nc)
	if err != nil {
		t.Fatalf("Failed to create NATS event bus: %v", err)
	}

	ctx := context.Background()
	topic := "test.eventbus.nats"

	// 测试消息接收
	received := make(chan *Message, 1)
	err = bus.SubscribeAsync(ctx, topic, func(ctx context.Context, msg *Message) error {
		received <- msg
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	// 等待订阅准备完成
	time.Sleep(100 * time.Millisecond)

	// 发布消息
	testMessage := "Hello NATS EventBus!"
	err = bus.Publish(ctx, topic, testMessage)
	if err != nil {
		t.Fatalf("Failed to publish: %v", err)
	}

	// 验证消息接收
	select {
	case msg := <-received:
		if msg.Value != testMessage {
			t.Errorf("Expected %q, got %q", testMessage, msg.Value)
		}
	case <-time.After(5 * time.Second):
		t.Error("Timeout waiting for message")
	}
}

func TestNATSEventBusWithQueueGroup(t *testing.T) {
	nc, err := nats.Connect("nats://localhost:4222")
	if err != nil {
		t.Skipf("Skipping test - NATS server not available: %v", err)
		return
	}
	defer nc.Close()

	bus, err := NewNATS(nc)
	if err != nil {
		t.Fatalf("Failed to create NATS event bus: %v", err)
	}

	ctx := context.Background()
	groupCtx := NewGroupIDContext(ctx, "test-group")
	topic := "test.eventbus.nats.queue"

	// 创建两个队列组消费者
	received1 := make(chan *Message, 10)
	received2 := make(chan *Message, 10)

	err = bus.SubscribeAsync(groupCtx, topic, func(ctx context.Context, msg *Message) error {
		received1 <- msg
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to subscribe consumer 1: %v", err)
	}

	err = bus.SubscribeAsync(groupCtx, topic, func(ctx context.Context, msg *Message) error {
		received2 <- msg
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to subscribe consumer 2: %v", err)
	}

	// 等待订阅准备完成
	time.Sleep(100 * time.Millisecond)

	// 发布多条消息
	messageCount := 10
	for i := 0; i < messageCount; i++ {
		err = bus.Publish(ctx, topic, fmt.Sprintf("Message %d", i))
		if err != nil {
			t.Fatalf("Failed to publish message %d: %v", i, err)
		}
	}

	// 验证消息分发（队列组应该实现负载均衡）
	time.Sleep(1 * time.Second)

	count1 := len(received1)
	count2 := len(received2)
	total := count1 + count2

	if total != messageCount {
		t.Errorf("Expected %d total messages, got %d (consumer1: %d, consumer2: %d)",
			messageCount, total, count1, count2)
	}

	// 队列组应该将消息分发给不同的消费者
	if count1 == 0 || count2 == 0 {
		t.Errorf("Messages should be distributed between consumers, got consumer1: %d, consumer2: %d",
			count1, count2)
	}
}

func TestNATSEventBusClose(t *testing.T) {
	nc, err := nats.Connect("nats://localhost:4222")
	if err != nil {
		t.Skipf("Skipping test - NATS server not available: %v", err)
		return
	}

	bus, err := NewNATS(nc)
	if err != nil {
		t.Fatalf("Failed to create NATS event bus: %v", err)
	}

	// 测试 Closer 接口
	if closer, ok := bus.(Closer); ok {
		err = closer.Close()
		if err != nil {
			t.Errorf("Failed to close event bus: %v", err)
		}
	} else {
		t.Error("NATS event bus should implement Closer interface")
	}
}
