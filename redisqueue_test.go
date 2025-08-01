package eventbus

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
)

func TestRedisQueue_PublishSubscribe(t *testing.T) {
	// 连接Redis
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
	defer client.Close()

	// 测试连接
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	// 创建Redis Queue事件总线
	bus, err := NewRedisQueue(client)
	if err != nil {
		t.Fatalf("Failed to create redis queue event bus: %v", err)
	}

	topic := "test_queue_topic"

	// 清理测试数据
	client.Del(ctx, topic)
	defer client.Del(ctx, topic)

	// 订阅消息
	received := make(chan *Message, 1)
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		err := bus.Subscribe(subCtx, topic, func(ctx context.Context, msg *Message) error {
			t.Logf("Received message: %+v", msg)
			received <- msg
			return nil
		})
		if err != nil && err != context.Canceled {
			t.Errorf("Subscribe error: %v", err)
		}
	}()

	// 等待订阅建立
	time.Sleep(100 * time.Millisecond)

	// 发布消息
	testMessage := map[string]interface{}{
		"id":   1,
		"name": "test",
		"data": []string{"a", "b", "c"},
	}

	err = bus.Publish(ctx, topic, testMessage)
	if err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}

	// 验证消息接收
	select {
	case msg := <-received:
		if msg == nil {
			t.Fatal("Received nil message")
		}
		if msg.Value == nil {
			t.Fatal("Message value is nil")
		}
		t.Logf("Test passed: received message %+v", msg.Value)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message")
	}
}

func TestRedisQueue_GroupSubscribe(t *testing.T) {
	// 连接Redis
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
	defer client.Close()

	// 测试连接
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	// 创建Redis Queue事件总线
	bus, err := NewRedisQueue(client)
	if err != nil {
		t.Fatalf("Failed to create redis queue event bus: %v", err)
	}

	topic := "test_group_topic"
	groupID1 := "group1"
	groupID2 := "group2"

	// 清理测试数据
	client.Del(ctx, topic)
	client.Del(ctx, fmt.Sprintf("%s:group:%s", topic, groupID1))
	client.Del(ctx, fmt.Sprintf("%s:group:%s", topic, groupID2))
	defer func() {
		client.Del(ctx, topic)
		client.Del(ctx, fmt.Sprintf("%s:group:%s", topic, groupID1))
		client.Del(ctx, fmt.Sprintf("%s:group:%s", topic, groupID2))
	}()

	// 创建两个消费组
	received1 := make(chan *Message, 5)
	received2 := make(chan *Message, 5)

	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// 消费组1订阅
	groupCtx1 := NewGroupIDContext(subCtx, groupID1)
	go func() {
		err := bus.Subscribe(groupCtx1, topic, func(ctx context.Context, msg *Message) error {
			t.Logf("Group1 received message: %+v", msg)
			received1 <- msg
			return nil
		})
		if err != nil && err != context.Canceled {
			t.Errorf("Group1 subscribe error: %v", err)
		}
	}()

	// 消费组2订阅
	groupCtx2 := NewGroupIDContext(subCtx, groupID2)
	go func() {
		err := bus.Subscribe(groupCtx2, topic, func(ctx context.Context, msg *Message) error {
			t.Logf("Group2 received message: %+v", msg)
			received2 <- msg
			return nil
		})
		if err != nil && err != context.Canceled {
			t.Errorf("Group2 subscribe error: %v", err)
		}
	}()

	// 等待订阅建立
	time.Sleep(200 * time.Millisecond)

	// 发布消息给组1
	pubCtx1 := NewGroupIDContext(ctx, groupID1)
	err = bus.Publish(pubCtx1, topic, map[string]interface{}{
		"group": groupID1,
		"data":  "message for group 1",
	})
	if err != nil {
		t.Fatalf("Failed to publish message for group1: %v", err)
	}

	// 发布消息给组2
	pubCtx2 := NewGroupIDContext(ctx, groupID2)
	err = bus.Publish(pubCtx2, topic, map[string]interface{}{
		"group": groupID2,
		"data":  "message for group 2",
	})
	if err != nil {
		t.Fatalf("Failed to publish message for group2: %v", err)
	}

	// 验证消息接收
	timeout := time.After(5 * time.Second)
	group1Received := 0
	group2Received := 0

	for group1Received == 0 || group2Received == 0 {
		select {
		case msg := <-received1:
			group1Received++
			if msg == nil {
				t.Fatal("Group1 received nil message")
			}
			t.Logf("Group1 test passed: received message %+v", msg.Value)
		case msg := <-received2:
			group2Received++
			if msg == nil {
				t.Fatal("Group2 received nil message")
			}
			t.Logf("Group2 test passed: received message %+v", msg.Value)
		case <-timeout:
			t.Fatalf("Timeout waiting for messages. Group1: %d, Group2: %d", group1Received, group2Received)
		}
	}
}

func TestRedisQueue_AsyncPublishSubscribe(t *testing.T) {
	// 连接Redis
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
	defer client.Close()

	// 测试连接
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	// 创建Redis Queue事件总线
	bus, err := NewRedisQueue(client)
	if err != nil {
		t.Fatalf("Failed to create redis queue event bus: %v", err)
	}

	topic := "test_async_topic"

	// 清理测试数据
	client.Del(ctx, topic)
	defer client.Del(ctx, topic)

	// 异步订阅消息
	received := make(chan *Message, 1)
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	err = bus.SubscribeAsync(subCtx, topic, func(ctx context.Context, msg *Message) error {
		t.Logf("Async received message: %+v", msg)
		received <- msg
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to async subscribe: %v", err)
	}

	// 等待订阅建立
	time.Sleep(200 * time.Millisecond)

	// 异步发布消息
	testMessage := map[string]interface{}{
		"async": true,
		"data":  "async test message",
	}

	err = bus.PublishAsync(ctx, topic, testMessage)
	if err != nil {
		t.Fatalf("Failed to async publish message: %v", err)
	}

	// 验证消息接收
	select {
	case msg := <-received:
		if msg == nil {
			t.Fatal("Received nil message")
		}
		if msg.Value == nil {
			t.Fatal("Message value is nil")
		}
		t.Logf("Async test passed: received message %+v", msg.Value)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for async message")
	}
}

// BenchmarkRedisQueue_Publish 性能测试
func BenchmarkRedisQueue_Publish(b *testing.B) {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
	defer client.Close()

	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		b.Skipf("Redis not available: %v", err)
	}

	bus, err := NewRedisQueue(client)
	if err != nil {
		b.Fatalf("Failed to create redis queue event bus: %v", err)
	}

	topic := "bench_topic"
	defer client.Del(ctx, topic)

	testMessage := map[string]interface{}{
		"id":   1,
		"data": "benchmark test message",
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := bus.Publish(ctx, topic, testMessage); err != nil {
				b.Errorf("Publish error: %v", err)
			}
		}
	})
}
