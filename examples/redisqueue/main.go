package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/nilorg/eventbus"
)

func main() {
	// 连接Redis
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password
		DB:       0,  // default DB
	})
	defer client.Close()

	ctx := context.Background()

	// 测试连接
	if err := client.Ping(ctx).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	fmt.Println("Connected to Redis successfully")

	// 创建Redis Queue事件总线
	bus, err := eventbus.NewRedisQueue(client)
	if err != nil {
		log.Fatalf("Failed to create Redis Queue event bus: %v", err)
	}

	// 创建上下文和信号处理
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// 监听系统信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 启动消费者
	go startConsumer(ctx, bus)
	go startGroupConsumer(ctx, bus, "group1")
	go startGroupConsumer(ctx, bus, "group2")

	// 启动生产者
	go startProducer(ctx, bus)

	fmt.Println("Redis Queue example started. Press Ctrl+C to stop...")

	// 等待信号
	<-sigChan
	fmt.Println("\nShutting down...")
	cancel()

	// 等待一段时间让goroutine优雅退出
	time.Sleep(2 * time.Second)
	fmt.Println("Shutdown complete")
}

func startConsumer(ctx context.Context, bus eventbus.EventBus) {
	topic := "simple_topic"

	fmt.Println("Starting simple consumer for topic:", topic)

	err := bus.SubscribeAsync(ctx, topic, func(ctx context.Context, msg *eventbus.Message) error {
		fmt.Printf("[Simple Consumer] Received message: %+v\n", msg.Value)
		return nil
	})

	if err != nil {
		log.Printf("Failed to subscribe to %s: %v", topic, err)
	}
}

func startGroupConsumer(ctx context.Context, bus eventbus.EventBus, groupID string) {
	topic := "group_topic"

	fmt.Printf("Starting group consumer %s for topic: %s\n", groupID, topic)

	// 创建带GroupID的上下文
	groupCtx := eventbus.NewGroupIDContext(ctx, groupID)

	err := bus.SubscribeAsync(groupCtx, topic, func(ctx context.Context, msg *eventbus.Message) error {
		fmt.Printf("[Group %s] Received message: %+v\n", groupID, msg.Value)
		return nil
	})

	if err != nil {
		log.Printf("Failed to subscribe group %s to %s: %v", groupID, topic, err)
	}
}

func startProducer(ctx context.Context, bus eventbus.EventBus) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	counter := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			counter++

			// 发布到简单topic
			simpleMsg := map[string]interface{}{
				"id":        counter,
				"message":   fmt.Sprintf("Simple message %d", counter),
				"timestamp": time.Now().Unix(),
			}

			if err := bus.PublishAsync(ctx, "simple_topic", simpleMsg); err != nil {
				log.Printf("Failed to publish simple message: %v", err)
			} else {
				fmt.Printf("[Producer] Published simple message %d\n", counter)
			}

			// 发布到组topic (轮流发给不同组)
			groupID := "group1"
			if counter%2 == 0 {
				groupID = "group2"
			}

			groupCtx := eventbus.NewGroupIDContext(ctx, groupID)
			groupMsg := map[string]interface{}{
				"id":        counter,
				"message":   fmt.Sprintf("Group message %d for %s", counter, groupID),
				"timestamp": time.Now().Unix(),
				"target":    groupID,
			}

			if err := bus.PublishAsync(groupCtx, "group_topic", groupMsg); err != nil {
				log.Printf("Failed to publish group message: %v", err)
			} else {
				fmt.Printf("[Producer] Published group message %d to %s\n", counter, groupID)
			}
		}
	}
}
