package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/nilorg/eventbus"
)

func main() {
	// 创建 Redis 客户端
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	// 配置 EventBus
	options := &eventbus.RedisOptions{
		ReadCount:         1,
		ReadBlock:         time.Second * 30,
		Serialize:         &eventbus.JSONSerialize{},
		Logger:            &eventbus.StdLogger{},
		MaxRetries:        3,
		RetryInterval:     time.Second * 2,
		BackoffMultiplier: 2.0,
		MaxBackoff:        time.Minute,
		MessageMaxRetries: 2,
		SkipBadMessages:   true,
		DeadLetterTopic:   "custom.header.dlq",
	}

	bus, err := eventbus.NewRedis(rdb, options)
	if err != nil {
		log.Fatalf("创建 EventBus 失败: %v", err)
	}

	ctx := context.Background()

	// 订阅消息
	go func() {
		err := bus.SubscribeAsync(ctx, "custom.header.topic", func(ctx context.Context, msg *eventbus.Message) error {
			fmt.Printf("🔔 收到消息:\n")
			fmt.Printf("  主题: %v\n", msg.Header["topic"])
			fmt.Printf("  用户ID: %v\n", msg.Header["user_id"])
			fmt.Printf("  请求ID: %v\n", msg.Header["request_id"])
			fmt.Printf("  时间戳: %v\n", msg.Header["timestamp"])
			fmt.Printf("  消息内容: %+v\n", msg.Value)
			fmt.Println("---")
			return nil
		})
		if err != nil {
			log.Printf("订阅失败: %v", err)
		}
	}()

	// 订阅死信队列消息（统一格式）
	go func() {
		err := bus.SubscribeAsync(ctx, "custom.header.dlq", func(ctx context.Context, msg *eventbus.Message) error {
			fmt.Printf("💀 死信队列收到消息:\n")
			if data, ok := msg.Value.(map[string]interface{}); ok {
				fmt.Printf("  原始ID: %v\n", data["original_id"])
				fmt.Printf("  原始主题: %v\n", data["original_topic"])
				fmt.Printf("  失败时间: %v\n", time.Unix(int64(data["failed_at"].(float64)), 0))
				fmt.Printf("  失败原因: %v\n", data["error_reason"])
			}
			fmt.Println("---")
			return nil
		})
		if err != nil {
			log.Printf("订阅死信队列失败: %v", err)
		}
	}()

	// 等待订阅启动
	time.Sleep(time.Second * 2)

	// 使用自定义消息头发布消息
	for i := 0; i < 3; i++ {
		// 创建带有自定义头的上下文
		headerCtx := eventbus.NewSetMessageHeaderContext(ctx, func(ctx context.Context) eventbus.MessageHeader {
			return eventbus.MessageHeader{
				"user_id":    fmt.Sprintf("user_%d", i),
				"request_id": fmt.Sprintf("req_%d_%d", i, time.Now().Unix()),
				"timestamp":  time.Now().Format(time.RFC3339),
				"source":     "custom_header_example",
			}
		})

		// 发布消息
		err := bus.Publish(headerCtx, "custom.header.topic", map[string]interface{}{
			"id":      i,
			"message": fmt.Sprintf("自定义头消息 %d", i),
			"data":    map[string]string{"key": fmt.Sprintf("value_%d", i)},
		})

		if err != nil {
			log.Printf("发布消息失败: %v", err)
		} else {
			fmt.Printf("✅ 发布消息 %d 成功\n", i)
		}

		time.Sleep(time.Second)
	}

	// 保持程序运行
	fmt.Println("程序运行中，观察消息头信息...")
	time.Sleep(time.Second * 10)
}
