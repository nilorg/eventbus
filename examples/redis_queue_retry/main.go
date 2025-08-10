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

	// 配置带有重试机制和消息处理优化的 Redis Queue EventBus 选项
	options := &eventbus.RedisQueueOptions{
		PollInterval:      time.Second * 5,
		Serialize:         &eventbus.JSONSerialize{},
		Logger:            &eventbus.StdLogger{},
		MaxRetries:        5,                      // 连接最大重试 5 次
		RetryInterval:     time.Second * 3,        // 初始重试间隔 3 秒
		BackoffMultiplier: 2.0,                    // 指数退避倍数
		MaxBackoff:        time.Minute * 2,        // 最大退避时间 2 分钟
		MessageMaxRetries: 3,                      // 单条消息最大重试 3 次
		SkipBadMessages:   true,                   // 跳过无法处理的消息
		DeadLetterTopic:   "queue.test.topic.dlq", // 死信队列主题
	}

	// 创建 Redis Queue EventBus
	bus, err := eventbus.NewRedisQueue(rdb, options)
	if err != nil {
		log.Fatalf("创建 Redis Queue EventBus 失败: %v", err)
	}

	ctx := context.Background()

	// 订阅消息（异步）
	go func() {
		err := bus.SubscribeAsync(ctx, "queue.test.topic", func(ctx context.Context, msg *eventbus.Message) error {
			// 现在可以从消息头中获取 topic 信息
			topic := msg.Header["topic"]
			fmt.Printf("📦 队列收到来自主题 [%v] 的消息: %+v\n", topic, msg.Value)
			fmt.Printf("📦 消息头信息: %+v\n", msg.Header)

			// 模拟处理可能失败的情况
			if data, ok := msg.Value.(map[string]interface{}); ok {
				if message, exists := data["message"].(string); exists && message == "队列测试消息 2" {
					return fmt.Errorf("模拟队列处理失败")
				}
			}

			return nil
		})
		if err != nil {
			log.Printf("订阅队列失败: %v", err)
		}
	}()

	// 订阅死信队列消息
	go func() {
		err := bus.SubscribeAsync(ctx, "queue.test.topic.dlq", func(ctx context.Context, msg *eventbus.Message) error {
			fmt.Printf("💀 死信队列收到消息: %+v\n", msg.Value)
			// 检查死信消息中的原始主题信息（统一格式）
			if data, ok := msg.Value.(map[string]interface{}); ok {
				if originalTopic, exists := data["original_topic"]; exists {
					fmt.Printf("💀 原始主题: %v\n", originalTopic)
				}
				if originalId, exists := data["original_id"]; exists {
					fmt.Printf("💀 原始ID: %v\n", originalId)
				}
				if failedAt, exists := data["failed_at"]; exists {
					fmt.Printf("💀 失败时间: %v\n", time.Unix(int64(failedAt.(float64)), 0))
				}
				if errorReason, exists := data["error_reason"]; exists {
					fmt.Printf("💀 失败原因: %v\n", errorReason)
				}
			}
			return nil
		})
		if err != nil {
			log.Printf("订阅死信队列失败: %v", err)
		}
	}()

	// 等待订阅启动
	time.Sleep(time.Second * 2)

	// 使用自定义消息头发布消息
	for i := 0; i < 5; i++ {
		// 创建带有自定义头的上下文
		headerCtx := eventbus.NewSetMessageHeaderContext(ctx, func(ctx context.Context) eventbus.MessageHeader {
			return eventbus.MessageHeader{
				"user_id":    fmt.Sprintf("queue_user_%d", i),
				"request_id": fmt.Sprintf("queue_req_%d_%d", i, time.Now().Unix()),
				"timestamp":  time.Now().Format(time.RFC3339),
				"source":     "redis_queue_example",
			}
		})

		// 发布消息到队列
		err := bus.Publish(headerCtx, "queue.test.topic", map[string]interface{}{
			"id":      i,
			"message": fmt.Sprintf("队列测试消息 %d", i),
			"data":    map[string]string{"queue_key": fmt.Sprintf("queue_value_%d", i)},
		})

		if err != nil {
			log.Printf("发布队列消息失败: %v", err)
		} else {
			fmt.Printf("✅ 发布队列消息 %d 成功\n", i)
		}

		time.Sleep(time.Second)
	}

	// 保持程序运行
	fmt.Println("📦 Redis Queue 程序运行中，观察消息头信息和重试机制...")
	time.Sleep(time.Second * 30)
}
