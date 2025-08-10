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
		Addr: "localhost:6379", // 如果 Redis 服务器不可用，这里会触发连接错误
	})

	// 配置带有重试机制和消息处理优化的 Redis EventBus 选项
	options := &eventbus.RedisOptions{
		ReadCount:         1,
		ReadBlock:         time.Second * 30,
		Serialize:         &eventbus.JSONSerialize{},
		Logger:            &eventbus.StdLogger{},
		MaxRetries:        5,                // 连接最大重试 5 次
		RetryInterval:     time.Second * 3,  // 初始重试间隔 3 秒
		BackoffMultiplier: 2.0,              // 指数退避倍数
		MaxBackoff:        time.Minute * 2,  // 最大退避时间 2 分钟
		MessageMaxRetries: 3,                // 单条消息最大重试 3 次
		SkipBadMessages:   true,             // 跳过无法处理的消息
		DeadLetterTopic:   "test.topic.dlq", // 死信队列主题
	}

	// 创建 EventBus
	bus, err := eventbus.NewRedis(rdb, options)
	if err != nil {
		log.Fatalf("创建 EventBus 失败: %v", err)
	}

	ctx := context.Background()

	// 订阅消息（异步）
	go func() {
		err := bus.SubscribeAsync(ctx, "test.topic", func(ctx context.Context, msg *eventbus.Message) error {
			// 现在可以从消息头中获取 topic 信息
			topic := msg.Header["topic"]
			fmt.Printf("收到来自主题 [%v] 的消息: %+v\n", topic, msg.Value)
			fmt.Printf("消息头信息: %+v\n", msg.Header)

			// 模拟处理可能失败的情况
			if data, ok := msg.Value.(map[string]interface{}); ok {
				if message, exists := data["message"].(string); exists && message == "测试消息 2" {
					return fmt.Errorf("模拟处理失败")
				}
			}

			return nil
		})
		if err != nil {
			log.Printf("订阅失败: %v", err)
		}
	}() // 订阅死信队列消息
	go func() {
		err := bus.SubscribeAsync(ctx, "test.topic.dlq", func(ctx context.Context, msg *eventbus.Message) error {
			fmt.Printf("死信队列收到消息: %+v\n", msg.Value)
			// 检查死信消息中的原始主题信息
			if data, ok := msg.Value.(map[string]interface{}); ok {
				if originalTopic, exists := data["original_topic"]; exists {
					fmt.Printf("原始主题: %v\n", originalTopic)
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

	// 发布一些测试消息
	for i := 0; i < 5; i++ {
		err := bus.Publish(ctx, "test.topic", map[string]interface{}{
			"message": fmt.Sprintf("测试消息 %d", i),
			"time":    time.Now(),
		})
		if err != nil {
			log.Printf("发布消息失败: %v", err)
		} else {
			fmt.Printf("发布消息 %d 成功\n", i)
		}
		time.Sleep(time.Second)
	}

	// 保持程序运行
	fmt.Println("程序运行中，等待消息处理...")
	time.Sleep(time.Minute)
}
