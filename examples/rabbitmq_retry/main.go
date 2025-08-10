package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/isayme/go-amqp-reconnect/rabbitmq"
	"github.com/nilorg/eventbus"
)

func main() {
	// 连接到 RabbitMQ
	conn, err := rabbitmq.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("连接 RabbitMQ 失败: %v", err)
	}
	defer conn.Close()

	// 配置 EventBus 选项
	options := &eventbus.RabbitMQOptions{
		ExchangeName:        "retry.test.exchange",
		ExchangeType:        "topic",
		QueueMessageExpires: 864000000, // 10 天
		Serialize:           &eventbus.JSONSerialize{},
		Logger:              &eventbus.StdLogger{},
		PoolMinOpen:         1,
		PoolMaxOpen:         10,

		// 连接重试配置
		MaxRetries:        3,
		RetryInterval:     time.Second * 2,
		BackoffMultiplier: 2.0,
		MaxBackoff:        time.Minute,

		// 消息重试配置
		MessageMaxRetries: 2,
		SkipBadMessages:   true,

		// 死信队列配置
		DeadLetterExchange: "retry.test.dlx",
	}

	// 创建 EventBus
	bus, err := eventbus.NewRabbitMQ(conn, options)
	if err != nil {
		log.Fatalf("创建 EventBus 失败: %v", err)
	}

	// 确保关闭资源
	defer func() {
		if closer, ok := bus.(interface{ Close() error }); ok {
			closer.Close()
		}
	}()

	ctx := context.Background()

	// 模拟错误处理器（前2次失败，第3次成功）
	var attemptCount int
	errorHandler := func(ctx context.Context, msg *eventbus.Message) error {
		attemptCount++
		fmt.Printf("🔄 处理消息 (尝试 %d):\n", attemptCount)
		fmt.Printf("  主题: %v\n", msg.Header["topic"])
		fmt.Printf("  重试次数: %v\n", msg.Header["x-retry-count"])
		fmt.Printf("  消息内容: %+v\n", msg.Value)

		if attemptCount <= 2 {
			fmt.Printf("  ❌ 模拟处理失败\n")
			return fmt.Errorf("模拟处理错误 (尝试 %d)", attemptCount)
		}

		fmt.Printf("  ✅ 处理成功\n")
		return nil
	}

	// 订阅正常消息
	go func() {
		err := bus.SubscribeAsync(ctx, "retry.test.topic", errorHandler)
		if err != nil {
			log.Printf("订阅失败: %v", err)
		}
	}()

	// 订阅死信队列消息
	go func() {
		err := bus.SubscribeAsync(ctx, "retry.test.topic.failed", func(ctx context.Context, msg *eventbus.Message) error {
			fmt.Printf("💀 死信队列收到消息:\n")
			if data, ok := msg.Value.(map[string]interface{}); ok {
				fmt.Printf("  原始ID: %v\n", data["original_id"])
				fmt.Printf("  原始主题: %v\n", data["original_topic"])
				fmt.Printf("  失败时间: %v\n", time.Unix(int64(data["failed_at"].(float64)), 0))
				fmt.Printf("  失败原因: %v\n", data["error_reason"])
				fmt.Printf("  原始数据: %v\n", data["original_values"])
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

	// 发布测试消息
	for i := 0; i < 3; i++ {
		// 创建带有自定义头的上下文
		headerCtx := eventbus.NewSetMessageHeaderContext(ctx, func(ctx context.Context) eventbus.MessageHeader {
			return eventbus.MessageHeader{
				"message_id": fmt.Sprintf("msg_%d_%d", i, time.Now().Unix()),
				"source":     "rabbitmq_retry_example",
				"timestamp":  time.Now().Format(time.RFC3339),
			}
		})

		err := bus.Publish(headerCtx, "retry.test.topic", map[string]interface{}{
			"id":      i,
			"message": fmt.Sprintf("测试重试消息 %d", i),
			"data":    map[string]string{"key": fmt.Sprintf("value_%d", i)},
		})

		if err != nil {
			log.Printf("发布消息失败: %v", err)
		} else {
			fmt.Printf("✅ 发布消息 %d 成功\n", i)
		}

		time.Sleep(time.Second * 3) // 给足够时间观察重试过程
		attemptCount = 0            // 重置计数器
	}

	// 测试发布会失败并进入死信队列的消息
	fmt.Println("\n📋 测试消息进入死信队列...")

	// 重新设置错误处理器（总是失败）
	alwaysFailHandler := func(ctx context.Context, msg *eventbus.Message) error {
		fmt.Printf("❌ 总是失败的处理器: %+v\n", msg.Value)
		return fmt.Errorf("总是失败的处理")
	}

	// 订阅一个新的主题用于测试死信队列
	go func() {
		err := bus.SubscribeAsync(ctx, "retry.test.fail", alwaysFailHandler)
		if err != nil {
			log.Printf("订阅失败主题失败: %v", err)
		}
	}()

	time.Sleep(time.Second)

	// 发布会进入死信队列的消息
	err = bus.Publish(ctx, "retry.test.fail", map[string]interface{}{
		"id":      999,
		"message": "这条消息会进入死信队列",
	})

	if err != nil {
		log.Printf("发布失败消息失败: %v", err)
	} else {
		fmt.Printf("✅ 发布失败消息成功\n")
	}

	// 保持程序运行以观察结果
	fmt.Println("程序运行中，观察重试和死信队列处理...")
	time.Sleep(time.Second * 15)
}
