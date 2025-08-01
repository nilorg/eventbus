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
	// 连接 RabbitMQ
	conn, err := rabbitmq.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("连接 RabbitMQ 失败: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("关闭连接失败: %v", err)
		}
	}()

	// 创建事件总线
	bus, err := eventbus.NewRabbitMQ(conn)
	if err != nil {
		log.Fatalf("创建事件总线失败: %v", err)
	}

	// 使用 defer 确保资源清理
	defer func() {
		// 使用统一的 Closer 接口调用 Close 方法
		if closer, ok := bus.(eventbus.Closer); ok {
			if err := closer.Close(); err != nil {
				log.Printf("关闭事件总线失败: %v", err)
			} else {
				fmt.Println("✅ 事件总线已关闭")
			}
		} else {
			fmt.Println("⚠️  当前事件总线实现不支持 Close 方法")
		}
	}()

	ctx := context.Background()
	topic := "close_example"

	// 订阅消息
	go func() {
		err := bus.Subscribe(ctx, topic, func(ctx context.Context, msg *eventbus.Message) error {
			fmt.Printf("📨 收到消息: %+v\n", msg.Value)
			return nil
		})
		if err != nil {
			log.Printf("订阅失败: %v", err)
		}
	}()

	// 等待订阅建立
	time.Sleep(100 * time.Millisecond)

	// 发布几条消息
	for i := 0; i < 3; i++ {
		message := map[string]interface{}{
			"id":      i,
			"content": fmt.Sprintf("测试消息 %d", i),
			"time":    time.Now().Format(time.RFC3339),
		}

		err := bus.Publish(ctx, topic, message)
		if err != nil {
			log.Printf("发布消息失败: %v", err)
		} else {
			fmt.Printf("📤 发布消息 %d\n", i)
		}

		time.Sleep(500 * time.Millisecond)
	}

	// 等待消息处理
	time.Sleep(2 * time.Second)

	fmt.Println("🔄 准备关闭...")

	// 演示不同的类型断言方式
	demonstrateTypeAssertions(bus)
}

// 演示不同的类型断言方式
func demonstrateTypeAssertions(bus eventbus.EventBus) {
	fmt.Println("=== 类型断言示例 ===")

	// 方法1: 使用统一的 eventbus.Closer 接口
	if closer, ok := bus.(eventbus.Closer); ok {
		fmt.Println("✅ 支持 eventbus.Closer 接口")
		_ = closer // 可以调用 closer.Close()
	}

	// 方法2: 检查是否实现了特定接口（通用方式）
	if closer, ok := bus.(interface{ Close() error }); ok {
		fmt.Println("✅ 支持 Close() error 方法")
		_ = closer // 可以调用 closer.Close()
	}

	// 方法3: 使用 switch 类型断言
	switch v := bus.(type) {
	case eventbus.Closer:
		fmt.Println("✅ 这个实现支持 eventbus.Closer 资源清理")
		_ = v
	case interface{ Close() error }:
		fmt.Println("✅ 这个实现支持通用资源清理")
		_ = v
	default:
		fmt.Println("⚠️  这个实现不支持资源清理")
	}
}
