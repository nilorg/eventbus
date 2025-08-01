package eventbus

import (
	"context"
)

var (
	// Version 版本
	Version = "v1"
)

// Subscriber 订阅接口
type Subscriber interface {
	Subscribe(ctx context.Context, topic string, h SubscribeHandler) (err error)
	SubscribeAsync(ctx context.Context, topic string, h SubscribeHandler) (err error)
}

// SubscribeHandler 订阅处理
type SubscribeHandler func(ctx context.Context, msg *Message) error

// Publisher 发布接口
type Publisher interface {
	Publish(ctx context.Context, topic string, v interface{}) (err error)
	PublishAsync(ctx context.Context, topic string, v interface{}) (err error)
}

// EventBus 事件总线
type EventBus interface {
	Publisher
	Subscriber
}

// Closer 定义了可以关闭资源的接口
// 某些 EventBus 实现（如 RabbitMQ）需要清理内部资源（如连接池）
// 可以通过类型断言使用此接口: if closer, ok := bus.(eventbus.Closer); ok { closer.Close() }
type Closer interface {
	Close() error
}
