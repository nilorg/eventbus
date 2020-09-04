package eventbus

import (
	"context"
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
	Publish(ctx context.Context, topic string, v interface{}, callbackName ...string) (err error)
	PublishAsync(ctx context.Context, topic string, v interface{}, callbackName ...string) (err error)
}

// EventBus 事件总线
type EventBus interface {
	Publisher
	Subscriber
}
