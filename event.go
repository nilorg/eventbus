package eventbus

import (
	"context"
	"google.golang.org/protobuf/proto"
)

func PublishEvent[T proto.Message](bus EventBus, ctx context.Context, topic string, event T) error {
	return bus.Publish(ctx, topic, event)
}

func SubscribeEvent[T proto.Message](bus EventBus, ctx context.Context, topic string, handler func(ctx context.Context, event T) error, opts ...SubscribeOption) error {
	wrappedHandler := func(ctx context.Context, msg *Message) error {
		var event T
		if err := proto.Unmarshal(msg.Value, event); err != nil {
			return err
		}
		return handler(ctx, event)
	}

	return bus.Subscribe(ctx, topic, wrappedHandler, opts...)
}