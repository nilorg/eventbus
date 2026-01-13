package eventbus

import "context"

// NATSJetStreamDeliveryMode NATS JetStream 消息投递模式
// 这是 JetStream 特有的概念，其他消息队列（如 RabbitMQ）不需要
type NATSJetStreamDeliveryMode int

const (
	// NATSJetStreamDeliveryModeWorkQueue 工作队列模式 - 消息只被一个消费者处理（负载均衡）
	// Group 在此模式下无意义，所有实例共享同一个 Consumer
	NATSJetStreamDeliveryModeWorkQueue NATSJetStreamDeliveryMode = iota

	// NATSJetStreamDeliveryModeBroadcast 广播模式 - 所有 Group 都收到消息
	// 每个 Group 创建独立的 Consumer，同 Group 内负载均衡
	NATSJetStreamDeliveryModeBroadcast

	// NATSJetStreamDeliveryModeLimits 限制模式 - 支持历史消息回溯
	// 有 Group 则组内负载均衡，无 Group 则每个实例独立消费
	NATSJetStreamDeliveryModeLimits
)

func (m NATSJetStreamDeliveryMode) String() string {
	switch m {
	case NATSJetStreamDeliveryModeWorkQueue:
		return "workqueue"
	case NATSJetStreamDeliveryModeBroadcast:
		return "broadcast"
	case NATSJetStreamDeliveryModeLimits:
		return "limits"
	default:
		return "unknown"
	}
}

// 上下文键
type natsJetStreamDeliveryModeKey struct{}

// WithNATSJetStreamDeliveryMode 设置 NATS JetStream 消息投递模式到上下文
func WithNATSJetStreamDeliveryMode(ctx context.Context, mode NATSJetStreamDeliveryMode) context.Context {
	return context.WithValue(ctx, natsJetStreamDeliveryModeKey{}, mode)
}

// FromNATSJetStreamDeliveryModeContext 从上下文获取 NATS JetStream 消息投递模式
func FromNATSJetStreamDeliveryModeContext(ctx context.Context) (NATSJetStreamDeliveryMode, bool) {
	mode, ok := ctx.Value(natsJetStreamDeliveryModeKey{}).(NATSJetStreamDeliveryMode)
	return mode, ok
}
