package eventbus

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

var (
	// DefaultNATSOptions 默认NATS可选项
	DefaultNATSOptions = NATSOptions{
		Serialize:         &JSONSerialize{},
		Logger:            &StdLogger{},
		MaxRetries:        3,
		RetryInterval:     time.Second * 2,
		BackoffMultiplier: 2.0,
		MaxBackoff:        time.Minute,
		MessageMaxRetries: 3,
		SkipBadMessages:   true,
		DeadLetterSubject: "",
		QueueGroup:        "",
	}
)

// NATSOptions NATS可选项
type NATSOptions struct {
	Serialize         Serializer
	Logger            Logger
	MaxRetries        int           // 最大重试次数
	RetryInterval     time.Duration // 重试间隔
	BackoffMultiplier float64       // 退避倍数
	MaxBackoff        time.Duration // 最大退避时间
	MessageMaxRetries int           // 单条消息最大重试次数
	SkipBadMessages   bool          // 是否跳过无法处理的消息
	DeadLetterSubject string        // 死信队列主题
	QueueGroup        string        // 队列组名，用于负载均衡
}

type natsEventBus struct {
	options *NATSOptions
	conn    *nats.Conn
}

// NewNATS 创建NATS事件总线
func NewNATS(conn *nats.Conn, options ...*NATSOptions) (bus EventBus, err error) {
	if conn == nil {
		return nil, fmt.Errorf("nats connection is nil")
	}

	var opts *NATSOptions
	if len(options) > 0 && options[0] != nil {
		opts = options[0]
	} else {
		opts = &DefaultNATSOptions
	}

	natsEventBus := &natsEventBus{
		options: opts,
		conn:    conn,
	}

	return natsEventBus, nil
}

// Publish 发布消息
func (n *natsEventBus) Publish(ctx context.Context, topic string, v interface{}) error {
	return n.publishWithRetry(ctx, topic, v, false)
}

// PublishAsync 异步发布消息
func (n *natsEventBus) PublishAsync(ctx context.Context, topic string, v interface{}) error {
	go func() {
		if err := n.publishWithRetry(ctx, topic, v, true); err != nil {
			n.options.Logger.Errorf(ctx, "Failed to publish async message to topic %s: %v", topic, err)
		}
	}()
	return nil
}

// publishWithRetry 带重试的发布消息
func (n *natsEventBus) publishWithRetry(ctx context.Context, topic string, v interface{}, isAsync bool) error {
	var msg *Message

	// 检查是否已经是 Message 类型
	if m, ok := v.(*Message); ok {
		msg = m
	} else {
		msg = &Message{
			Header: make(MessageHeader),
			Value:  v,
		}
	}

	// 从上下文中获取消息头设置函数
	if setHeader, ok := FromSetMessageHeaderContext(ctx); ok {
		for k, v := range setHeader(ctx) {
			msg.Header[k] = v
		}
	}

	// 自动注入 topic 到消息头
	msg.Header["topic"] = topic

	// 序列化消息
	data, err := n.options.Serialize.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	// 执行重试逻辑
	retryInterval := n.options.RetryInterval
	for attempt := 0; attempt <= n.options.MaxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(retryInterval):
				// 计算下次重试间隔
				retryInterval = time.Duration(float64(retryInterval) * n.options.BackoffMultiplier)
				if retryInterval > n.options.MaxBackoff {
					retryInterval = n.options.MaxBackoff
				}
			}
		}

		err = n.conn.Publish(topic, data)
		if err == nil {
			if isAsync {
				n.options.Logger.Debugf(ctx, "Published async message to topic %s (attempt %d)", topic, attempt+1)
			} else {
				n.options.Logger.Debugf(ctx, "Published message to topic %s (attempt %d)", topic, attempt+1)
			}
			return nil
		}

		if isAsync {
			n.options.Logger.Debugf(ctx, "Failed to publish async message to topic %s (attempt %d/%d): %v",
				topic, attempt+1, n.options.MaxRetries+1, err)
		} else {
			n.options.Logger.Warnf(ctx, "Failed to publish message to topic %s (attempt %d/%d): %v",
				topic, attempt+1, n.options.MaxRetries+1, err)
		}
	}

	// 如果重试后仍然失败，发送到死信队列
	if n.options.DeadLetterSubject != "" {
		n.sendToDeadLetter(ctx, topic, msg, err.Error())
	}

	return fmt.Errorf("failed to publish message to topic %s after %d attempts: %w",
		topic, n.options.MaxRetries+1, err)
}

// sendToDeadLetter 发送消息到死信队列
func (n *natsEventBus) sendToDeadLetter(ctx context.Context, originalTopic string, msg *Message, errorReason string) {
	if n.options.DeadLetterSubject == "" {
		return
	}

	dlqMsg := CreateDeadLetterMessage(originalTopic, msg, errorReason, n.options.DeadLetterSubject, "nats")

	data, err := n.options.Serialize.Marshal(dlqMsg)
	if err != nil {
		n.options.Logger.Errorf(ctx, "Failed to marshal dead letter message: %v", err)
		return
	}

	if err := n.conn.Publish(n.options.DeadLetterSubject, data); err != nil {
		n.options.Logger.Errorf(ctx, "Failed to publish to dead letter subject %s: %v",
			n.options.DeadLetterSubject, err)
	} else {
		n.options.Logger.Infof(ctx, "Published failed message to dead letter subject %s",
			n.options.DeadLetterSubject)
	}
}

// Subscribe 订阅消息（同步）
func (n *natsEventBus) Subscribe(ctx context.Context, topic string, h SubscribeHandler) error {
	return n.subscribe(ctx, topic, h, false)
}

// SubscribeAsync 订阅消息（异步）
func (n *natsEventBus) SubscribeAsync(ctx context.Context, topic string, h SubscribeHandler) error {
	return n.subscribe(ctx, topic, h, true)
}

// subscribe 内部订阅实现
func (n *natsEventBus) subscribe(ctx context.Context, topic string, h SubscribeHandler, isAsync bool) error {
	// 包装处理函数
	handler := func(msg *nats.Msg) {
		n.processMessage(ctx, msg, h, topic)
	}

	var sub *nats.Subscription
	var err error

	// 检查是否有队列组设置
	groupID, hasGroup := FromGroupIDContext(ctx)
	if hasGroup && groupID != "" {
		// 使用队列组订阅，实现负载均衡
		sub, err = n.conn.QueueSubscribe(topic, groupID, handler)
	} else if n.options.QueueGroup != "" {
		// 使用默认队列组
		sub, err = n.conn.QueueSubscribe(topic, n.options.QueueGroup, handler)
	} else {
		// 普通订阅
		sub, err = n.conn.Subscribe(topic, handler)
	}

	if err != nil {
		return fmt.Errorf("failed to subscribe to topic %s: %w", topic, err)
	}

	n.options.Logger.Infof(ctx, "Subscribed to topic %s (async: %v, group: %s)",
		topic, isAsync, n.getEffectiveQueueGroup(ctx))

	// 监听上下文取消，自动取消订阅
	if !isAsync {
		go func() {
			<-ctx.Done()
			if err := sub.Unsubscribe(); err != nil {
				n.options.Logger.Errorf(ctx, "Failed to unsubscribe from topic %s: %v", topic, err)
			}
		}()
	}

	return nil
}

// processMessage 处理接收到的消息
func (n *natsEventBus) processMessage(ctx context.Context, natsMsg *nats.Msg, h SubscribeHandler, topic string) {
	var msg Message
	if err := n.options.Serialize.Unmarshal(natsMsg.Data, &msg); err != nil {
		n.options.Logger.Errorf(ctx, "Failed to unmarshal message from topic %s: %v", topic, err)
		if n.options.SkipBadMessages {
			return
		}
		// 如果不跳过坏消息，创建一个基本消息结构发送到死信队列
		badMsg := &Message{
			Header: MessageHeader{
				"topic": topic,
				"error": "unmarshal_failed",
			},
			Value: string(natsMsg.Data),
		}
		n.sendToDeadLetter(ctx, topic, badMsg, err.Error())
		return
	}

	// 获取原始topic（如果消息头中有的话）
	originalTopic := topic
	if topicFromHeader, exists := msg.Header["topic"]; exists {
		originalTopic = topicFromHeader
	}

	// 执行消息处理
	retryInterval := n.options.RetryInterval
	var lastErr error
	for attempt := 0; attempt <= n.options.MessageMaxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(retryInterval):
				// 计算下次重试间隔
				retryInterval = time.Duration(float64(retryInterval) * n.options.BackoffMultiplier)
				if retryInterval > n.options.MaxBackoff {
					retryInterval = n.options.MaxBackoff
				}
			}
		}

		err := h(ctx, &msg)
		if err == nil {
			n.options.Logger.Debugf(ctx, "Successfully processed message from topic %s (attempt %d)",
				topic, attempt+1)
			return
		}

		lastErr = err
		n.options.Logger.Warnf(ctx, "Failed to process message from topic %s (attempt %d/%d): %v",
			topic, attempt+1, n.options.MessageMaxRetries+1, err)
	}

	// 如果处理失败，发送到死信队列
	n.sendToDeadLetter(ctx, originalTopic, &msg, lastErr.Error())
}

// getEffectiveQueueGroup 获取有效的队列组名
func (n *natsEventBus) getEffectiveQueueGroup(ctx context.Context) string {
	if groupID, hasGroup := FromGroupIDContext(ctx); hasGroup && groupID != "" {
		return groupID
	}
	return n.options.QueueGroup
}

// Close 关闭NATS连接
// 注意：各个订阅应该通过context取消来自动清理，此方法主要关闭连接
func (n *natsEventBus) Close() error {
	// 关闭连接，这会自动取消所有活跃的订阅
	if n.conn != nil && !n.conn.IsClosed() {
		n.conn.Close()
	}

	return nil
}
