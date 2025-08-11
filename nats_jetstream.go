package eventbus

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

var (
	// DefaultNATSJetStreamOptions 默认NATS JetStream可选项
	DefaultNATSJetStreamOptions = NATSJetStreamOptions{
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
		StreamName:        "EVENTBUS",
		MaxMsgs:           1000000,
		MaxAge:            24 * time.Hour,
		DuplicateWindow:   2 * time.Minute,
		Replicas:          1,
		AckWait:           30 * time.Second,
		MaxDeliver:        3,
	}
)

// NATSJetStreamOptions NATS JetStream可选项
type NATSJetStreamOptions struct {
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

	// JetStream 特有配置
	StreamName      string        // 流名称
	MaxMsgs         int64         // 流中最大消息数
	MaxAge          time.Duration // 消息最大保存时间
	DuplicateWindow time.Duration // 重复消息检测窗口
	Replicas        int           // 副本数
	AckWait         time.Duration // 消息确认等待时间
	MaxDeliver      int           // 最大投递次数
}

type natsJetStreamEventBus struct {
	options *NATSJetStreamOptions
	conn    *nats.Conn
	js      nats.JetStreamContext
}

// NewNATSJetStream 创建NATS JetStream事件总线
func NewNATSJetStream(conn *nats.Conn, options ...*NATSJetStreamOptions) (bus EventBus, err error) {
	if conn == nil {
		return nil, fmt.Errorf("nats connection is nil")
	}

	var opts *NATSJetStreamOptions
	if len(options) > 0 && options[0] != nil {
		opts = options[0]
	} else {
		opts = &DefaultNATSJetStreamOptions
	}

	// 创建JetStream上下文
	js, err := conn.JetStream()
	if err != nil {
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	natsJSEventBus := &natsJetStreamEventBus{
		options: opts,
		conn:    conn,
		js:      js,
	}

	// 确保流存在
	err = natsJSEventBus.ensureStream()
	if err != nil {
		return nil, fmt.Errorf("failed to ensure stream: %w", err)
	}

	return natsJSEventBus, nil
}

// ensureStream 确保JetStream流存在
func (n *natsJetStreamEventBus) ensureStream() error {
	streamConfig := &nats.StreamConfig{
		Name:       n.options.StreamName,
		Subjects:   []string{fmt.Sprintf("%s.>", n.options.StreamName)},
		MaxMsgs:    n.options.MaxMsgs,
		MaxAge:     n.options.MaxAge,
		Duplicates: n.options.DuplicateWindow,
		Replicas:   n.options.Replicas,
		Storage:    nats.FileStorage,
		Retention:  nats.WorkQueuePolicy,
		Discard:    nats.DiscardOld,
	}

	_, err := n.js.StreamInfo(n.options.StreamName)
	if err != nil {
		// 流不存在，创建它
		_, err = n.js.AddStream(streamConfig)
		if err != nil {
			return fmt.Errorf("failed to create stream %s: %w", n.options.StreamName, err)
		}
		n.options.Logger.Infof(context.Background(), "Created JetStream stream: %s", n.options.StreamName)
	} else {
		// 流已存在，更新配置
		_, err = n.js.UpdateStream(streamConfig)
		if err != nil {
			n.options.Logger.Warnf(context.Background(), "Failed to update stream %s: %v", n.options.StreamName, err)
		}
	}

	return nil
}

// getJetStreamSubject 获取JetStream主题名
func (n *natsJetStreamEventBus) getJetStreamSubject(topic string) string {
	return fmt.Sprintf("%s.%s", n.options.StreamName, topic)
}

// Publish 发布消息
func (n *natsJetStreamEventBus) Publish(ctx context.Context, topic string, v interface{}) error {
	return n.publishWithRetry(ctx, topic, v, false)
}

// PublishAsync 异步发布消息
func (n *natsJetStreamEventBus) PublishAsync(ctx context.Context, topic string, v interface{}) error {
	go func() {
		if err := n.publishWithRetry(ctx, topic, v, true); err != nil {
			n.options.Logger.Errorf(ctx, "Failed to publish async message to topic %s: %v", topic, err)
		}
	}()
	return nil
}

// publishWithRetry 带重试的发布消息
func (n *natsJetStreamEventBus) publishWithRetry(ctx context.Context, topic string, v interface{}, isAsync bool) error {
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

	jsSubject := n.getJetStreamSubject(topic)

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

		// 设置发布选项
		opts := []nats.PubOpt{}
		if ctx.Err() != nil {
			opts = append(opts, nats.Context(ctx))
		}

		// 可以添加消息ID用于去重
		if msgID, ok := msg.Header["message-id"]; ok {
			opts = append(opts, nats.MsgId(msgID))
		}

		_, err = n.js.Publish(jsSubject, data, opts...)
		if err == nil {
			if isAsync {
				n.options.Logger.Debugf(ctx, "Published async message to JetStream subject %s (attempt %d)", jsSubject, attempt+1)
			} else {
				n.options.Logger.Debugf(ctx, "Published message to JetStream subject %s (attempt %d)", jsSubject, attempt+1)
			}
			return nil
		}

		if isAsync {
			n.options.Logger.Debugf(ctx, "Failed to publish async message to JetStream subject %s (attempt %d/%d): %v",
				jsSubject, attempt+1, n.options.MaxRetries+1, err)
		} else {
			n.options.Logger.Warnf(ctx, "Failed to publish message to JetStream subject %s (attempt %d/%d): %v",
				jsSubject, attempt+1, n.options.MaxRetries+1, err)
		}
	}

	// 如果重试后仍然失败，发送到死信队列
	if n.options.DeadLetterSubject != "" {
		n.sendToDeadLetter(ctx, topic, msg, err.Error())
	}

	return fmt.Errorf("failed to publish message to JetStream subject %s after %d attempts: %w",
		jsSubject, n.options.MaxRetries+1, err)
}

// sendToDeadLetter 发送消息到死信队列
func (n *natsJetStreamEventBus) sendToDeadLetter(ctx context.Context, originalTopic string, msg *Message, errorReason string) {
	if n.options.DeadLetterSubject == "" {
		return
	}

	dlqMsg := CreateDeadLetterMessage(originalTopic, msg, errorReason, n.options.DeadLetterSubject, "nats-jetstream")

	data, err := n.options.Serialize.Marshal(dlqMsg)
	if err != nil {
		n.options.Logger.Errorf(ctx, "Failed to marshal dead letter message: %v", err)
		return
	}

	dlqSubject := n.getJetStreamSubject(n.options.DeadLetterSubject)
	if _, err := n.js.Publish(dlqSubject, data); err != nil {
		n.options.Logger.Errorf(ctx, "Failed to publish to dead letter subject %s: %v",
			dlqSubject, err)
	} else {
		n.options.Logger.Infof(ctx, "Published failed message to dead letter subject %s",
			dlqSubject)
	}
}

// Subscribe 订阅消息（同步）
func (n *natsJetStreamEventBus) Subscribe(ctx context.Context, topic string, h SubscribeHandler) error {
	return n.subscribe(ctx, topic, h, false)
}

// SubscribeAsync 订阅消息（异步）
func (n *natsJetStreamEventBus) SubscribeAsync(ctx context.Context, topic string, h SubscribeHandler) error {
	return n.subscribe(ctx, topic, h, true)
}

// subscribe 内部订阅实现
func (n *natsJetStreamEventBus) subscribe(ctx context.Context, topic string, h SubscribeHandler, isAsync bool) error {
	jsSubject := n.getJetStreamSubject(topic)

	// 包装处理函数
	handler := func(msg *nats.Msg) {
		n.processMessage(ctx, msg, h, topic)
	}

	// 创建消费者配置
	var sub *nats.Subscription
	var err error

	// 检查是否有队列组设置
	groupID, hasGroup := FromGroupIDContext(ctx)
	effectiveGroup := n.getEffectiveQueueGroup(ctx)

	if hasGroup && groupID != "" || n.options.QueueGroup != "" {
		// 使用JetStream Pull订阅实现队列组功能
		consumerName := effectiveGroup
		if consumerName == "" {
			consumerName = "default-consumer"
		}

		// 创建或获取消费者
		consumerConfig := &nats.ConsumerConfig{
			Durable:       consumerName,
			AckPolicy:     nats.AckExplicitPolicy,
			AckWait:       n.options.AckWait,
			MaxDeliver:    n.options.MaxDeliver,
			FilterSubject: jsSubject,
			DeliverGroup:  effectiveGroup,
		}

		// 确保消费者存在
		_, err = n.js.AddConsumer(n.options.StreamName, consumerConfig)
		if err != nil {
			// 如果消费者已存在，尝试获取现有的
			_, err = n.js.ConsumerInfo(n.options.StreamName, consumerName)
			if err != nil {
				return fmt.Errorf("failed to create or get consumer %s: %w", consumerName, err)
			}
		}

		// 创建Pull订阅
		sub, err = n.js.PullSubscribe(jsSubject, consumerName)
		if err != nil {
			return fmt.Errorf("failed to create pull subscription for topic %s: %w", topic, err)
		}

		// 启动消息拉取循环
		go n.pullMessages(ctx, sub, handler)
	} else {
		// 使用普通的Push订阅
		sub, err = n.js.Subscribe(jsSubject, handler, nats.AckExplicit())
		if err != nil {
			return fmt.Errorf("failed to subscribe to JetStream subject %s: %w", jsSubject, err)
		}
	}

	n.options.Logger.Infof(ctx, "Subscribed to JetStream subject %s (async: %v, group: %s)",
		jsSubject, isAsync, effectiveGroup)

	// 监听上下文取消，自动取消订阅
	if !isAsync {
		go func() {
			<-ctx.Done()
			if err := sub.Unsubscribe(); err != nil {
				n.options.Logger.Errorf(ctx, "Failed to unsubscribe from JetStream subject %s: %v", jsSubject, err)
			}
		}()
	}

	return nil
}

// pullMessages Pull订阅的消息拉取循环
func (n *natsJetStreamEventBus) pullMessages(ctx context.Context, sub *nats.Subscription, handler nats.MsgHandler) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// 拉取消息，批量处理
			msgs, err := sub.Fetch(10, nats.MaxWait(time.Second))
			if err != nil {
				if err == nats.ErrTimeout {
					continue // 超时是正常的，继续拉取
				}
				n.options.Logger.Errorf(ctx, "Failed to fetch messages: %v", err)
				time.Sleep(time.Second)
				continue
			}

			for _, msg := range msgs {
				handler(msg)
			}
		}
	}
}

// processMessage 处理接收到的消息
func (n *natsJetStreamEventBus) processMessage(ctx context.Context, natsMsg *nats.Msg, h SubscribeHandler, topic string) {
	var msg Message
	if err := n.options.Serialize.Unmarshal(natsMsg.Data, &msg); err != nil {
		n.options.Logger.Errorf(ctx, "Failed to unmarshal message from topic %s: %v", topic, err)
		if n.options.SkipBadMessages {
			natsMsg.Ack() // 确认消息以避免重复投递
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
		natsMsg.Nak()
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
				natsMsg.Nak()
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
			natsMsg.Ack() // 确认消息处理成功
			return
		}

		lastErr = err
		n.options.Logger.Warnf(ctx, "Failed to process message from topic %s (attempt %d/%d): %v",
			topic, attempt+1, n.options.MessageMaxRetries+1, err)
	}

	// 如果处理失败，发送到死信队列并NAK消息
	if lastErr != nil {
		n.sendToDeadLetter(ctx, originalTopic, &msg, lastErr.Error())
	}

	// NAK消息，JetStream会根据配置重新投递
	natsMsg.Nak()

	n.options.Logger.Errorf(ctx, "Message processing failed after %d attempts, NAK'd for redelivery",
		n.options.MessageMaxRetries+1)
}

// getEffectiveQueueGroup 获取有效的队列组名
func (n *natsJetStreamEventBus) getEffectiveQueueGroup(ctx context.Context) string {
	if groupID, hasGroup := FromGroupIDContext(ctx); hasGroup && groupID != "" {
		return groupID
	}
	return n.options.QueueGroup
}

// Close 关闭NATS连接
// 注意：各个订阅应该通过context取消来自动清理，此方法主要关闭连接
func (n *natsJetStreamEventBus) Close() error {
	// 关闭连接，这会自动取消所有活跃的订阅
	if n.conn != nil && !n.conn.IsClosed() {
		n.conn.Close()
	}

	return nil
}
