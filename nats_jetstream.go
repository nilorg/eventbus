package eventbus

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

// nats.go 官方默认常量（未导出的常量需要在此定义）
const (
	// defaultAsyncPubAckInflight 是 nats.go 官方的异步发布最大待处理数默认值
	// 参考: https://github.com/nats-io/nats.go/blob/main/js.go
	defaultAsyncPubAckInflight = 4000

	// defaultAPITimeout 是 nats.go 官方的 JetStream API 默认超时时间
	// 参考: https://github.com/nats-io/nats.go/blob/main/jetstream/jetstream.go
	defaultAPITimeout = 5 * time.Second
)

var (
	// DefaultNATSJetStreamOptions 默认NATS JetStream可选项
	DefaultNATSJetStreamOptions = NATSJetStreamOptions{
		Serialize:              &JSONSerialize{},
		Logger:                 &StdLogger{},
		MaxRetries:             nats.DefaultPubRetryAttempts, // 官方默认值: 2
		RetryInterval:          nats.DefaultPubRetryWait,     // 官方默认值: 250ms
		BackoffMultiplier:      2.0,
		MaxBackoff:             time.Minute,
		MessageMaxRetries:      nats.DefaultPubRetryAttempts, // 官方默认值: 2
		SkipBadMessages:        true,
		DeadLetterSubject:      "",
		QueueGroup:             "",
		StreamName:             "EVENTBUS",
		MaxMsgs:                1000000,
		MaxAge:                 24 * time.Hour,
		DuplicateWindow:        2 * time.Minute,
		Replicas:               1,
		AckWait:                defaultAPITimeout,                  // 官方默认值: 5s
		MaxDeliver:             -1,                                 // 官方服务器默认值: -1 (无限)
		MaxWaiting:             512,                                // NATS JetStream 服务器默认值
		PublishAsyncMaxPending: defaultAsyncPubAckInflight,         // 官方默认值: 4000
		DefaultDeliveryMode:    NATSJetStreamDeliveryModeWorkQueue, // 默认使用工作队列模式
		InactiveThreshold:      24 * time.Hour,                     // 消费者不活跃自动删除时间
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
	StreamName             string                    // 流名称前缀（会根据模式自动添加后缀）
	MaxMsgs                int64                     // 流中最大消息数
	MaxAge                 time.Duration             // 消息最大保存时间
	DuplicateWindow        time.Duration             // 重复消息检测窗口
	Replicas               int                       // 副本数
	AckWait                time.Duration             // 消息确认等待时间
	MaxDeliver             int                       // 最大投递次数
	MaxWaiting             int                       // Pull消费者最大等待请求数
	PublishAsyncMaxPending int                       // 异步发布最大待处理数
	DefaultDeliveryMode    NATSJetStreamDeliveryMode // 默认投递模式
	InactiveThreshold      time.Duration             // 消费者不活跃自动删除时间（仅Broadcast模式）
}

type natsJetStreamEventBus struct {
	options *NATSJetStreamOptions
	conn    *nats.Conn
	js      nats.JetStreamContext
	streams map[NATSJetStreamDeliveryMode]string // 模式对应的Stream名称
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

	// 参数校验和默认值填充
	if opts.PublishAsyncMaxPending < 1 {
		opts.PublishAsyncMaxPending = defaultAsyncPubAckInflight
	}
	if opts.AckWait <= 0 {
		opts.AckWait = defaultAPITimeout
	}
	if opts.MaxWaiting < 1 {
		opts.MaxWaiting = 512
	}

	// 创建JetStream上下文
	jsOpts := []nats.JSOpt{
		nats.MaxWait(opts.AckWait),                               // 设置API请求的默认超时时间
		nats.PublishAsyncMaxPending(opts.PublishAsyncMaxPending), // 设置异步发布最大待处理数
	}
	js, err := conn.JetStream(jsOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	natsJSEventBus := &natsJetStreamEventBus{
		options: opts,
		conn:    conn,
		js:      js,
		streams: make(map[NATSJetStreamDeliveryMode]string),
	}

	// 确保流存在
	err = natsJSEventBus.ensureStreams()
	if err != nil {
		return nil, fmt.Errorf("failed to ensure streams: %w", err)
	}

	return natsJSEventBus, nil
}

// getStreamName 获取指定模式的Stream名称
func (n *natsJetStreamEventBus) getStreamName(mode NATSJetStreamDeliveryMode) string {
	switch mode {
	case NATSJetStreamDeliveryModeBroadcast:
		return n.options.StreamName + "_BROADCAST"
	case NATSJetStreamDeliveryModeLimits:
		return n.options.StreamName + "_LIMITS"
	default:
		return n.options.StreamName
	}
}

// ensureStreams 确保所有模式的JetStream流存在
func (n *natsJetStreamEventBus) ensureStreams() error {
	// 定义三种模式的Stream配置
	streamConfigs := []struct {
		mode      NATSJetStreamDeliveryMode
		retention nats.RetentionPolicy
	}{
		{NATSJetStreamDeliveryModeWorkQueue, nats.WorkQueuePolicy}, // 任务分发：消息只被一个消费者处理
		{NATSJetStreamDeliveryModeBroadcast, nats.InterestPolicy},  // 广播：所有订阅者都收到
		{NATSJetStreamDeliveryModeLimits, nats.LimitsPolicy},       // 历史回溯：保留消息直到达到限制
	}

	for _, sc := range streamConfigs {
		streamName := n.getStreamName(sc.mode)
		n.streams[sc.mode] = streamName

		config := &nats.StreamConfig{
			Name:       streamName,
			Subjects:   []string{fmt.Sprintf("%s.>", streamName)},
			MaxMsgs:    n.options.MaxMsgs,
			MaxAge:     n.options.MaxAge,
			Duplicates: n.options.DuplicateWindow,
			Replicas:   n.options.Replicas,
			Storage:    nats.FileStorage,
			Retention:  sc.retention,
			Discard:    nats.DiscardOld,
		}

		_, err := n.js.StreamInfo(streamName)
		if err != nil {
			// 流不存在，创建它
			_, err = n.js.AddStream(config)
			if err != nil {
				return fmt.Errorf("failed to create stream %s: %w", streamName, err)
			}
			n.options.Logger.Infof(context.Background(), "Created JetStream stream: %s (Retention: %s)", streamName, sc.retention)
		} else {
			// 流已存在，尝试更新配置
			_, err = n.js.UpdateStream(config)
			if err != nil {
				return fmt.Errorf("failed to update stream %s: %w", streamName, err)
			}
		}
	}

	return nil
}

// ensureConsumer 确保消费者存在并更新可变配置
func (n *natsJetStreamEventBus) ensureConsumer(ctx context.Context, streamName, consumerName string, config *nats.ConsumerConfig) error {
	// 先尝试获取现有消费者信息
	existingConsumer, err := n.js.ConsumerInfo(streamName, consumerName)
	if err != nil {
		// 消费者不存在，创建新的
		_, err = n.js.AddConsumer(streamName, config)
		if err != nil {
			return fmt.Errorf("failed to create consumer %s on stream %s: %w", consumerName, streamName, err)
		}
		n.options.Logger.Infof(ctx, "Created JetStream consumer: %s on stream %s (MaxWaiting: %d)", consumerName, streamName, config.MaxWaiting)
		return nil
	}

	// 消费者已存在，检查是否需要更新配置
	needsUpdate := false
	updateReasons := []string{}

	// 检查可更新的配置项
	if existingConsumer.Config.MaxWaiting != config.MaxWaiting {
		needsUpdate = true
		updateReasons = append(updateReasons, fmt.Sprintf("MaxWaiting: %d -> %d", existingConsumer.Config.MaxWaiting, config.MaxWaiting))
	}
	if existingConsumer.Config.AckWait != config.AckWait {
		needsUpdate = true
		updateReasons = append(updateReasons, fmt.Sprintf("AckWait: %v -> %v", existingConsumer.Config.AckWait, config.AckWait))
	}
	if existingConsumer.Config.MaxDeliver != config.MaxDeliver {
		needsUpdate = true
		updateReasons = append(updateReasons, fmt.Sprintf("MaxDeliver: %d -> %d", existingConsumer.Config.MaxDeliver, config.MaxDeliver))
	}

	if needsUpdate {
		// 更新消费者配置
		// 注意：某些配置在更新时不能改变，需要保留原有值
		updateConfig := &nats.ConsumerConfig{
			Durable:       config.Durable,
			AckPolicy:     existingConsumer.Config.AckPolicy, // 不可变
			AckWait:       config.AckWait,                    // 可更新
			MaxDeliver:    config.MaxDeliver,                 // 可更新
			FilterSubject: existingConsumer.Config.FilterSubject,
			DeliverGroup:  existingConsumer.Config.DeliverGroup,
			MaxWaiting:    config.MaxWaiting, // 可更新
			// 保留其他不可变配置
			DeliverSubject: existingConsumer.Config.DeliverSubject,
			DeliverPolicy:  existingConsumer.Config.DeliverPolicy,
			ReplayPolicy:   existingConsumer.Config.ReplayPolicy,
		}

		_, err = n.js.UpdateConsumer(streamName, updateConfig)
		if err != nil {
			n.options.Logger.Warnf(ctx, "Failed to update consumer %s: %v (changes: %v)", consumerName, err, updateReasons)
			// 更新失败不影响订阅，继续使用现有配置
			return nil
		}
		n.options.Logger.Infof(ctx, "Updated JetStream consumer: %s (%s)", consumerName, strings.Join(updateReasons, ", "))
	} else {
		n.options.Logger.Debugf(ctx, "JetStream consumer %s already exists with correct config (MaxWaiting: %d)", consumerName, existingConsumer.Config.MaxWaiting)
	}

	return nil
}

// getJetStreamSubject 获取JetStream主题名
func (n *natsJetStreamEventBus) getJetStreamSubject(mode NATSJetStreamDeliveryMode, topic string) string {
	streamName := n.getStreamName(mode)
	return fmt.Sprintf("%s.%s", streamName, topic)
}

// sanitizeConsumerName 生成合法的 Consumer 名称
// NATS Consumer 名称不能包含 '.', '>', '*', ' ' 等特殊字符
func sanitizeConsumerName(group, topic string) string {
	consumerName := group
	if consumerName == "" {
		consumerName = "default-consumer"
	}

	// 替换 topic 中的非法字符
	safeTopic := topic
	replacer := strings.NewReplacer(
		".", "_",
		">", "_",
		"*", "_",
		" ", "_",
	)
	safeTopic = replacer.Replace(safeTopic)

	return fmt.Sprintf("%s_%s", consumerName, safeTopic)
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

	// 从上下文获取投递模式
	mode := n.options.DefaultDeliveryMode
	if ctxMode, ok := FromNATSJetStreamDeliveryModeContext(ctx); ok {
		mode = ctxMode
	}

	jsSubject := n.getJetStreamSubject(mode, topic)

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
				n.options.Logger.Debugf(ctx, "Published async message to JetStream subject %s (mode: %s, attempt %d)", jsSubject, mode, attempt+1)
			} else {
				n.options.Logger.Debugf(ctx, "Published message to JetStream subject %s (mode: %s, attempt %d)", jsSubject, mode, attempt+1)
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

	// 死信队列使用 WorkQueue 模式
	dlqSubject := n.getJetStreamSubject(NATSJetStreamDeliveryModeWorkQueue, n.options.DeadLetterSubject)
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
// 根据 NATSJetStreamDeliveryMode 处理不同的消费模式：
// - WorkQueue: Group 被忽略，所有实例共享一个 Consumer，消息只被一个消费者处理
// - Broadcast: Group 作为实例标识，每个 Group 创建独立的 Consumer，所有订阅者都收到消息
// - Limits: Group 可选用于负载均衡，支持历史消息回溯
func (n *natsJetStreamEventBus) subscribe(ctx context.Context, topic string, h SubscribeHandler, isAsync bool) error {
	// 从上下文获取投递模式
	mode := n.options.DefaultDeliveryMode
	if ctxMode, ok := FromNATSJetStreamDeliveryModeContext(ctx); ok {
		mode = ctxMode
	}

	// 获取 Stream 名称和 Subject
	streamName := n.getStreamName(mode)
	jsSubject := n.getJetStreamSubject(mode, topic)

	// 包装处理函数
	handler := func(msg *nats.Msg) {
		n.processMessage(ctx, msg, h, topic)
	}

	var sub *nats.Subscription
	var err error
	var consumerName string

	// 根据模式确定 Consumer 名称策略
	groupID, _ := FromGroupIDContext(ctx)

	switch mode {
	case NATSJetStreamDeliveryModeWorkQueue:
		// WorkQueue 模式：忽略 Group，所有实例共享一个 Consumer
		// Consumer 名称只与 topic 相关
		consumerName = sanitizeConsumerName("workqueue", topic)
		n.options.Logger.Debugf(ctx, "WorkQueue mode: Group ignored, using shared consumer %s", consumerName)

	case NATSJetStreamDeliveryModeBroadcast:
		// Broadcast 模式：Group 作为实例标识，每个 Group 创建独立 Consumer
		// 如果没有 Group，使用默认配置的 QueueGroup 或生成唯一 ID
		effectiveGroup := groupID
		if effectiveGroup == "" {
			effectiveGroup = n.options.QueueGroup
		}
		if effectiveGroup == "" {
			// 没有 Group 时生成唯一实例 ID
			effectiveGroup = fmt.Sprintf("instance_%d_%d", time.Now().UnixNano(), rand.Int63())
		}
		consumerName = sanitizeConsumerName(effectiveGroup, topic)
		n.options.Logger.Debugf(ctx, "Broadcast mode: Using instance-specific consumer %s (group: %s)", consumerName, effectiveGroup)

	case NATSJetStreamDeliveryModeLimits:
		// Limits 模式：Group 可选用于负载均衡
		effectiveGroup := groupID
		if effectiveGroup == "" {
			effectiveGroup = n.options.QueueGroup
		}
		if effectiveGroup == "" {
			effectiveGroup = "default"
		}
		consumerName = sanitizeConsumerName(effectiveGroup, topic)
		n.options.Logger.Debugf(ctx, "Limits mode: Using group-based consumer %s (group: %s)", consumerName, effectiveGroup)

	default:
		return fmt.Errorf("unsupported delivery mode: %d", mode)
	}

	// 创建消费者配置
	consumerConfig := &nats.ConsumerConfig{
		Durable:       consumerName,
		AckPolicy:     nats.AckExplicitPolicy,
		AckWait:       n.options.AckWait,
		MaxDeliver:    n.options.MaxDeliver,
		FilterSubject: jsSubject,
		MaxWaiting:    n.options.MaxWaiting,
	}

	// Broadcast 模式特殊配置：设置不活跃清理时间
	if mode == NATSJetStreamDeliveryModeBroadcast && n.options.InactiveThreshold > 0 {
		consumerConfig.InactiveThreshold = n.options.InactiveThreshold
	}

	// 确保消费者存在并更新配置
	err = n.ensureConsumer(ctx, streamName, consumerName, consumerConfig)
	if err != nil {
		return fmt.Errorf("failed to ensure consumer %s on stream %s: %w", consumerName, streamName, err)
	}

	// 创建 Pull 订阅
	sub, err = n.js.PullSubscribe(jsSubject, consumerName, nats.Bind(streamName, consumerName))
	if err != nil {
		return fmt.Errorf("failed to create pull subscription for topic %s on stream %s: %w", topic, streamName, err)
	}

	// 启动消息拉取循环
	go n.pullMessages(ctx, sub, handler)

	n.options.Logger.Infof(ctx, "Subscribed to JetStream subject %s (mode: %s, consumer: %s, async: %v)",
		jsSubject, mode, consumerName, isAsync)

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
	backoff := time.Millisecond * 100
	maxBackoff := time.Second * 5

	for {
		select {
		case <-ctx.Done():
			return
		default:
			// 拉取消息，批量处理
			msgs, err := sub.Fetch(10, nats.MaxWait(time.Second))
			if err != nil {
				if err == nats.ErrTimeout {
					backoff = time.Millisecond * 100 // 重置退避时间
					continue                         // 超时是正常的，继续拉取
				}
				// 处理 Exceeded MaxWaiting 错误，使用指数退避
				if strings.Contains(err.Error(), "Exceeded MaxWaiting") {
					n.options.Logger.Debugf(ctx, "Exceeded MaxWaiting, backing off for %v", backoff)
					select {
					case <-ctx.Done():
						return
					case <-time.After(backoff):
						// 指数退避
						backoff = backoff * 2
						if backoff > maxBackoff {
							backoff = maxBackoff
						}
					}
					continue
				}
				n.options.Logger.Errorf(ctx, "Failed to fetch messages: %v", err)
				time.Sleep(time.Second)
				continue
			}

			backoff = time.Millisecond * 100 // 成功获取消息后重置退避时间
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
