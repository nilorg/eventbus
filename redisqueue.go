package eventbus

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

var (
	// DefaultRedisQueueOptions 默认Redis Queue可选项
	DefaultRedisQueueOptions = RedisQueueOptions{
		PollInterval:      time.Second,
		Serialize:         &JSONSerialize{},
		Logger:            &StdLogger{},
		MaxRetries:        3,
		RetryInterval:     time.Second * 5,
		BackoffMultiplier: 2.0,
		MaxBackoff:        time.Minute * 5,
		MessageMaxRetries: 3,                  // 单条消息默认重试3次
		SkipBadMessages:   true,               // 默认跳过无法处理的消息
		DeadLetterTopic:   "",                 // 默认不使用死信队列
		DeadLetterMaxLen:  1000,               // 死信队列最大长度
		DeadLetterTTL:     time.Hour * 24 * 7, // 死信消息保留7天
	}
)

// RedisQueueOptions Redis Queue可选项
type RedisQueueOptions struct {
	PollInterval      time.Duration // 轮询间隔
	Serialize         Serializer
	Logger            Logger
	MaxRetries        int           // 最大重试次数
	RetryInterval     time.Duration // 重试间隔
	BackoffMultiplier float64       // 退避倍数
	MaxBackoff        time.Duration // 最大退避时间
	MessageMaxRetries int           // 单条消息最大重试次数
	SkipBadMessages   bool          // 是否跳过无法处理的消息
	DeadLetterTopic   string        // 死信队列主题
	DeadLetterMaxLen  int64         // 死信队列最大长度
	DeadLetterTTL     time.Duration // 死信消息TTL
}

// QueueMessage Redis队列消息结构
type QueueMessage struct {
	ID      string        `json:"id"`
	Topic   string        `json:"topic"`
	Header  MessageHeader `json:"header"`
	Value   interface{}   `json:"value"`
	GroupID string        `json:"group_id,omitempty"`
}

type redisQueueEventBus struct {
	options *RedisQueueOptions
	conn    *redis.Client
}

// NewRedisQueue 创建Redis Queue事件总线
func NewRedisQueue(conn *redis.Client, options ...*RedisQueueOptions) (bus EventBus, err error) {
	var ops RedisQueueOptions
	if len(options) == 0 {
		ops = DefaultRedisQueueOptions
	} else {
		ops = *options[0]
	}
	rqbus := &redisQueueEventBus{
		conn:    conn,
		options: &ops,
	}
	bus = rqbus
	return
}

func (bus *redisQueueEventBus) Publish(ctx context.Context, topic string, v interface{}) (err error) {
	return bus.publish(ctx, topic, v, false)
}

func (bus *redisQueueEventBus) PublishAsync(ctx context.Context, topic string, v interface{}) (err error) {
	return bus.publish(ctx, topic, v, true)
}

func (bus *redisQueueEventBus) publish(ctx context.Context, topic string, v interface{}, async bool) (err error) {
	var (
		msg   *Message
		msgOK bool
	)
	if v1, v1Ok := v.(*Message); v1Ok {
		msg = v1
		msgOK = true
	} else if v2, v2Ok := v.(Message); v2Ok {
		msg = &v2
		msgOK = true
	}
	if !msgOK {
		msg = &Message{
			Header: make(MessageHeader),
			Value:  v,
		}
	}

	// set message header
	if f, ok := FromSetMessageHeaderContext(ctx); ok {
		if headers := f(ctx); headers != nil {
			for key, value := range headers {
				msg.Header[key] = value
			}
		}
	}
	// 自动添加 topic 到消息头
	msg.Header["topic"] = topic

	var msgHeader []byte
	msgHeader, err = bus.options.Serialize.Marshal(msg.Header)
	if err != nil {
		return
	}
	var msgValue []byte
	msgValue, err = bus.options.Serialize.Marshal(msg.Value)
	if err != nil {
		return
	}

	bus.options.Logger.Debugf(ctx, "publish queue msg data: %s", string(msgValue))

	// 构建队列消息，使用与redis.go相同的格式
	queueMsg := map[string]interface{}{
		"header": string(msgHeader),
		"value":  string(msgValue),
	}
	var queueMsgData []byte
	queueMsgData, err = bus.options.Serialize.Marshal(queueMsg)
	if err != nil {
		return
	}

	push := func(targetQueue string) error {
		if err := bus.conn.LPush(ctx, targetQueue, string(queueMsgData)).Err(); err != nil {
			bus.options.Logger.Errorf(ctx, "publish to queue %s error: %v", targetQueue, err)
			return err
		}
		return nil
	}

	if async {
		go func(queue string) {
			if asyncErr := push(queue); asyncErr != nil {
				bus.options.Logger.Errorf(ctx, "async publish error: %v", asyncErr)
			}
		}(topic)
		return
	}

	err = push(topic)
	return
}

func (bus *redisQueueEventBus) Subscribe(ctx context.Context, topic string, h SubscribeHandler) (err error) {
	return bus.subscribe(ctx, topic, h, false)
}

func (bus *redisQueueEventBus) SubscribeAsync(ctx context.Context, topic string, h SubscribeHandler) (err error) {
	return bus.subscribe(ctx, topic, h, true)
}

func (bus *redisQueueEventBus) subscribe(ctx context.Context, topic string, h SubscribeHandler, async bool) (err error) {
	queueName := topic

	if async {
		go func(subCtx context.Context) {
			if asyncErr := bus.consumeQueue(subCtx, queueName, h); asyncErr != nil {
				bus.options.Logger.Errorf(subCtx, "async subscribe queue %s error: %v", queueName, asyncErr)
			}
		}(ctx)
		return
	}

	err = bus.consumeQueue(ctx, queueName, h)
	return
}

func (bus *redisQueueEventBus) consumeQueue(ctx context.Context, queueName string, h SubscribeHandler) (err error) {
	retryCount := 0
	baseRetryInterval := bus.options.RetryInterval
	if baseRetryInterval <= 0 {
		baseRetryInterval = time.Second * 5
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// 使用 BRPOP 阻塞式从队列右侧弹出消息
			var result []string
			result, err = bus.conn.BRPop(ctx, bus.options.PollInterval, queueName).Result()
			if err != nil {
				if err == redis.Nil {
					// 没有消息，继续轮询
					continue
				}

				bus.options.Logger.Errorf(ctx, "BRPop from queue %s error: %v", queueName, err)

				// 检查是否为网络错误或连接错误
				if bus.isRetryableError(err) && retryCount < bus.options.MaxRetries {
					retryCount++
					retryInterval := bus.calculateBackoff(baseRetryInterval, retryCount)
					bus.options.Logger.Warnf(ctx, "Redis queue connection error, retrying %d/%d after %v: %v",
						retryCount, bus.options.MaxRetries, retryInterval, err)

					// 使用 context 来控制重试等待时间
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(retryInterval):
						continue
					}
				}

				// 如果不是可重试的错误，或者重试次数已达上限，直接返回
				return err
			}

			// 成功读取数据，重置重试计数
			retryCount = 0

			if len(result) != 2 {
				continue
			}

			msgData := result[1]
			if err = bus.handleQueueMessageWithRetry(ctx, queueName, msgData, h); err != nil {
				bus.options.Logger.Errorf(ctx, "message processing failed after retries: %v", err)
				// 根据配置决定是否继续处理其他消息
				if !bus.options.SkipBadMessages {
					return err
				}
			}
		}
	}
}

func (bus *redisQueueEventBus) handleQueueMessage(ctx context.Context, msgData string, h SubscribeHandler) (err error) {
	// 解析队列消息，格式与redis.go保持一致
	var queueMsg map[string]interface{}
	if err = bus.options.Serialize.Unmarshal([]byte(msgData), &queueMsg); err != nil {
		return
	}

	msgHeader, msgHeaderOK := queueMsg["header"]
	if !msgHeaderOK {
		return fmt.Errorf("message header not found")
	}
	msgValue, msgValueOK := queueMsg["value"]
	if !msgValueOK {
		return fmt.Errorf("message value not found")
	}

	m := Message{
		Header: make(MessageHeader),
	}
	if err = bus.options.Serialize.Unmarshal([]byte(msgHeader.(string)), &m.Header); err != nil {
		return
	}

	bus.options.Logger.Debugf(ctx, "subscribe queue msg data: %s", msgValue)
	if err = bus.options.Serialize.Unmarshal([]byte(msgValue.(string)), &m.Value); err != nil {
		return
	}

	// 调用处理器
	err = h(ctx, &m)
	return
}

// RedistributeMessages 重新分发消息（用于消费组负载均衡）
func (bus *redisQueueEventBus) RedistributeMessages(ctx context.Context, fromTopic, toTopic string, count int64) (err error) {
	for i := int64(0); i < count; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// 从源队列移动消息到目标队列
			var result string
			result, err = bus.conn.RPopLPush(ctx, fromTopic, toTopic).Result()
			if err != nil {
				if err == redis.Nil {
					// 没有更多消息
					return nil
				}
				return
			}
			bus.options.Logger.Debugf(ctx, "redistributed message from %s to %s: %s", fromTopic, toTopic, result)
		}
	}
	return
}

// GetQueueLength 获取队列长度
func (bus *redisQueueEventBus) GetQueueLength(ctx context.Context, queueName string) (length int64, err error) {
	return bus.conn.LLen(ctx, queueName).Result()
}

// PeekQueue 查看队列中的消息而不移除
func (bus *redisQueueEventBus) PeekQueue(ctx context.Context, queueName string, start, stop int64) (messages []string, err error) {
	return bus.conn.LRange(ctx, queueName, start, stop).Result()
}

// isRetryableError 判断错误是否可以重试
func (bus *redisQueueEventBus) isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	errorStr := strings.ToLower(err.Error())
	// 检查常见的网络错误和连接错误
	return strings.Contains(errorStr, "timeout") ||
		strings.Contains(errorStr, "connection refused") ||
		strings.Contains(errorStr, "connection reset") ||
		strings.Contains(errorStr, "no route to host") ||
		strings.Contains(errorStr, "network is unreachable") ||
		strings.Contains(errorStr, "broken pipe") ||
		strings.Contains(errorStr, "connection lost")
}

// calculateBackoff 计算退避时间
func (bus *redisQueueEventBus) calculateBackoff(baseInterval time.Duration, retryCount int) time.Duration {
	multiplier := bus.options.BackoffMultiplier
	if multiplier <= 0 {
		multiplier = 2.0
	}

	maxBackoff := bus.options.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = time.Minute * 5
	}

	// 指数退避：baseInterval * multiplier^retryCount
	power := 1.0
	for i := 0; i < retryCount; i++ {
		power *= multiplier
	}

	newInterval := time.Duration(float64(baseInterval) * power)

	// 限制最大退避时间
	if newInterval > maxBackoff {
		newInterval = maxBackoff
	}

	return newInterval
}

// handleQueueMessageWithRetry 带重试的消息处理
func (bus *redisQueueEventBus) handleQueueMessageWithRetry(ctx context.Context, queueName, msgData string, h SubscribeHandler) (err error) {
	maxRetries := bus.options.MessageMaxRetries
	if maxRetries <= 0 {
		maxRetries = 1 // 至少尝试一次
	}

	for attempt := 0; attempt <= maxRetries; attempt++ {
		err = bus.handleQueueMessage(ctx, msgData, h)
		if err == nil {
			// 处理成功，返回
			return nil
		}

		// 记录重试信息
		if attempt < maxRetries {
			bus.options.Logger.Warnf(ctx, "queue message processing failed (attempt %d/%d), retrying: %v",
				attempt+1, maxRetries+1, err)

			// 短暂等待后重试
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Millisecond * 100): // 100ms 重试间隔
				continue
			}
		}
	}

	// 所有重试都失败了
	bus.options.Logger.Errorf(ctx, "queue message processing failed after %d attempts: %v", maxRetries+1, err)

	// 如果配置了死信队列，将消息发送到死信队列
	if bus.options.DeadLetterTopic != "" {
		if dlErr := bus.sendToDeadLetterQueue(ctx, queueName, msgData); dlErr != nil {
			bus.options.Logger.Errorf(ctx, "failed to send message to dead letter queue: %v", dlErr)
		} else {
			bus.options.Logger.Infof(ctx, "message sent to dead letter queue")
			return nil // 不返回错误，因为消息已经处理（发送到DLQ）
		}
	}

	return err
}

// sendToDeadLetterQueue 发送消息到死信队列
func (bus *redisQueueEventBus) sendToDeadLetterQueue(ctx context.Context, originalQueue, msgData string) error {
	if bus.options.DeadLetterTopic == "" {
		return fmt.Errorf("dead letter topic not configured")
	}

	// 解析原始消息以获取更多信息
	var queueMsg map[string]interface{}
	if err := bus.options.Serialize.Unmarshal([]byte(msgData), &queueMsg); err != nil {
		bus.options.Logger.Errorf(ctx, "failed to unmarshal message for dead letter queue: %v", err)
		// 如果无法解析原始消息，创建简单的死信消息
		queueMsg = map[string]interface{}{
			"raw_data": msgData,
		}
	}

	// 尝试解析消息内容
	var originalMsg *Message
	if header, headerOK := queueMsg["header"]; headerOK {
		if value, valueOK := queueMsg["value"]; valueOK {
			originalMsg = &Message{
				Header: make(MessageHeader),
			}

			// 解析header
			if err := bus.options.Serialize.Unmarshal([]byte(header.(string)), &originalMsg.Header); err != nil {
				bus.options.Logger.Warnf(ctx, "failed to unmarshal header for dead letter: %v", err)
				originalMsg.Header = MessageHeader{"topic": originalQueue}
			}

			// 解析value
			if err := bus.options.Serialize.Unmarshal([]byte(value.(string)), &originalMsg.Value); err != nil {
				bus.options.Logger.Warnf(ctx, "failed to unmarshal value for dead letter: %v", err)
				originalMsg.Value = value
			}
		}
	}

	// 如果无法解析为Message格式，创建简单的消息
	if originalMsg == nil {
		originalMsg = &Message{
			Header: MessageHeader{
				"topic":    originalQueue,
				"raw_data": "true",
			},
			Value: queueMsg,
		}
	}

	// 使用统一的死信消息创建函数
	dlqMsg := CreateDeadLetterMessage(originalQueue, originalMsg, "message processing failed after retries", bus.options.DeadLetterTopic, "redis-queue")

	// 序列化死信消息
	var dlqMsgData []byte
	var err error
	dlqMsgData, err = bus.options.Serialize.Marshal(dlqMsg)
	if err != nil {
		bus.options.Logger.Errorf(ctx, "failed to marshal dead letter message: %v", err)
		return err
	}

	// 发送到死信队列
	if err := bus.conn.LPush(ctx, bus.options.DeadLetterTopic, string(dlqMsgData)).Err(); err != nil {
		bus.options.Logger.Errorf(ctx, "failed to send message to dead letter queue: %v", err)
		return err
	}

	// 限制死信队列长度
	if bus.options.DeadLetterMaxLen > 0 {
		if err := bus.conn.LTrim(ctx, bus.options.DeadLetterTopic, 0, bus.options.DeadLetterMaxLen-1).Err(); err != nil {
			bus.options.Logger.Warnf(ctx, "failed to trim dead letter queue: %v", err)
		}
	}

	// 如果配置了TTL，设置过期时间
	if bus.options.DeadLetterTTL > 0 {
		if err := bus.conn.Expire(ctx, bus.options.DeadLetterTopic, bus.options.DeadLetterTTL).Err(); err != nil {
			bus.options.Logger.Warnf(ctx, "failed to set TTL for dead letter queue: %v", err)
		}
	}

	bus.options.Logger.Infof(ctx, "message sent to dead letter queue %s", bus.options.DeadLetterTopic)
	return nil
}
