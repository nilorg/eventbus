package eventbus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

var (
	// DefaultRedisOptions 默认Redis可选项
	DefaultRedisOptions = RedisOptions{
		ReadCount:         1,
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

// RedisOptions Redis可选项
type RedisOptions struct {
	ReadCount         int64
	ReadBlock         time.Duration
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

type redisEventBus struct {
	options *RedisOptions
	conn    *redis.Client
}

// NewRedis 创建Redis事件总线
func NewRedis(conn *redis.Client, options ...*RedisOptions) (bus EventBus, err error) {
	var ops RedisOptions
	if len(options) == 0 {
		ops = DefaultRedisOptions
	} else {
		ops = *options[0]
	}
	rbus := &redisEventBus{
		conn:    conn,
		options: &ops,
	}
	bus = rbus
	return
}

func (bus *redisEventBus) Publish(ctx context.Context, topic string, v interface{}) (err error) {
	return bus.publish(ctx, topic, v, false)
}

func (bus *redisEventBus) PublishAsync(ctx context.Context, topic string, v interface{}) (err error) {
	return bus.publish(ctx, topic, v, true)
}

func (bus *redisEventBus) publish(ctx context.Context, topic string, v interface{}, _ bool) (err error) {
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
	bus.options.Logger.Debugf(ctx, "publish msg data: %s", string(msgValue))

	err = bus.conn.XAdd(ctx, &redis.XAddArgs{
		Stream: topic,
		Values: map[string]interface{}{
			"header": string(msgHeader),
			"value":  string(msgValue),
		},
	}).Err()
	return
}

func (bus *redisEventBus) Subscribe(ctx context.Context, topic string, h SubscribeHandler) (err error) {
	return bus.subscribe(ctx, topic, h, false)
}

func (bus *redisEventBus) SubscribeAsync(ctx context.Context, topic string, h SubscribeHandler) (err error) {
	return bus.subscribe(ctx, topic, h, true)
}

func (bus *redisEventBus) subscribe(ctx context.Context, topic string, h SubscribeHandler, async bool) (err error) {

	queueName := fmt.Sprintf("%s.default.group.%s", topic, Version)
	var consumer string
	if gid, ok := FromGroupIDContext(ctx); ok {
		queueName = fmt.Sprintf("%s-%s.group.%s", topic, gid, Version)
		consumer = gid
	}

	// 一对多要生产不同的消费组，根据groupID来区分
	bus.conn.XGroupCreateMkStream(ctx, topic, queueName, "0-0")
	if async {
		go func(subCtx context.Context) {
			if asyncErr := bus.xReadGroup(subCtx, topic, queueName, consumer, h); asyncErr != nil {
				bus.options.Logger.Errorf(subCtx, "async subscribe %s error: %v", topic, asyncErr)
			}
		}(ctx)
		return
	}
	err = bus.xReadGroup(ctx, topic, queueName, consumer, h)
	return
}

func (bus *redisEventBus) xReadGroup(ctx context.Context, stream, group, consumer string, h SubscribeHandler) (err error) {
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
			count := bus.options.ReadCount
			if count < 0 {
				count = 1
			}
			var streams []redis.XStream
			streams, err = bus.conn.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    group,
				Consumer: consumer,
				Count:    count,
				Streams:  []string{stream, ">"},
				Block:    bus.options.ReadBlock,
			}).Result()
			if err != nil {
				bus.options.Logger.Errorf(ctx, "XReadGroup: %v", err)

				// 检查是否为网络错误或连接错误
				if bus.isRetryableError(err) && retryCount < bus.options.MaxRetries {
					retryCount++
					retryInterval := bus.calculateBackoff(baseRetryInterval, retryCount)
					bus.options.Logger.Warnf(ctx, "Redis connection error, retrying %d/%d after %v: %v",
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

			streamsLen := len(streams)
			for i := 0; i < streamsLen; i++ {
				s := streams[i]
				msgLen := len(s.Messages)
				for j := 0; j < msgLen; j++ {
					if err = bus.handleSubMessageWithRetry(ctx, s.Stream, group, s.Messages[j], h); err != nil {
						bus.options.Logger.Errorf(ctx, "message processing failed after retries: %v", err)
						// 根据配置决定是否继续处理其他消息
						if !bus.options.SkipBadMessages {
							return err
						}
					}
				}
			}
		}
	}
}

func (bus *redisEventBus) handleSubMessage(ctx context.Context, stream, group string, msg redis.XMessage, h SubscribeHandler) (err error) {
	msgHeader, msgHeaderOK := msg.Values["header"]
	if !msgHeaderOK {
		return
	}
	msgValue, msgValueOK := msg.Values["value"]
	if !msgValueOK {
		return
	}
	m := Message{
		Header: make(MessageHeader),
	}
	if err = bus.options.Serialize.Unmarshal([]byte(msgHeader.(string)), &m.Header); err != nil {
		return
	}
	bus.options.Logger.Debugf(ctx, "subscribe msg data: %s", msgValue)
	if err = bus.options.Serialize.Unmarshal([]byte(msgValue.(string)), &m.Value); err != nil {
		return
	}
	if err = h(ctx, &m); err == nil {
		err = bus.conn.XAck(ctx, stream, group, msg.ID).Err()
	}
	return
}

// isRetryableError 判断错误是否可以重试
func (bus *redisEventBus) isRetryableError(err error) bool {
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
		strings.Contains(errorStr, "connection lost") ||
		errors.Is(err, redis.Nil)
}

// calculateBackoff 计算退避时间
func (bus *redisEventBus) calculateBackoff(baseInterval time.Duration, retryCount int) time.Duration {
	multiplier := bus.options.BackoffMultiplier
	if multiplier <= 0 {
		multiplier = 2.0
	}

	maxBackoff := bus.options.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = time.Minute * 5
	}

	// 指数退避：baseInterval * multiplier^retryCount
	// 使用 math.Pow 计算 multiplier^retryCount
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

// handleSubMessageWithRetry 带重试的消息处理
func (bus *redisEventBus) handleSubMessageWithRetry(ctx context.Context, stream, group string, msg redis.XMessage, h SubscribeHandler) (err error) {
	maxRetries := bus.options.MessageMaxRetries
	if maxRetries <= 0 {
		maxRetries = 1 // 至少尝试一次
	}

	for attempt := 0; attempt <= maxRetries; attempt++ {
		err = bus.handleSubMessage(ctx, stream, group, msg, h)
		if err == nil {
			// 处理成功，返回
			return nil
		}

		// 记录重试信息
		if attempt < maxRetries {
			bus.options.Logger.Warnf(ctx, "message processing failed (attempt %d/%d), retrying: %v",
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
	bus.options.Logger.Errorf(ctx, "message processing failed after %d attempts: %v", maxRetries+1, err)

	// 如果配置了死信队列，将消息发送到死信队列
	if bus.options.DeadLetterTopic != "" {
		if dlErr := bus.sendToDeadLetter(ctx, stream, msg); dlErr != nil {
			bus.options.Logger.Errorf(ctx, "failed to send message to dead letter queue: %v", dlErr)
		} else {
			bus.options.Logger.Infof(ctx, "message sent to dead letter queue: %s", msg.ID)
			// 发送到死信队列成功，确认原消息
			if ackErr := bus.conn.XAck(ctx, stream, group, msg.ID).Err(); ackErr != nil {
				bus.options.Logger.Errorf(ctx, "failed to ack message after sending to DLQ: %v", ackErr)
			}
			return nil // 不返回错误，因为消息已经处理（发送到DLQ）
		}
	}

	return err
}

// sendToDeadLetter 发送消息到死信队列
func (bus *redisEventBus) sendToDeadLetter(ctx context.Context, originalTopic string, msg redis.XMessage) error {
	if bus.options.DeadLetterTopic == "" {
		return fmt.Errorf("dead letter topic not configured")
	}

	// 将Redis消息转换为EventBus消息格式
	originalMsg := &Message{
		Header: MessageHeader{
			"message_id": msg.ID,
			"topic":      originalTopic,
		},
		Value: msg.Values,
	}

	// 尝试从消息值中提取原始的header和value
	if headerData, headerOK := msg.Values["header"]; headerOK {
		if headerStr, isString := headerData.(string); isString {
			var header MessageHeader
			if err := bus.options.Serialize.Unmarshal([]byte(headerStr), &header); err == nil {
				// 成功解析header，合并到消息中
				for k, v := range header {
					originalMsg.Header[k] = v
				}
			}
		}
	}

	if valueData, valueOK := msg.Values["value"]; valueOK {
		if valueStr, isString := valueData.(string); isString {
			var value interface{}
			if err := bus.options.Serialize.Unmarshal([]byte(valueStr), &value); err == nil {
				originalMsg.Value = value
			}
		}
	}

	// 使用统一的死信消息创建函数
	dlqMsg := CreateDeadLetterMessage(originalTopic, originalMsg, "message processing failed after retries", bus.options.DeadLetterTopic, "redis")

	// 将死信消息序列化为JSON字符串，然后作为单个值存储
	dlqData, err := bus.options.Serialize.Marshal(dlqMsg)
	if err != nil {
		bus.options.Logger.Errorf(ctx, "failed to marshal dead letter message: %v", err)
		return err
	}

	// 使用统一的格式存储死信消息，并设置长度限制
	dlqValues := map[string]interface{}{
		"dead_letter_message": string(dlqData),
		"timestamp":           time.Now().Unix(),
		"source":              "redis-stream",
	}

	addArgs := &redis.XAddArgs{
		Stream: bus.options.DeadLetterTopic,
		Values: dlqValues,
	}

	// 设置死信队列最大长度
	if bus.options.DeadLetterMaxLen > 0 {
		addArgs.MaxLen = bus.options.DeadLetterMaxLen
		addArgs.Approx = true // 使用近似修剪提高性能
	}

	if err := bus.conn.XAdd(ctx, addArgs).Err(); err != nil {
		bus.options.Logger.Errorf(ctx, "failed to send message to dead letter stream: %v", err)
		return err
	}

	// 如果配置了TTL，设置过期时间
	if bus.options.DeadLetterTTL > 0 {
		if err := bus.conn.Expire(ctx, bus.options.DeadLetterTopic, bus.options.DeadLetterTTL).Err(); err != nil {
			bus.options.Logger.Warnf(ctx, "failed to set TTL for dead letter stream: %v", err)
		}
	}

	bus.options.Logger.Infof(ctx, "message sent to dead letter stream %s", bus.options.DeadLetterTopic)
	return nil
}
