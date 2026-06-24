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
		StreamTrimMaxLen:  10000,              // 主动裁剪时保留的最大长度
		StreamTrimApprox:  true,               // 使用近似裁剪以提升性能
		StreamTrimLimit:   100,                // 近似裁剪时的限制参数
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
	StreamTrimMaxLen  int64         // Stream 主动裁剪时保留的最大长度，<=0 表示不裁剪
	StreamTrimApprox  bool          // Stream 裁剪时是否使用近似模式
	StreamTrimLimit   int64         // Stream 近似裁剪时使用的 LIMIT 参数，<=0 表示不设置
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
	var msg *Message
	
	if m, ok := v.(*Message); ok {
		msg = m
	} else {
		eventBytes, marshalErr := bus.options.Serialize.Marshal(v)
		if marshalErr != nil {
			return fmt.Errorf("stage 1 marshal event failed: %w", marshalErr)
		}
		
		msg = &Message{
			Header: extractHeaders(ctx, topic),
			Value:  eventBytes,
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
	msgBytes, err := bus.options.Serialize.Marshal(msg)
	if err != nil {
		return fmt.Errorf("stage 2 marshal message failed: %w", err)
	}
	bus.options.Logger.Debugf(ctx, "publish msg data: %s", string(msgBytes))

	err = bus.conn.XAdd(ctx, &redis.XAddArgs{
		Stream: topic,
		Values: map[string]interface{}{
			"message": string(msgBytes),
		},
	}).Err()
	return
}

func (bus *redisEventBus) Subscribe(ctx context.Context, topic string, h SubscribeHandler, opts ...SubscribeOption) (err error) {
	return bus.subscribe(ctx, topic, h, false, opts...)
}

func (bus *redisEventBus) SubscribeAsync(ctx context.Context, topic string, h SubscribeHandler, opts ...SubscribeOption) (err error) {
	return bus.subscribe(ctx, topic, h, true, opts...)
}

func (bus *redisEventBus) subscribe(ctx context.Context, topic string, h SubscribeHandler, async bool, opts ...SubscribeOption) (err error) {
	options := &SubscribeOptions{}
	for _, opt := range opts {
		opt(options)
	}
	queueName := fmt.Sprintf("%s.default.group.%s", topic, Version)
	var consumer string
	if gid, ok := FromGroupIDContext(ctx); ok {
		queueName = fmt.Sprintf("%s-%s.group.%s", topic, gid, Version)
		consumer = gid
	}

	// 一对多要生产不同的消费组，根据groupID来区分
	bus.conn.XGroupCreateMkStream(ctx, topic, queueName, "$")
	if async {
		go func(subCtx context.Context) {
			if asyncErr := bus.xReadGroup(subCtx, topic, queueName, consumer, h, options); asyncErr != nil {
				bus.options.Logger.Errorf(subCtx, "async subscribe %s error: %v", topic, asyncErr)
			}
		}(ctx)
		return
	}
	err = bus.xReadGroup(ctx, topic, queueName, consumer, h, options)
	return
}

func (bus *redisEventBus) CleanupStream(ctx context.Context, stream string) error {
	if bus == nil || bus.conn == nil {
		return errors.New("redisEventBus: redis client is nil")
	}

	var logger Logger
	if bus.options != nil && bus.options.Logger != nil {
		logger = bus.options.Logger
	}

	maxLen := bus.options.StreamTrimMaxLen
	if maxLen <= 0 {
		if logger != nil {
			logger.Debugf(ctx, "stream %s cleanup skipped: trim max len disabled", stream)
		}
		return nil
	}

	var trimCmd *redis.IntCmd
	if bus.options.StreamTrimApprox {
		limit := bus.options.StreamTrimLimit
		if limit > 0 {
			trimCmd = bus.conn.XTrimMaxLenApprox(ctx, stream, maxLen, limit)
		} else {
			trimCmd = bus.conn.XTrimMaxLen(ctx, stream, maxLen)
		}
	} else {
		trimCmd = bus.conn.XTrimMaxLen(ctx, stream, maxLen)
	}

	trimmed, err := trimCmd.Result()
	if err != nil {
		if errors.Is(err, redis.Nil) || isRedisNoStreamError(err) {
			if logger != nil {
				logger.Infof(ctx, "redis stream %s not found, skip trim", stream)
			}
			return nil
		}
		if logger != nil {
			logger.Errorf(ctx, "failed to trim stream %s: %v", stream, err)
		}
		return err
	}

	if logger != nil {
		logger.Infof(ctx, "redis stream %s trimmed to max len %d, removed %d entries", stream, maxLen, trimmed)
	}
	return nil
}

func isRedisNoStreamError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "no such key") ||
		strings.Contains(msg, "stream not found") ||
		strings.Contains(msg, "key not found")
}

func (bus *redisEventBus) xReadGroup(ctx context.Context, stream, group, consumer string, h SubscribeHandler, options *SubscribeOptions) (err error) {
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
					if err = bus.handleSubMessageWithRetry(ctx, s.Stream, group, s.Messages[j], h, options); err != nil {
						bus.options.Logger.Errorf(ctx, "message processing failed after retries: %v", err)
						if !bus.options.SkipBadMessages {
							return err
						}
					}
				}
			}
		}
	}
}

func (bus *redisEventBus) handleSubMessage(ctx context.Context, stream, group string, msg redis.XMessage, h SubscribeHandler, options *SubscribeOptions) (err error) {
	msgData, msgDataOK := msg.Values["message"]
	if !msgDataOK {
		return
	}

	bus.options.Logger.Debugf(ctx, "subscribe msg data: %s", msgData)

	msgBytes := []byte(msgData.(string))

	var convertedMsg *Message
	var convertErr error

	if options.Converter == nil {
		convertedMsg = &Message{}
		convertErr = bus.options.Serialize.Unmarshal(msgBytes, convertedMsg)
	} else {
		convertedMsg, convertErr = options.Converter.Convert(msgBytes)
	}

	if convertErr != nil {
		bus.options.Logger.Errorf(ctx, "failed to convert message: %v", convertErr)
		if bus.options.SkipBadMessages {
			if ackErr := bus.conn.XAck(ctx, stream, group, msg.ID).Err(); ackErr != nil {
				bus.options.Logger.Errorf(ctx, "failed to ack bad message: %v", ackErr)
			}
			return nil
		}
		return convertErr
	}

	if err = h(ctx, convertedMsg); err == nil {
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
func (bus *redisEventBus) handleSubMessageWithRetry(ctx context.Context, stream, group string, msg redis.XMessage, h SubscribeHandler, options *SubscribeOptions) (err error) {
	maxRetries := bus.options.MessageMaxRetries
	if maxRetries <= 0 {
		maxRetries = 2
	}

	retryCount := 0
	baseInterval := bus.options.RetryInterval
	if baseInterval <= 0 {
		baseInterval = time.Second
	}

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			retryInterval := bus.calculateBackoff(baseInterval, retryCount)
			bus.options.Logger.Warnf(ctx, "Retrying message processing (attempt %d/%d) after %v",
				attempt, maxRetries+1, retryInterval)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(retryInterval):
			}
		}

		err = bus.handleSubMessage(ctx, stream, group, msg, h, options)
		if err == nil {
			return nil
		}

		retryCount++
		bus.options.Logger.Errorf(ctx, "Message processing failed (attempt %d/%d): %v",
			attempt+1, maxRetries+1, err)
	}

	// 如果所有重试都失败，发送到死信队列
	if bus.options.DeadLetterTopic != "" {
		bus.options.Logger.Errorf(ctx, "Message processing failed after %d attempts, sending to dead letter queue", maxRetries+1)
		
		// 先尝试将原始消息发送到死信队列
		originalMsg := &Message{
			Header: make(map[string]string),
			Value:  []byte(fmt.Sprintf("%v", msg.Values)),
		}
		originalMsg.Header["topic"] = stream
		originalMsg.Header["message_id"] = msg.ID
		
		dlqMsg := CreateDeadLetterMessage(stream, originalMsg, err.Error(), bus.options.DeadLetterTopic, "redis", bus.options.Serialize)
		if dlqErr := bus.Publish(ctx, bus.options.DeadLetterTopic, dlqMsg); dlqErr != nil {
			bus.options.Logger.Errorf(ctx, "Failed to send to dead letter queue: %v", dlqErr)
			// 死信队列发送失败，记录日志但继续处理（避免阻塞）
		} else {
			// 死信队列发送成功，确认原始消息
			if ackErr := bus.conn.XAck(ctx, stream, group, msg.ID).Err(); ackErr != nil {
				bus.options.Logger.Errorf(ctx, "Failed to ack message after dead letter: %v", ackErr)
			}
		}
	}

	return err
}

