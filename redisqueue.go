package eventbus

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

var (
	// DefaultRedisQueueOptions 默认Redis Queue可选项
	DefaultRedisQueueOptions = RedisQueueOptions{
		PollInterval: time.Second,
		Serialize:    &JSONSerialize{},
		Logger:       &StdLogger{},
	}
)

// RedisQueueOptions Redis Queue可选项
type RedisQueueOptions struct {
	PollInterval time.Duration // 轮询间隔
	Serialize    Serializer
	Logger       Logger
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

	// 根据是否有GroupID来决定队列名称
	queueName := topic
	if gid, ok := FromGroupIDContext(ctx); ok {
		// 为了支持消费组，发布到特定消费组的队列
		queueName = fmt.Sprintf("%s:group:%s", topic, gid)
	}

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

	if async {
		go func() {
			// 异步发布到Redis List
			if asyncErr := bus.conn.LPush(ctx, queueName, string(queueMsgData)).Err(); asyncErr != nil {
				bus.options.Logger.Errorf(ctx, "async publish to queue %s error: %v", queueName, asyncErr)
			}
		}()
		return
	}

	// 同步发布到Redis List
	err = bus.conn.LPush(ctx, queueName, string(queueMsgData)).Err()
	return
}

func (bus *redisQueueEventBus) Subscribe(ctx context.Context, topic string, h SubscribeHandler) (err error) {
	return bus.subscribe(ctx, topic, h, false)
}

func (bus *redisQueueEventBus) SubscribeAsync(ctx context.Context, topic string, h SubscribeHandler) (err error) {
	return bus.subscribe(ctx, topic, h, true)
}

func (bus *redisQueueEventBus) subscribe(ctx context.Context, topic string, h SubscribeHandler, async bool) (err error) {
	// 根据是否有GroupID来决定队列名称
	queueName := topic
	if gid, ok := FromGroupIDContext(ctx); ok {
		// 为了支持消费组，我们使用不同的队列名称
		queueName = fmt.Sprintf("%s:group:%s", topic, gid)
	}

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
				// 遇到错误时，短暂休眠后继续
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Second):
					continue
				}
			}

			if len(result) != 2 {
				continue
			}

			msgData := result[1]
			if err = bus.handleQueueMessage(ctx, msgData, h); err != nil {
				bus.options.Logger.Errorf(ctx, "handle queue message error: %v", err)
				// 处理消息失败，继续处理下一条消息
				continue
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
