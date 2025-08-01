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

	// 创建队列消息
	queueMsg := &QueueMessage{
		ID:     fmt.Sprintf("%d", time.Now().UnixNano()),
		Topic:  topic,
		Header: msg.Header,
		Value:  msg.Value,
	}

	// 如果有GroupID，设置到消息中
	if gid, ok := FromGroupIDContext(ctx); ok {
		queueMsg.GroupID = gid
	}

	var msgData []byte
	msgData, err = bus.options.Serialize.Marshal(queueMsg)
	if err != nil {
		return
	}

	bus.options.Logger.Debugf(ctx, "publish queue msg data: %s", string(msgData))

	if async {
		go func() {
			// 异步发布到Redis List
			if asyncErr := bus.conn.LPush(ctx, topic, string(msgData)).Err(); asyncErr != nil {
				bus.options.Logger.Errorf(ctx, "async publish to queue %s error: %v", topic, asyncErr)
			}
		}()
		return
	}

	// 同步发布到Redis List
	err = bus.conn.LPush(ctx, topic, string(msgData)).Err()
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
	var groupID string
	if gid, ok := FromGroupIDContext(ctx); ok {
		groupID = gid
		// 为了支持消费组，我们使用不同的队列名称
		queueName = fmt.Sprintf("%s:group:%s", topic, gid)
	}

	if async {
		go func(subCtx context.Context) {
			if asyncErr := bus.consumeQueue(subCtx, queueName, groupID, h); asyncErr != nil {
				bus.options.Logger.Errorf(subCtx, "async subscribe queue %s error: %v", queueName, asyncErr)
			}
		}(ctx)
		return
	}

	err = bus.consumeQueue(ctx, queueName, groupID, h)
	return
}

func (bus *redisQueueEventBus) consumeQueue(ctx context.Context, queueName, groupID string, h SubscribeHandler) (err error) {
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
			if err = bus.handleQueueMessage(ctx, msgData, groupID, h); err != nil {
				bus.options.Logger.Errorf(ctx, "handle queue message error: %v", err)
				// 处理消息失败，继续处理下一条消息
				continue
			}
		}
	}
}

func (bus *redisQueueEventBus) handleQueueMessage(ctx context.Context, msgData, expectedGroupID string, h SubscribeHandler) (err error) {
	var queueMsg QueueMessage
	if err = bus.options.Serialize.Unmarshal([]byte(msgData), &queueMsg); err != nil {
		return
	}

	// 如果指定了GroupID，检查消息是否匹配
	if expectedGroupID != "" && queueMsg.GroupID != expectedGroupID {
		// 消息不匹配当前消费组，重新放入队列
		return bus.conn.LPush(ctx, fmt.Sprintf("%s:group:%s", queueMsg.Topic, queueMsg.GroupID), msgData).Err()
	}

	bus.options.Logger.Debugf(ctx, "subscribe queue msg data: %s", msgData)

	// 构造消息对象
	msg := &Message{
		Header: queueMsg.Header,
		Value:  queueMsg.Value,
	}

	// 如果Value是map类型，保持数据结构一致性
	if valueMap, ok := queueMsg.Value.(map[string]interface{}); ok {
		// 这里我们保持Value为原始的interface{}类型，让消费者自己处理类型转换
		msg.Value = valueMap
	}

	// 调用处理器
	err = h(ctx, msg)
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
