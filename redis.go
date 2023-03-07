package eventbus

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

var (
	// DefaultRedisOptions 默认Redis可选项
	DefaultRedisOptions = RedisOptions{
		ReadCount: 1,
		Serialize: &JSONSerialize{},
		Logger:    &StdLogger{},
	}
)

// RedisOptions Redis可选项
type RedisOptions struct {
	ReadCount int64
	ReadBlock time.Duration
	Serialize Serializer
	Logger    Logger
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
	err = bus.conn.XGroupCreateMkStream(ctx, topic, queueName, "0-0").Err()
	if err != nil {
		bus.options.Logger.Errorf(ctx, "XGroupCreateMkStream: %v", err)
		return
	}
	if async {
		go func(subCtx context.Context) {
			if asyncErr := bus.xReadGroup(subCtx, topic, queueName, consumer, h); asyncErr != nil {
				bus.options.Logger.Errorf(subCtx, "async subscribe %s error: %v", topic, asyncErr)
			}
		}(ctx)
	} else {
		err = bus.xReadGroup(ctx, topic, queueName, consumer, h)
	}
	return
}

func (bus *redisEventBus) xReadGroup(ctx context.Context, stream, group, consumer string, h SubscribeHandler) (err error) {
	for {
		select {
		case <-ctx.Done():
			return
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
				return
			}

			streamsLen := len(streams)
			for i := 0; i < streamsLen; i++ {
				s := streams[i]
				msgLen := len(s.Messages)
				for j := 0; j < msgLen; j++ {
					if err = bus.handleSubMessage(ctx, s.Stream, group, s.Messages[j], h); err != nil {
						return
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
