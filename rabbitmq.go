package eventbus

import (
	"context"
	"errors"

	"os"
	"runtime"

	"sync"

	"github.com/streadway/amqp"
)

var (
	// ErrRabbitMQChannelNotFound ...
	ErrRabbitMQChannelNotFound = errors.New("rabbitmq channel not found")
)

var (
	// DefaultRabbitMQOptions 默认RabbitMQ可选项
	DefaultRabbitMQOptions = RabbitMQOptions{
		ExchangeName:        "nilorg.eventbus",
		ExchangeType:        "topic",
		DefaultGroupID:      "nilorg.eventbus.default.group.v1",
		QueueMessageExpires: 864000000, // 默认 864000000 毫秒 (10 天)
		Serialize:           &JSONSerialize{},
		Logger:              &StdLogger{},
	}
)

// RabbitMQOptions RabbitMQ可选项
type RabbitMQOptions struct {
	ExchangeName        string
	ExchangeType        string
	DefaultGroupID      string
	QueueMessageExpires int64
	Serialize           Serializer
	Logger              Logger
}

type groupIDKey struct{}

// NewGroupIDContext ...
func NewGroupIDContext(parent context.Context, groupID string) context.Context {
	return context.WithValue(parent, groupIDKey{}, groupID)
}

// FromGroupIDContext ...
func FromGroupIDContext(ctx context.Context) (groupID string, ok bool) {
	groupID, ok = ctx.Value(groupIDKey{}).(string)
	return
}

// NewRabbitMQ 创建RabbitMQ事件总线
func NewRabbitMQ(conn *amqp.Connection, options *RabbitMQOptions) (bus EventBus, err error) {
	var ops RabbitMQOptions
	if options == nil {
		ops = DefaultRabbitMQOptions
	} else {
		ops = *options
	}
	rbus := &rabbitMQEventBus{
		conn:    conn,
		options: &ops,
		channelPool: &sync.Pool{
			New: func() interface{} {
				ch, chErr := conn.Channel()
				if chErr != nil {
					return nil
				}
				return ch
			},
		},
	}
	err = rbus.exchangeDeclare()
	if err != nil {
		return
	}
	bus = rbus
	return
}

type rabbitMQEventBus struct {
	options     *RabbitMQOptions
	conn        *amqp.Connection
	channelPool *sync.Pool
}

func (bus *rabbitMQEventBus) getChannel() (ch *amqp.Channel, err error) {
	bus.options.Logger.Debugf(context.Background(), "PID: %d, GroutineCount: %d, Channel OK", os.Getpid(), runtime.NumGoroutine())
	v := bus.channelPool.Get()
	ok := false
	if v == nil {
		ch, err = bus.conn.Channel()
		if err != nil {
			return
		}
		return
	}
	if ch, ok = v.(*amqp.Channel); !ok {
		err = ErrRabbitMQChannelNotFound
		return
	}
	return
}

func (bus *rabbitMQEventBus) putChannel(ch *amqp.Channel) {
	bus.channelPool.Put(ch)
}

func (bus *rabbitMQEventBus) exchangeDeclare() (err error) {
	var ch *amqp.Channel
	if ch, err = bus.getChannel(); err != nil {
		return
	}
	defer bus.putChannel(ch)

	err = ch.ExchangeDeclare(
		bus.options.ExchangeName, //名称
		bus.options.ExchangeType, //类型
		true,                     //持久
		false,                    //自动删除的
		false,                    //内部
		false,                    //无等待
		nil,                      //参数
	)
	return
}

func (bus *rabbitMQEventBus) queueDeclare(ch *amqp.Channel, groupID string) (queue amqp.Queue, err error) {
	args := amqp.Table{
		"x-message-ttl": bus.options.QueueMessageExpires,
	}
	queue, err = ch.QueueDeclare(
		groupID, // 名称
		true,    // 持久性
		false,   // 删除未使用时
		false,   // 独有的
		false,   // 不等待
		args,    //参数
	)
	return
}

func (bus *rabbitMQEventBus) Publish(ctx context.Context, topic string, v interface{}) (err error) {
	return bus.publish(ctx, topic, v, false)
}

func (bus *rabbitMQEventBus) PublishAsync(ctx context.Context, topic string, v interface{}) (err error) {
	return bus.publish(ctx, topic, v, true)
}

func (bus *rabbitMQEventBus) publish(ctx context.Context, topic string, v interface{}, async bool) (err error) {
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
	var data []byte
	data, err = bus.options.Serialize.Marshal(msg)
	if err != nil {
		return
	}
	bus.options.Logger.Debugf(ctx, "publish msg data: %s", string(data))
	var ch *amqp.Channel
	if ch, err = bus.getChannel(); err != nil {
		return
	}
	defer bus.putChannel(ch)

	err = ch.Publish(
		bus.options.ExchangeName, //交换
		topic,                    //路由密钥
		!async,                   //强制
		false,                    //立即
		amqp.Publishing{
			ContentType: bus.options.Serialize.ContentType(),
			Body:        data,
		})
	return
}

func (bus *rabbitMQEventBus) Subscribe(ctx context.Context, topic string, h SubscribeHandler) (err error) {
	return bus.subscribe(ctx, topic, h, false)
}

func (bus *rabbitMQEventBus) SubscribeAsync(ctx context.Context, topic string, h SubscribeHandler) (err error) {
	return bus.subscribe(ctx, topic, h, true)
}

func (bus *rabbitMQEventBus) subscribe(ctx context.Context, topic string, h SubscribeHandler, async bool) (err error) {
	var ch *amqp.Channel
	if ch, err = bus.getChannel(); err != nil {
		return
	}
	defer bus.putChannel(ch)
	groupID := bus.options.DefaultGroupID
	if gid, ok := FromGroupIDContext(ctx); ok {
		groupID = gid
		return
	}
	var queue amqp.Queue
	// 一对多要生产不同的queue，根据groupID来区分
	queue, err = bus.queueDeclare(ch, groupID)
	if err != nil {
		return
	}
	err = ch.QueueBind(
		queue.Name,               // 队列
		topic,                    // routing key
		bus.options.ExchangeName, // 交换
		!async,                   // 不等待
		nil,
	)
	if err != nil {
		return
	}

	var msgs <-chan amqp.Delivery
	msgs, err = ch.Consume(
		queue.Name, // 队列
		"",         // 消费者
		false,      // 自动确认
		false,      // 独有的
		false,      // no-local
		!async,     // 不等待
		nil,        // 参数
	)
	if err != nil {
		return
	}

	if async {
		go func() {
			if asyncErr := bus.handleSubMessage(ctx, msgs, h); asyncErr != nil {
				bus.options.Logger.Errorf(context.Background(), "async subscribe %s error: %v", topic, asyncErr)
			}
		}()
	} else {
		err = bus.handleSubMessage(ctx, msgs, h)
	}
	return
}

func (bus *rabbitMQEventBus) handleSubMessage(ctx context.Context, msgs <-chan amqp.Delivery, h SubscribeHandler) (err error) {
	for {
		select {
		case msg := <-msgs:
			var m Message
			bus.options.Logger.Debugf(ctx, "subscribe msg data: %s", string(msg.Body))
			if err = bus.options.Serialize.Unmarshal(msg.Body, &m); err != nil {
				return
			}
			if err = h(ctx, &m); err == nil {
				if err = msg.Ack(false); err != nil {
					return
				}
			} else {
				if err = msg.Nack(false, true); err != nil {
					return
				}
				return
			}
		case <-ctx.Done():
			return
		}
	}
}
