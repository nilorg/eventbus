package eventbus

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"os"
	"runtime"
	"strconv"

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
		ExchangeName: "nilorg.eventbus",
		ExchangeType: "topic",
		Serialize:    &JSONSerialize{},
		Logger:       &StdLogger{},
	}
)

// RabbitMQOptions RabbitMQ可选项
type RabbitMQOptions struct {
	ExchangeName string
	ExchangeType string
	Serialize    Serializer
	Logger       Logger
}

type rabbitMQRoutingKey struct{}

// NewRabbitMQRoutingKeyContext ...
func NewRabbitMQRoutingKeyContext(parent context.Context, routingKey string) context.Context {
	return context.WithValue(parent, rabbitMQRoutingKey{}, routingKey)
}

// FromRabbitMQRoutingKeyContext ...
func FromRabbitMQRoutingKeyContext(ctx context.Context) (routingKey string, ok bool) {
	routingKey, ok = ctx.Value(rabbitMQRoutingKey{}).(string)
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
		conn:             conn,
		options:          &ops,
		subscribeCancels: &sync.Map{},
		channelPool: &sync.Pool{
			New: func() interface{} {
				fmt.Println("===================创建")
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

type subscribeCancel struct {
	Ctx       context.Context
	CtxCancel context.CancelFunc
}

type rabbitMQEventBus struct {
	options          *RabbitMQOptions
	conn             *amqp.Connection
	subscribeCancels *sync.Map
	channelPool      *sync.Pool
}

// GetGoroutineID ...
func GetGoroutineID() uint64 {
	b := make([]byte, 64)
	runtime.Stack(b, false)
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}
func (bus *rabbitMQEventBus) channel() (ch *amqp.Channel, err error) {
	bus.options.Logger.Debugf(context.Background(), "PID: %d, GoroutineID: %d Channel OK", os.Getpid(), GetGoroutineID())
	v := bus.channelPool.Get()
	ok := false
	if v == nil {
		ch, err = bus.conn.Channel()
		if err != nil {
			return
		}
		bus.channelPool.Put(ch)
		return
	}
	if ch, ok = v.(*amqp.Channel); !ok {
		err = ErrRabbitMQChannelNotFound
		return
	}
	return
}

func (bus *rabbitMQEventBus) exchangeDeclare() (err error) {
	var ch *amqp.Channel
	if ch, err = bus.channel(); err != nil {
		return
	}
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

func (bus *rabbitMQEventBus) queueDeclare(ch *amqp.Channel, topic string) (queue amqp.Queue, err error) {
	queue, err = ch.QueueDeclare(
		topic, // 名称
		true,  // 持久性
		false, // 删除未使用时
		false, // 独有的
		false, // 不等待
		nil,   //参数
	)
	return
}

func (bus *rabbitMQEventBus) Publish(ctx context.Context, topic string, v interface{}) (err error) {
	return bus.publish(ctx, topic, v, "", false)
}

func (bus *rabbitMQEventBus) PublishAsync(ctx context.Context, topic, callbackName string, v interface{}) (err error) {
	return bus.publish(ctx, topic, v, callbackName, true)
}

func (bus *rabbitMQEventBus) publish(ctx context.Context, topic string, v interface{}, callbackName string, async bool) (err error) {
	msg := &Message{
		Header: make(MessageHeader),
		Value:  v,
	}
	if async {
		msg.Header[MessageHeaderCallback] = callbackName
	}
	var data []byte
	data, err = bus.options.Serialize.Marshal(msg)
	if err != nil {
		return
	}
	publishKey := ""
	if routingKey, ok := FromRabbitMQRoutingKeyContext(ctx); ok {
		publishKey = routingKey
	}
	var ch *amqp.Channel
	if ch, err = bus.channel(); err != nil {
		return
	}
	err = ch.Publish(
		bus.options.ExchangeName, //交换
		publishKey,               //路由密钥
		!async,                   //强制
		!async,                   //立即
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
	if ch, err = bus.channel(); err != nil {
		return
	}
	var queue amqp.Queue
	// 一对多，要生产不同的 queue
	queue, err = bus.queueDeclare(ch, topic)
	if err != nil {
		return
	}
	consumeQueueName := ""
	if routingKey, ok := FromRabbitMQRoutingKeyContext(ctx); ok {
		err = ch.QueueBind(
			queue.Name,               // 队列
			routingKey,               // routing key
			bus.options.ExchangeName, // 交换
			false,                    // 不等待
			nil,
		)
		if err != nil {
			return
		}
		consumeQueueName = queue.Name
	}
	var msgs <-chan amqp.Delivery
	msgs, err = ch.Consume(
		consumeQueueName, // 队列
		"",               // 消费者
		false,            // 自动确认
		false,            // 独有的
		false,            // no-local
		!async,           // 不等待
		nil,              // 参数
	)
	if err != nil {
		return
	}

	var (
		cancelCtx context.Context
		cancel    context.CancelFunc
	)
	if v, ok := bus.subscribeCancels.Load(topic); ok {
		sc := v.(*subscribeCancel)
		cancelCtx = sc.Ctx
		cancel = sc.CtxCancel
	} else {
		cancelCtx, cancel = context.WithCancel(ctx)
		sc := &subscribeCancel{
			Ctx:       cancelCtx,
			CtxCancel: cancel,
		}
		bus.subscribeCancels.Store(topic, sc)
	}
	if async {
		go func() {
			if asyncErr := bus.handleSubMessage(cancelCtx, msgs, h); asyncErr != nil {
				bus.options.Logger.Errorf(context.Background(), "async subscribe %s error: %v", topic, asyncErr)
			}
		}()
	} else {
		err = bus.handleSubMessage(cancelCtx, msgs, h)
	}
	return
}

func (bus *rabbitMQEventBus) handleSubMessage(ctx context.Context, msgs <-chan amqp.Delivery, h SubscribeHandler) (err error) {
	for {
		select {
		case msg := <-msgs:
			var m Message
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

func (bus *rabbitMQEventBus) Unsubscribe(topic string) (err error) {
	if v, ok := bus.subscribeCancels.Load(topic); ok {
		sc := v.(*subscribeCancel)
		bus.unsubscribe(topic, sc)
	}
	return
}

func (bus *rabbitMQEventBus) unsubscribe(topic string, sc *subscribeCancel) (err error) {
	bus.options.Logger.Debugf(context.Background(), "unsubscribe %s", topic)
	if sc != nil && sc.CtxCancel != nil {
		bus.options.Logger.Debugf(context.Background(), "unsubscribe %s CtxCancel", topic)
		sc.CtxCancel()
	}
	var ch *amqp.Channel
	if ch, err = bus.channel(); err != nil {
		return
	}
	var queue amqp.Queue
	queue, err = ch.QueueInspect(topic)
	if err != nil {
		return
	}
	bus.options.Logger.Debugf(context.Background(), "queue Consumers: %d, Messages: %d", queue.Consumers, queue.Messages)
	if queue.Consumers == 0 && queue.Messages == 0 {
		_, err = ch.QueueDelete(topic, true, true, false)
	}
	return
}

func (bus *rabbitMQEventBus) Close() (err error) {
	bus.subscribeCancels.Range(func(key interface{}, value interface{}) bool {
		topic := key.(string)
		sc := value.(*subscribeCancel)
		if unErr := bus.unsubscribe(topic, sc); unErr != nil {
			bus.options.Logger.Errorf(context.Background(), "unsubscribe %s error: %v", topic, unErr)
		}
		return true
	})
	var ch *amqp.Channel
	if ch, err = bus.channel(); err != nil {
		return
	}
	return ch.Close()
}
