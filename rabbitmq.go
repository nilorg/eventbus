package eventbus

import (
	"context"

	"log"
	"sync"

	"github.com/streadway/amqp"
)

var (
	// DefaultRabbitMQOptions 默认RabbitMQ可选项
	DefaultRabbitMQOptions = RabbitMQOptions{
		ExchangeName: "nilorg.eventbus",
		ExchangeType: "topic",
		Serialize:    &JSONSerialize{},
	}
)

// RabbitMQOptions RabbitMQ可选项
type RabbitMQOptions struct {
	ExchangeName string
	ExchangeType string
	Serialize    Serializer
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
	var (
		ch  *amqp.Channel
		ops RabbitMQOptions
	)
	ch, err = conn.Channel()
	if err != nil {
		return
	}
	if options == nil {
		ops = DefaultRabbitMQOptions
	} else {
		ops = *options
	}
	rbus := &rabbitMQEventBus{
		channel:          ch,
		options:          &ops,
		subscribeCancels: &sync.Map{},
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
	channel          *amqp.Channel
	subscribeCancels *sync.Map
}

func (bus *rabbitMQEventBus) exchangeDeclare() (err error) {
	err = bus.channel.ExchangeDeclare(
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

func (bus *rabbitMQEventBus) queueDeclare(topic string) (queue amqp.Queue, err error) {
	queue, err = bus.channel.QueueDeclare(
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
	return bus.publish(ctx, topic, v, false)
}

func (bus *rabbitMQEventBus) PublishAsync(ctx context.Context, topic, callbackName string, v interface{}) (err error) {
	return bus.publish(ctx, topic, v, true)
}

func (bus *rabbitMQEventBus) publish(ctx context.Context, topic string, v interface{}, async bool) (err error) {
	msg := &Message{
		Value: v,
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
	err = bus.channel.Publish(
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
	var queue amqp.Queue
	// 一对多，要生产不同的 queue
	queue, err = bus.queueDeclare(topic)
	if err != nil {
		return
	}
	consumeQueueName := ""
	if routingKey, ok := FromRabbitMQRoutingKeyContext(ctx); ok {
		err = bus.channel.QueueBind(
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
	msgs, err = bus.channel.Consume(
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
			asyncErr := bus.handleSubMessage(cancelCtx, msgs, h)
			log.Printf("EventBus async error: %s\n", asyncErr)
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
		if sc != nil && sc.CtxCancel != nil {
			sc.CtxCancel()
		}
	}
	return
}

func (bus *rabbitMQEventBus) Close() (err error) {
	return bus.channel.Close()
}
