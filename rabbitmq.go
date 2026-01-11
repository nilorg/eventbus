package eventbus

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/isayme/go-amqp-reconnect/rabbitmq"
	"github.com/nilorg/sdk/pool"
	"github.com/streadway/amqp"
)

var (
	// ErrRabbitMQChannelNotFound ...
	ErrRabbitMQChannelNotFound = errors.New("rabbitmq channel not found")
)

const (
	DefaultRabbitMQDeadLetterExchange = "nilorg.eventbus.dlx"
)

var (
	// DefaultRabbitMQOptions 默认RabbitMQ可选项
	DefaultRabbitMQOptions = RabbitMQOptions{
		ExchangeName:        "nilorg.eventbus",
		ExchangeType:        "topic",
		QueueMessageExpires: 864000000, // 默认 864000000 毫秒 (10 天)
		Serialize:           &JSONSerialize{},
		Logger:              &StdLogger{},
		PoolMinOpen:         1,
		PoolMaxOpen:         10,
		MaxRetries:          3,
		RetryInterval:       time.Second * 2,
		BackoffMultiplier:   2.0,
		MaxBackoff:          time.Minute,
		MessageMaxRetries:   2,
		SkipBadMessages:     true,
		DeadLetterExchange:  "", // 默认不使用死信队列，由用户根据需要配置
	}
)

// RabbitMQOptions RabbitMQ可选项
type RabbitMQOptions struct {
	ExchangeName             string
	ExchangeType             string
	QueueMessageExpires      int64
	Serialize                Serializer
	Logger                   Logger
	PoolMinOpen, PoolMaxOpen int

	// 重试机制配置
	MaxRetries        int           // 连接重试次数
	RetryInterval     time.Duration // 重试间隔
	BackoffMultiplier float64       // 指数退避倍数
	MaxBackoff        time.Duration // 最大退避时间

	// 消息级重试配置
	MessageMaxRetries int  // 消息最大重试次数
	SkipBadMessages   bool // 是否跳过无法处理的消息

	// 死信队列配置
	DeadLetterExchange string // 死信交换机名称
}

// NewRabbitMQ 创建RabbitMQ事件总线
func NewRabbitMQ(conn *rabbitmq.Connection, options ...*RabbitMQOptions) (bus EventBus, err error) {
	var ops RabbitMQOptions
	if len(options) == 0 {
		ops = DefaultRabbitMQOptions
	} else {
		ops = *options[0]
	}
	var channelPool pool.Pooler
	channelPool, err = pool.NewCommonPool(ops.PoolMinOpen, ops.PoolMaxOpen, func() (io.Closer, error) {
		return conn.Channel()
	})
	if err != nil {
		return
	}
	rbus := &rabbitMQEventBus{
		conn:        conn,
		options:     &ops,
		channelPool: channelPool,
	}
	err = rbus.exchangeDeclare(context.Background())
	if err != nil {
		return
	}
	// 设置死信交换机
	err = rbus.setupDeadLetterExchange(context.Background())
	if err != nil {
		return
	}
	bus = rbus
	return
}

type rabbitMQEventBus struct {
	options     *RabbitMQOptions
	conn        *rabbitmq.Connection
	channelPool pool.Pooler
}

// Close 关闭事件总线，清理资源
// 此方法不在 EventBus 接口中，需要通过类型断言调用
func (bus *rabbitMQEventBus) Close() error {
	if bus.channelPool != nil {
		// 关闭连接池，这会自动关闭池中的所有资源
		err := bus.channelPool.Shutdown()
		if err != nil {
			return fmt.Errorf("failed to shutdown channel pool: %w", err)
		}
	}
	return nil
}

// executeWithRetry 执行带重试的操作
func (bus *rabbitMQEventBus) executeWithRetry(ctx context.Context, operation func() error) error {
	var lastErr error

	for attempt := 0; attempt <= bus.options.MaxRetries; attempt++ {
		if attempt > 0 {
			// 计算指数退避时间
			backoff := time.Duration(float64(bus.options.RetryInterval) *
				math.Pow(bus.options.BackoffMultiplier, float64(attempt-1)))
			if backoff > bus.options.MaxBackoff {
				backoff = bus.options.MaxBackoff
			}

			bus.options.Logger.Debugf(ctx, "retrying operation (attempt %d/%d) after %v",
				attempt+1, bus.options.MaxRetries+1, backoff)

			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		if err := operation(); err != nil {
			lastErr = err
			bus.options.Logger.Errorf(ctx, "operation failed (attempt %d/%d): %v",
				attempt+1, bus.options.MaxRetries+1, err)
			continue
		}

		return nil
	}

	return fmt.Errorf("operation failed after %d attempts: %w", bus.options.MaxRetries+1, lastErr)
}

// setupDeadLetterExchange 设置死信交换机
func (bus *rabbitMQEventBus) setupDeadLetterExchange(ctx context.Context) error {
	if bus.options.DeadLetterExchange == "" {
		return nil
	}

	return bus.executeWithRetry(ctx, func() error {
		ch, err := bus.getChannel(ctx)
		if err != nil {
			return err
		}
		defer bus.putChannel(ch)

		// 声明死信交换机
		return ch.ExchangeDeclare(
			bus.options.DeadLetterExchange,
			"topic",
			true,  // 持久
			false, // 自动删除
			false, // 内部
			false, // 无等待
			nil,
		)
	})
}

// republishForRetry 重新发布消息用于重试（带有更新后的 retry count）
func (bus *rabbitMQEventBus) republishForRetry(ctx context.Context, originalMsg amqp.Delivery, retryCount int) error {
	return bus.executeWithRetry(ctx, func() error {
		ch, err := bus.getChannel(ctx)
		if err != nil {
			return err
		}
		defer bus.putChannel(ch)

		// 复制原始 headers 并更新 retry count
		headers := make(amqp.Table)
		for k, v := range originalMsg.Headers {
			headers[k] = v
		}
		headers["x-retry-count"] = retryCount

		return ch.Publish(
			originalMsg.Exchange,   // 使用原始交换机
			originalMsg.RoutingKey, // 使用原始路由键
			false,                  // 强制
			false,                  // 立即
			amqp.Publishing{
				Headers:         headers,
				ContentType:     originalMsg.ContentType,
				ContentEncoding: originalMsg.ContentEncoding,
				DeliveryMode:    originalMsg.DeliveryMode,
				Priority:        originalMsg.Priority,
				CorrelationId:   originalMsg.CorrelationId,
				ReplyTo:         originalMsg.ReplyTo,
				Expiration:      originalMsg.Expiration,
				MessageId:       originalMsg.MessageId,
				Timestamp:       originalMsg.Timestamp,
				Type:            originalMsg.Type,
				UserId:          originalMsg.UserId,
				AppId:           originalMsg.AppId,
				Body:            originalMsg.Body,
			},
		)
	})
}

// sendToDeadLetter 发送消息到死信队列
func (bus *rabbitMQEventBus) sendToDeadLetter(ctx context.Context, originalTopic string, msg *Message, errorReason string) {
	if bus.options.DeadLetterExchange == "" {
		return
	}

	dlqMsg := CreateDeadLetterMessage(originalTopic, msg, errorReason, bus.options.DeadLetterExchange, "rabbitmq")

	err := bus.executeWithRetry(ctx, func() error {
		ch, err := bus.getChannel(ctx)
		if err != nil {
			return err
		}
		defer bus.putChannel(ch)

		data, err := bus.options.Serialize.Marshal(dlqMsg)
		if err != nil {
			return err
		}

		// 创建AMQP头信息
		headers := make(amqp.Table)
		for k, v := range dlqMsg.Header {
			headers[k] = v
		}

		return ch.Publish(
			bus.options.DeadLetterExchange,
			fmt.Sprintf("%s.failed", originalTopic),
			false, // 强制
			false, // 立即
			amqp.Publishing{
				ContentType: bus.options.Serialize.ContentType(),
				Body:        data,
				Headers:     headers,
			},
		)
	})

	if err != nil {
		bus.options.Logger.Errorf(ctx, "failed to send message to dead letter queue: %v", err)
	} else {
		bus.options.Logger.Debugf(ctx, "sent failed message to dead letter queue: %s.failed", originalTopic)
	}
}

func (bus *rabbitMQEventBus) getChannel(_ context.Context) (ch *rabbitmq.Channel, err error) {
	var v io.Closer
	v, err = bus.channelPool.Get()
	if err != nil {
		return
	}
	var ok bool
	if ch, ok = v.(*rabbitmq.Channel); !ok {
		// 类型断言失败时，需要将资源放回池中
		bus.channelPool.Put(v)
		err = ErrRabbitMQChannelNotFound
		return
	}
	return
}

func (bus *rabbitMQEventBus) putChannel(ch *rabbitmq.Channel) {
	bus.channelPool.Put(ch)
}

func (bus *rabbitMQEventBus) exchangeDeclare(ctx context.Context) (err error) {
	var ch *rabbitmq.Channel
	if ch, err = bus.getChannel(ctx); err != nil {
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

func (bus *rabbitMQEventBus) queueDeclare(ch *rabbitmq.Channel, queueName string) (queue amqp.Queue, err error) {
	args := amqp.Table{
		"x-message-ttl": bus.options.QueueMessageExpires,
	}
	queue, err = ch.QueueDeclare(
		queueName, // 名称
		true,      // 持久性
		false,     // 删除未使用时
		false,     // 独有的
		false,     // 不等待
		args,      //参数
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

	// 自动注入 topic 到消息头
	msg.Header["topic"] = topic

	// set message header
	if f, ok := FromSetMessageHeaderContext(ctx); ok {
		if headers := f(ctx); headers != nil {
			for key, value := range headers {
				msg.Header[key] = value
			}
		}
	}

	var data []byte
	data, err = bus.options.Serialize.Marshal(msg.Value)
	if err != nil {
		return
	}
	bus.options.Logger.Debugf(ctx, "publish msg data: %s", string(data))

	// 使用重试机制发布消息
	return bus.executeWithRetry(ctx, func() error {
		ch, err := bus.getChannel(ctx)
		if err != nil {
			return err
		}
		defer bus.putChannel(ch)

		headers := make(amqp.Table)
		for k, v := range msg.Header {
			headers[k] = v
		}

		return ch.Publish(
			bus.options.ExchangeName, //交换
			topic,                    //路由密钥
			!async,                   //强制
			false,                    //立即
			amqp.Publishing{
				Headers:     headers,
				ContentType: bus.options.Serialize.ContentType(),
				Body:        data,
			})
	})
}

func (bus *rabbitMQEventBus) Subscribe(ctx context.Context, topic string, h SubscribeHandler) (err error) {
	return bus.subscribe(ctx, topic, h, false)
}

func (bus *rabbitMQEventBus) SubscribeAsync(ctx context.Context, topic string, h SubscribeHandler) (err error) {
	return bus.subscribe(ctx, topic, h, true)
}

func (bus *rabbitMQEventBus) subscribe(ctx context.Context, topic string, h SubscribeHandler, async bool) (err error) {
	var ch *rabbitmq.Channel
	if ch, err = bus.getChannel(ctx); err != nil {
		return
	}
	queueName := fmt.Sprintf("%s.default.group.%s", topic, Version)
	if gid, ok := FromGroupIDContext(ctx); ok {
		queueName = fmt.Sprintf("%s-%s.group.%s", topic, gid, Version)
	}
	var queue amqp.Queue
	// 一对多要生产不同的queue，根据groupID来区分
	queue, err = bus.queueDeclare(ch, queueName)
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
	// 公平分配，每个消费者一次取一个
	err = ch.Qos(1, 0, false)
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
		go func(subCtx context.Context, subCh *rabbitmq.Channel) {
			if asyncErr := bus.handleSubMessage(subCtx, msgs, h); asyncErr != nil {
				bus.options.Logger.Errorf(subCtx, "async subscribe %s error: %v", topic, asyncErr)
			}
			bus.putChannel(subCh)
		}(ctx, ch)
	} else {
		err = bus.handleSubMessage(ctx, msgs, h)
		bus.putChannel(ch)
	}
	return
}

func (bus *rabbitMQEventBus) handleSubMessage(ctx context.Context, msgs <-chan amqp.Delivery, h SubscribeHandler) (err error) {
	for {
		select {
		case msg := <-msgs:
			if len(msg.Body) == 0 {
				// 空消息需要确认，避免重复投递
				if ackErr := msg.Ack(false); ackErr != nil {
					bus.options.Logger.Errorf(ctx, "failed to ack empty message: %v", ackErr)
				}
				continue
			}

			m := Message{
				Header: make(MessageHeader),
			}
			for k, v := range msg.Headers {
				m.Header[k] = fmt.Sprint(v)
			}

			bus.options.Logger.Debugf(ctx, "subscribe msg data: %s", string(msg.Body))

			// 反序列化消息
			if unmarshalErr := bus.options.Serialize.Unmarshal(msg.Body, &m.Value); unmarshalErr != nil {
				bus.options.Logger.Errorf(ctx, "failed to unmarshal message: %v", unmarshalErr)
				if bus.options.SkipBadMessages {
					// 跳过无法反序列化的消息
					if ackErr := msg.Ack(false); ackErr != nil {
						bus.options.Logger.Errorf(ctx, "failed to ack bad message: %v", ackErr)
					}
					continue
				}
				return unmarshalErr
			}

			// 获取原始topic
			originalTopic := msg.RoutingKey
			if topicFromHeader, exists := m.Header["topic"]; exists {
				originalTopic = topicFromHeader
			}

			// 获取重试次数
			retryCount := 0
			if retryHeader, exists := m.Header["x-retry-count"]; exists {
				if count, parseErr := fmt.Sscanf(retryHeader, "%d", &retryCount); parseErr != nil || count != 1 {
					retryCount = 0
				}
			}

			// 处理消息
			handlerErr := h(ctx, &m)

			if handlerErr == nil {
				// 成功处理，确认消息
				if ackErr := msg.Ack(false); ackErr != nil {
					bus.options.Logger.Errorf(ctx, "failed to ack message: %v", ackErr)
					if !bus.options.SkipBadMessages {
						return ackErr
					}
				}
			} else {
				bus.options.Logger.Errorf(ctx, "message handler error: %v", handlerErr)

				// 检查是否可以重试
				if retryCount < bus.options.MessageMaxRetries {
					// 增加重试次数并重新发布消息
					newRetryCount := retryCount + 1
					bus.options.Logger.Debugf(ctx, "republishing message for retry %d/%d", newRetryCount, bus.options.MessageMaxRetries)

					// 先确认原消息
					if ackErr := msg.Ack(false); ackErr != nil {
						bus.options.Logger.Errorf(ctx, "failed to ack message before retry: %v", ackErr)
						if !bus.options.SkipBadMessages {
							return ackErr
						}
						continue
					}

					// 重新发布带有更新后 retry count 的消息
					if republishErr := bus.republishForRetry(ctx, msg, newRetryCount); republishErr != nil {
						bus.options.Logger.Errorf(ctx, "failed to republish message for retry: %v", republishErr)
						// 发送到死信队列
						bus.sendToDeadLetter(ctx, originalTopic, &m, fmt.Sprintf("republish failed: %v, original error: %v", republishErr, handlerErr))
					}
				} else {
					// 达到最大重试次数，发送到死信队列
					bus.options.Logger.Errorf(ctx, "message exceeded max retries (%d), sending to dead letter queue", bus.options.MessageMaxRetries)
					bus.sendToDeadLetter(ctx, originalTopic, &m, handlerErr.Error())

					// 确认消息（避免重复处理）
					if ackErr := msg.Ack(false); ackErr != nil {
						bus.options.Logger.Errorf(ctx, "failed to ack failed message: %v", ackErr)
						if !bus.options.SkipBadMessages {
							return ackErr
						}
					}
				}

				// 继续处理下一条消息，而不是返回错误中断订阅
				if !bus.options.SkipBadMessages && retryCount >= bus.options.MessageMaxRetries {
					// 如果不跳过坏消息且已达到最大重试次数，则中断订阅
					return handlerErr
				}
			}
		case <-ctx.Done():
			bus.options.Logger.Debugf(ctx, "subscription context cancelled")
			return ctx.Err()
		}
	}
}
