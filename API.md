# API 文档

## 核心接口

### EventBus

```go
type EventBus interface {
    Publisher
    Subscriber
}
```

### Closer

```go
// Closer 定义了可以关闭资源的接口
// 某些 EventBus 实现（如 RabbitMQ）需要清理内部资源（如连接池）
// 可以通过类型断言使用此接口: if closer, ok := bus.(eventbus.Closer); ok { closer.Close() }
type Closer interface {
    Close() error
}
```

### Publisher

```go
type Publisher interface {
    // Publish 同步发布消息
    Publish(ctx context.Context, topic string, v interface{}) error
    
    // PublishAsync 异步发布消息
    PublishAsync(ctx context.Context, topic string, v interface{}) error
}
```

### Subscriber

```go
type Subscriber interface {
    // Subscribe 同步订阅消息
    Subscribe(ctx context.Context, topic string, h SubscribeHandler) error
    
    // SubscribeAsync 异步订阅消息
    SubscribeAsync(ctx context.Context, topic string, h SubscribeHandler) error
}
```

### SubscribeHandler

```go
type SubscribeHandler func(ctx context.Context, msg *Message) error
```

## 消息结构

### Message

```go
type Message struct {
    Header MessageHeader `json:"header"` // 消息头
    Value  interface{}   `json:"value"`  // 消息内容
}
```

### MessageHeader

```go
type MessageHeader map[string]string
```

## 上下文函数

### GroupID 相关

```go
// NewGroupIDContext 创建带消费组ID的上下文
func NewGroupIDContext(parent context.Context, groupID string) context.Context

// FromGroupIDContext 从上下文中获取消费组ID
func FromGroupIDContext(ctx context.Context) (groupID string, ok bool)
```

### 消息头相关

```go
// SetMessageHeader 设置消息头的函数类型
type SetMessageHeader func(ctx context.Context) MessageHeader

// NewSetMessageHeaderContext 创建带消息头设置函数的上下文
func NewSetMessageHeaderContext(ctx context.Context, f SetMessageHeader) context.Context

// FromSetMessageHeaderContext 从上下文中获取消息头设置函数
func FromSetMessageHeaderContext(ctx context.Context) (f SetMessageHeader, ok bool)
```

## 实现类型

### Redis Streams

```go
// NewRedis 创建Redis Streams事件总线
func NewRedis(conn *redis.Client, options ...*RedisOptions) (EventBus, error)

type RedisOptions struct {
    ReadCount int64         // 每次读取的消息数量
    ReadBlock time.Duration // 阻塞读取超时时间
    Serialize Serializer    // 序列化器
    Logger    Logger        // 日志器
}
```

### Redis Queue

```go
// NewRedisQueue 创建Redis Queue事件总线
func NewRedisQueue(conn *redis.Client, options ...*RedisQueueOptions) (EventBus, error)

type RedisQueueOptions struct {
    PollInterval time.Duration // 轮询间隔
    Serialize    Serializer    // 序列化器
    Logger       Logger        // 日志器
}
```

Redis Queue 额外方法：

```go
// RedistributeMessages 重新分发消息（用于消费组负载均衡）
func (bus *redisQueueEventBus) RedistributeMessages(ctx context.Context, fromTopic, toTopic string, count int64) error

// GetQueueLength 获取队列长度
func (bus *redisQueueEventBus) GetQueueLength(ctx context.Context, queueName string) (int64, error)

// PeekQueue 查看队列中的消息而不移除
func (bus *redisQueueEventBus) PeekQueue(ctx context.Context, queueName string, start, stop int64) ([]string, error)
```

### RabbitMQ

```go
// NewRabbitMQ 创建RabbitMQ事件总线
func NewRabbitMQ(conn *amqp.Connection, options ...*RabbitMQOptions) (EventBus, error)

type RabbitMQOptions struct {
    Serialize Serializer // 序列化器
    Logger    Logger     // 日志器
}
```

## 序列化器接口

```go
type Serializer interface {
    Unmarshal(data []byte, msg interface{}) error
    Marshal(msg interface{}) ([]byte, error)
    ContentType() string
}
```

内置实现：

```go
type JSONSerialize struct{}
```

## 日志器接口

```go
type Logger interface {
    Debugf(ctx context.Context, format string, args ...interface{})
    Debugln(ctx context.Context, args ...interface{})
    Infof(ctx context.Context, format string, args ...interface{})
    Infoln(ctx context.Context, args ...interface{})
    Warnf(ctx context.Context, format string, args ...interface{})
    Warnln(ctx context.Context, args ...interface{})
    Warningf(ctx context.Context, format string, args ...interface{})
    Warningln(ctx context.Context, args ...interface{})
    Errorf(ctx context.Context, format string, args ...interface{})
    Errorln(ctx context.Context, args ...interface{})
}
```

内置实现：

```go
type StdLogger struct{}    // 标准库日志
type LogrusLogger struct{} // Logrus日志
type ZapLogger struct{}    // Zap日志
type ZlogLogger struct{}   // Nilorg Zlog日志
```

## 使用示例

### 完整示例

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/go-redis/redis/v8"
    "github.com/nilorg/eventbus"
)

func main() {
    // 1. 创建Redis客户端
    client := redis.NewClient(&redis.Options{
        Addr: "localhost:6379",
        DB:   0,
    })
    defer client.Close()

    // 2. 创建事件总线
    bus, err := eventbus.NewRedisQueue(client, &eventbus.RedisQueueOptions{
        PollInterval: time.Second,
        Serialize:    &eventbus.JSONSerialize{},
        Logger:       &eventbus.StdLogger{},
    })
    if err != nil {
        log.Fatal(err)
    }

    ctx := context.Background()

    // 3. 订阅消息
    go func() {
        // 创建消费组上下文
        groupCtx := eventbus.NewGroupIDContext(ctx, "worker_group")
        
        err := bus.Subscribe(groupCtx, "tasks", func(ctx context.Context, msg *eventbus.Message) error {
            log.Printf("处理任务: %+v", msg.Value)
            return nil
        })
        if err != nil {
            log.Printf("订阅失败: %v", err)
        }
    }()

    // 4. 发布消息
    for i := 0; i < 10; i++ {
        // 创建带消息头的上下文
        headerCtx := eventbus.NewSetMessageHeaderContext(ctx, func(ctx context.Context) eventbus.MessageHeader {
            return eventbus.MessageHeader{
                "source":    "task_scheduler",
                "timestamp": time.Now().Format(time.RFC3339),
                "task_id":   fmt.Sprintf("task_%d", i),
            }
        })

        // 创建消费组上下文
        groupCtx := eventbus.NewGroupIDContext(headerCtx, "worker_group")

        task := map[string]interface{}{
            "id":   i,
            "type": "data_processing",
            "data": fmt.Sprintf("data_%d", i),
        }

        err := bus.PublishAsync(groupCtx, "tasks", task)
        if err != nil {
            log.Printf("发布任务失败: %v", err)
        } else {
            log.Printf("发布任务 %d", i)
        }

        time.Sleep(500 * time.Millisecond)
    }

    // 等待处理完成
    time.Sleep(5 * time.Second)
}
```

### 资源清理

```go
// 创建事件总线
bus, err := eventbus.NewRabbitMQ(conn)
if err != nil {
    log.Fatal(err)
}

// 使用 defer 确保资源清理
defer func() {
    if closer, ok := bus.(eventbus.Closer); ok {
        if err := closer.Close(); err != nil {
            log.Printf("关闭事件总线失败: %v", err)
        }
    }
}()

// 使用事件总线...
```

### 错误处理

```go
// 订阅时的错误处理
err := bus.Subscribe(ctx, "events", func(ctx context.Context, msg *eventbus.Message) error {
    // 业务处理
    if err := processMessage(msg); err != nil {
        // 返回错误，消息处理失败
        return fmt.Errorf("处理消息失败: %w", err)
    }
    // 返回nil表示处理成功
    return nil
})
```

### 性能优化

```go
// 1. 使用异步模式
bus.PublishAsync(ctx, topic, data)
bus.SubscribeAsync(ctx, topic, handler)

// 2. 批量发布
for _, data := range batch {
    bus.PublishAsync(ctx, topic, data)
}

// 3. 合理配置参数
options := &eventbus.RedisQueueOptions{
    PollInterval: 100 * time.Millisecond, // 降低延迟
}
```

## NATS 实现

### NewNATS

创建基于NATS Core的事件总线，适用于高性能、低延迟的实时通信场景。

```go
func NewNATS(conn *nats.Conn, options ...*NATSOptions) (bus EventBus, err error)
```

**参数：**
- `conn`: NATS连接实例
- `options`: 可选配置参数

**示例：**

```go
// 连接NATS服务器
nc, err := nats.Connect("nats://localhost:4222")
if err != nil {
    return err
}

// 创建事件总线
bus, err := eventbus.NewNATS(nc)
if err != nil {
    return err
}

// 发布消息
err = bus.Publish(ctx, "user.created", userData)

// 订阅消息
err = bus.SubscribeAsync(ctx, "user.*", func(ctx context.Context, msg *eventbus.Message) error {
    // 处理消息
    return nil
})
```

### NewNATSJetStream

创建基于NATS JetStream的事件总线，支持消息持久化、确认机制，适用于可靠性要求高的场景。

```go
func NewNATSJetStream(conn *nats.Conn, options ...*NATSJetStreamOptions) (bus EventBus, err error)
```

**参数：**
- `conn`: NATS连接实例（需要支持JetStream）
- `options`: JetStream配置参数

**示例：**

```go
// 连接NATS服务器
nc, err := nats.Connect("nats://localhost:4222")
if err != nil {
    return err
}

// 配置JetStream选项
options := eventbus.DefaultNATSJetStreamOptions
options.StreamName = "ORDERS"
options.MaxMsgs = 10000
options.MaxAge = 24 * time.Hour

// 创建JetStream事件总线
bus, err := eventbus.NewNATSJetStream(nc, &options)
if err != nil {
    return err
}

// 发布消息（会被持久化）
err = bus.Publish(ctx, "order.created", orderData)

// 队列组订阅（负载均衡）
groupCtx := eventbus.NewGroupIDContext(ctx, "order-processors")
err = bus.SubscribeAsync(groupCtx, "order.*", func(ctx context.Context, msg *eventbus.Message) error {
    // 处理订单事件
    return nil
})
```

### NATSOptions

NATS Core配置选项。

```go
type NATSOptions struct {
    Serialize         Serializer    // 序列化器，默认JSON
    Logger            Logger        // 日志器
    MaxRetries        int           // 发布重试次数，默认3
    RetryInterval     time.Duration // 重试间隔，默认2秒
    BackoffMultiplier float64       // 退避倍数，默认2.0
    MaxBackoff        time.Duration // 最大退避时间，默认1分钟
    MessageMaxRetries int           // 消息处理重试次数，默认3
    SkipBadMessages   bool          // 跳过无法序列化的消息，默认true
    DeadLetterSubject string        // 死信队列主题
    QueueGroup        string        // 默认队列组名
}
```

### NATSJetStreamOptions

NATS JetStream配置选项，继承NATSOptions的所有配置。

```go
type NATSJetStreamOptions struct {
    // 基础选项（继承自NATSOptions）
    Serialize         Serializer
    Logger            Logger
    MaxRetries        int
    RetryInterval     time.Duration
    BackoffMultiplier float64
    MaxBackoff        time.Duration
    MessageMaxRetries int
    SkipBadMessages   bool
    DeadLetterSubject string
    QueueGroup        string
    
    // JetStream特有配置
    StreamName      string        // 流名称，默认"EVENTBUS"
    MaxMsgs         int64         // 流最大消息数，默认1000000
    MaxAge          time.Duration // 消息最大保存时间，默认24小时
    DuplicateWindow time.Duration // 去重窗口，默认2分钟
    Replicas        int           // 副本数，默认1
    AckWait         time.Duration // 确认等待时间，默认30秒
    MaxDeliver      int           // 最大投递次数，默认3
}
```

### 队列组支持

NATS实现支持队列组功能，实现消费者负载均衡：

```go
// 创建队列组上下文
groupCtx := eventbus.NewGroupIDContext(ctx, "my-service-group")

// 同一队列组的多个消费者会进行负载均衡
err = bus.SubscribeAsync(groupCtx, "events.*", handler1) // 消费者1
err = bus.SubscribeAsync(groupCtx, "events.*", handler2) // 消费者2

// 不同队列组的消费者会收到所有消息
otherGroupCtx := eventbus.NewGroupIDContext(ctx, "other-service-group")
err = bus.SubscribeAsync(otherGroupCtx, "events.*", handler3) // 独立消费者
```

### 主题匹配

NATS支持主题通配符：

```go
// 精确匹配
bus.Subscribe(ctx, "user.created", handler)

// 单级通配符 *
bus.Subscribe(ctx, "user.*", handler) // 匹配 user.created, user.updated 等

// 多级通配符 >
bus.Subscribe(ctx, "events.>", handler) // 匹配 events.user.created, events.order.paid 等
```

### 消息头支持

```go
// 设置消息头
headerCtx := eventbus.NewSetMessageHeaderContext(ctx, func(ctx context.Context) eventbus.MessageHeader {
    return eventbus.MessageHeader{
        "message-id": generateUniqueID(),
        "source":     "user-service",
        "timestamp":  fmt.Sprintf("%d", time.Now().Unix()),
    }
})

// 发布带头信息的消息
err = bus.Publish(headerCtx, "user.created", userData)
```

### 错误处理和重试

NATS实现提供多层次的错误处理：

1. **发布重试**：发布失败时自动重试
2. **消息处理重试**：消息处理失败时重试
3. **死信队列**：重试失败的消息发送到死信队列

```go
options := eventbus.NATSOptions{
    MaxRetries:        5,                    // 发布重试5次
    RetryInterval:     time.Second,          // 初始重试间隔1秒
    BackoffMultiplier: 1.5,                  // 每次重试间隔增加50%
    MaxBackoff:        time.Minute,          // 最大重试间隔1分钟
    MessageMaxRetries: 3,                    // 消息处理重试3次
    DeadLetterSubject: "failed.messages",    // 死信队列
}
```

### JetStream特性

JetStream提供的额外特性：

**消息持久化**：
```go
// 消息会被持久化到磁盘，服务重启后仍可消费
err = bus.Publish(ctx, "important.event", data)
```

**消息确认**：
```go
// 消息处理成功后自动确认，失败时会重新投递
err = bus.SubscribeAsync(ctx, "events.*", func(ctx context.Context, msg *eventbus.Message) error {
    if err := processMessage(msg); err != nil {
        return err // 返回错误，消息会重新投递
    }
    return nil // 返回nil，消息被确认
})
```

**去重机制**：
```go
// 使用消息ID实现去重
headerCtx := eventbus.NewSetMessageHeaderContext(ctx, func(ctx context.Context) eventbus.MessageHeader {
    return eventbus.MessageHeader{
        "message-id": "unique-id-123", // 相同ID的消息会被去重
    }
})
```

### 性能优化

```go
// 1. 使用异步模式提高吞吐量
bus.PublishAsync(ctx, topic, data)
bus.SubscribeAsync(ctx, topic, handler)

// 2. 合理配置JetStream参数
options := &eventbus.NATSJetStreamOptions{
    MaxMsgs:     100000,             // 根据业务调整流大小
    MaxAge:      time.Hour,          // 消息保留时间
    AckWait:     10 * time.Second,   // 确认等待时间
    MaxDeliver:  5,                  // 最大投递次数
}

// 3. 使用队列组实现水平扩展
groupCtx := eventbus.NewGroupIDContext(ctx, "worker-group")
for i := 0; i < workerCount; i++ {
    bus.SubscribeAsync(groupCtx, "work.tasks", processTask)
}
```
