# API 文档

## 核心接口

### EventBus

```go
type EventBus interface {
    Publisher
    Subscriber
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
