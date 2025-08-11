# EventBus

[![Go](https://img.shields.io/badge/Go-1.14+-00ADD8?style=flat&logo=go)](https://golang.org)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/nilorg/eventbus)](https://goreportcard.com/report/github.com/nilorg/eventbus)

EventBus 是一个高性能、可扩展的 Go 语言事件总线库，支持多种消息中间件后端。

## 特性

✨ **多种后端支持**
- ✅ RabbitMQ - 企业级消息队列
- ✅ Redis Streams - 高性能流式处理
- ✅ Redis Queue (List) - 轻量级队列实现
- ✅ NATS Core - 高性能云原生消息系统
- ✅ NATS JetStream - 持久化消息流平台

🚀 **核心功能**
- 同步/异步消息发布订阅
- 消费组支持
- 消息头自定义
- 可插拔序列化器
- 多种日志器支持
- 优雅的错误处理
- 智能重试机制
- 统一死信队列支持
- 消息级别重试
- 连接自动恢复
- 队列长度限制
- 消息TTL管理

🛠 **易于使用**
- 统一的 API 接口
- 丰富的配置选项
- 完整的示例代码
- 详细的文档说明

## 快速开始

### 安装

```bash
go get github.com/nilorg/eventbus
```

### 基本用法

#### Redis Streams

```go
package main

import (
    "context"
    "log"
    
    "github.com/go-redis/redis/v8"
    "github.com/nilorg/eventbus"
)

func main() {
    // 连接Redis
    client := redis.NewClient(&redis.Options{
        Addr: "localhost:6379",
    })
    defer client.Close()

    // 创建事件总线
    bus, err := eventbus.NewRedis(client)
    if err != nil {
        log.Fatal(err)
    }

    ctx := context.Background()
    topic := "user_events"

    // 订阅消息
    go func() {
        bus.Subscribe(ctx, topic, func(ctx context.Context, msg *eventbus.Message) error {
            log.Printf("收到消息: %+v", msg.Value)
            return nil
        })
    }()

    // 发布消息
    err = bus.Publish(ctx, topic, map[string]interface{}{
        "user_id": 123,
        "action":  "login",
    })
    if err != nil {
        log.Fatal(err)
    }
}
```

#### Redis Queue

```go
// 创建 Redis Queue 事件总线
bus, err := eventbus.NewRedisQueue(client)
if err != nil {
    log.Fatal(err)
}

// 使用方式与 Redis Streams 相同
```

#### NATS Core

```go
package main

import (
    "context"
    "log"
    
    "github.com/nats-io/nats.go"
    "github.com/nilorg/eventbus"
)

func main() {
    // 连接NATS
    nc, err := nats.Connect(nats.DefaultURL)
    if err != nil {
        log.Fatal(err)
    }
    defer nc.Close()

    // 创建NATS事件总线
    bus, err := eventbus.NewNATS(nc)
    if err != nil {
        log.Fatal(err)
    }

    ctx := context.Background()
    topic := "user.events"

    // 订阅消息
    go func() {
        bus.Subscribe(ctx, topic, func(ctx context.Context, msg *eventbus.Message) error {
            log.Printf("NATS收到消息: %+v", msg.Value)
            return nil
        })
    }()

    // 发布消息
    err = bus.Publish(ctx, topic, map[string]interface{}{
        "user_id": 123,
        "action":  "login",
    })
    if err != nil {
        log.Fatal(err)
    }
}
```

#### NATS JetStream

```go
// 创建NATS JetStream事件总线，支持持久化
options := &eventbus.NATSJetStreamOptions{
    StreamName:    "USER_EVENTS",
    Subjects:      []string{"user.>"},
    Storage:       nats.FileStorage,
    MaxMsgs:       1000000,
    MaxBytes:      1024 * 1024 * 1024, // 1GB
    MaxAge:        time.Hour * 24 * 7,  // 7天
    Replicas:      1,
}

bus, err := eventbus.NewNATSJetStream(nc, options)
if err != nil {
    log.Fatal(err)
}

#### RabbitMQ

```go
import "github.com/streadway/amqp"

// 连接RabbitMQ
conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
if err != nil {
    log.Fatal(err)
}
defer conn.Close()

// 创建事件总线
bus, err := eventbus.NewRabbitMQ(conn)
if err != nil {
    log.Fatal(err)
}
```

## 高级配置

### 死信队列配置

EventBus 支持统一的死信队列处理，当消息处理失败时自动发送到死信队列：

```go
// Redis Streams 死信配置
redisOptions := &eventbus.RedisOptions{
    DeadLetterTopic:   "user_events_dlq",    // 死信队列主题
    DeadLetterMaxLen:  1000,                 // 死信队列最大长度
    DeadLetterTTL:     time.Hour * 24,       // 死信消息TTL
    MessageMaxRetries: 3,                    // 消息最大重试次数
    SkipBadMessages:   true,                 // 跳过无法处理的消息
}

bus, err := eventbus.NewRedis(client, redisOptions)

// Redis Queue 死信配置
queueOptions := &eventbus.RedisQueueOptions{
    DeadLetterTopic:   "user_queue_dlq",
    DeadLetterMaxLen:  1000,
    DeadLetterTTL:     time.Hour * 24,
    MessageMaxRetries: 3,
    SkipBadMessages:   true,
}

bus, err := eventbus.NewRedisQueue(client, queueOptions)

// NATS 死信配置
natsOptions := &eventbus.NATSOptions{
    DeadLetterSubject: "user.events.dlq",
    MessageMaxRetries: 3,
    SkipBadMessages:   true,
}

bus, err := eventbus.NewNATS(nc, natsOptions)
```

### 处理死信消息

```go
// 监听死信队列
err := bus.Subscribe(ctx, "user_events_dlq", func(ctx context.Context, msg *eventbus.Message) error {
    // 死信消息使用统一的 dlq_ 前缀格式
    if data, ok := msg.Value.(map[string]interface{}); ok {
        originalTopic := data["dlq_original_topic"]
        originalID := data["dlq_original_id"]
        errorReason := data["dlq_error_reason"]
        failedAt := data["dlq_failed_at"]
        originalValues := data["dlq_original_values"]
        
        log.Printf("处理死信: topic=%v, id=%v, error=%v, time=%v", 
                   originalTopic, originalID, errorReason, failedAt)
        
        // 进行告警、重新处理或记录
        return handleDeadLetterMessage(originalValues)
    }
    return nil
})
```

## 高级用法

### 消费组

```go
// 创建带消费组的上下文
groupCtx := eventbus.NewGroupIDContext(ctx, "payment_service")

// 订阅消息
bus.Subscribe(groupCtx, "order_events", handler)

// 发布到特定消费组
bus.Publish(groupCtx, "order_events", orderData)
```

### 消息头

```go
// 设置消息头
headerCtx := eventbus.NewSetMessageHeaderContext(ctx, func(ctx context.Context) eventbus.MessageHeader {
    return eventbus.MessageHeader{
        "source":    "user_service",
        "timestamp": time.Now().Format(time.RFC3339),
        "version":   "v1.0",
    }
})

bus.Publish(headerCtx, "events", data)
```

### 错误处理和重试

EventBus 提供了强大的错误处理和重试机制：

```go
// 配置重试选项
options := &eventbus.RedisOptions{
    MaxRetries:        3,                   // 连接失败最大重试次数
    RetryInterval:     time.Second * 2,     // 重试间隔
    BackoffMultiplier: 2.0,                 // 指数退避倍数
    MaxBackoff:        time.Minute,         // 最大退避时间
    MessageMaxRetries: 2,                   // 单条消息最大重试次数
    SkipBadMessages:   true,                // 跳过无法处理的消息
}

// 错误处理示例
bus.Subscribe(ctx, "events", func(ctx context.Context, msg *eventbus.Message) error {
    // 处理消息
    if err := processMessage(msg); err != nil {
        // 错误会自动重试，达到最大次数后进入死信队列
        return fmt.Errorf("处理失败: %w", err)
    }
    return nil
})
```

### 死信队列

当消息处理失败达到最大重试次数时，会自动发送到死信队列：

```go
// 启用死信队列
options := &eventbus.RedisOptions{
    DeadLetterTopic: "my_app.dlq",  // 设置死信队列主题
    // ... 其他配置
}

// 订阅死信队列处理失败消息
bus.Subscribe(ctx, "my_app.dlq", func(ctx context.Context, msg *eventbus.Message) error {
    // 死信消息使用统一的 dlq_ 前缀格式
    if data, ok := msg.Value.(map[string]interface{}); ok {
        log.Printf("失败消息: ID=%v, 主题=%v, 错误=%v", 
            data["dlq_original_id"], 
            data["dlq_original_topic"], 
            data["dlq_error_reason"])
        
        // 可以实现告警、重新处理等逻辑
        return handleFailedMessage(data)
    }
    return nil
})
```

### 自定义配置

#### Redis Streams 配置

```go
options := &eventbus.RedisOptions{
    ReadCount: 10,                          // 每次读取消息数量
    ReadBlock: time.Second * 5,            // 阻塞读取超时
    Serialize: &eventbus.JSONSerialize{},   // 序列化器
    Logger:    &eventbus.StdLogger{},       // 日志器
    
    // 重试机制配置
    MaxRetries:        3,                   // 连接最大重试次数
    RetryInterval:     time.Second * 5,     // 重试间隔
    BackoffMultiplier: 2.0,                 // 指数退避倍数
    MaxBackoff:        time.Minute * 5,     // 最大退避时间
    MessageMaxRetries: 3,                   // 消息最大重试次数
    SkipBadMessages:   true,                // 跳过无法处理的消息
    DeadLetterTopic:   "my_app.dlq",        // 死信队列主题（可选）
}

bus, err := eventbus.NewRedis(client, options)
```

#### Redis Queue 配置

```go
options := &eventbus.RedisQueueOptions{
    PollInterval: time.Second * 2,          // 轮询间隔
    Serialize:    &eventbus.JSONSerialize{}, // 序列化器
    Logger:       &eventbus.StdLogger{},     // 日志器
    
    // 重试机制配置
    MaxRetries:        3,                   // 连接最大重试次数
    RetryInterval:     time.Second * 5,     // 重试间隔
    BackoffMultiplier: 2.0,                 // 指数退避倍数
    MaxBackoff:        time.Minute * 5,     // 最大退避时间
    MessageMaxRetries: 3,                   // 消息最大重试次数
    SkipBadMessages:   true,                // 跳过无法处理的消息
    DeadLetterTopic:   "my_app.dlq",        // 死信队列主题（可选）
}

bus, err := eventbus.NewRedisQueue(client, options)
```

#### NATS Core 配置

```go
options := &eventbus.NATSOptions{
    Serialize:         &eventbus.JSONSerialize{}, // 序列化器
    Logger:            &eventbus.StdLogger{},     // 日志器
    
    // 重试机制配置
    MaxRetries:        3,                   // 连接最大重试次数
    RetryInterval:     time.Second * 5,     // 重试间隔
    BackoffMultiplier: 2.0,                 // 指数退避倍数
    MaxBackoff:        time.Minute * 5,     // 最大退避时间
    MessageMaxRetries: 3,                   // 消息最大重试次数
    SkipBadMessages:   true,                // 跳过无法处理的消息
    DeadLetterSubject: "my_app.dlq",        // 死信主题（可选）
}

bus, err := eventbus.NewNATS(nc, options)
```

#### NATS JetStream 配置

```go
options := &eventbus.NATSJetStreamOptions{
    StreamName:        "MY_STREAM",         // 流名称
    Subjects:          []string{"events.>"},// 主题模式
    Description:       "Event stream",      // 流描述
    Storage:           nats.FileStorage,    // 存储类型
    MaxMsgs:           1000000,             // 最大消息数
    MaxBytes:          1024 * 1024 * 1024,  // 最大字节数
    MaxAge:            time.Hour * 24 * 7,  // 消息最大保留时间
    MaxMsgSize:        1024 * 1024,         // 单个消息最大大小
    Replicas:          1,                   // 副本数
    
    Serialize:         &eventbus.JSONSerialize{}, // 序列化器
    Logger:            &eventbus.StdLogger{},     // 日志器
    
    // 重试机制配置
    MaxRetries:        3,                   // 连接最大重试次数
    RetryInterval:     time.Second * 5,     // 重试间隔
    BackoffMultiplier: 2.0,                 // 指数退避倍数
    MaxBackoff:        time.Minute * 5,     // 最大退避时间
    MessageMaxRetries: 3,                   // 消息最大重试次数
    SkipBadMessages:   true,                // 跳过无法处理的消息
    DeadLetterSubject: "my_app.dlq",        // 死信主题（可选）
}

bus, err := eventbus.NewNATSJetStream(nc, options)
```

#### RabbitMQ 配置

```go
options := &eventbus.RabbitMQOptions{
    ExchangeName:        "my_exchange",     // 交换机名称
    ExchangeType:        "topic",           // 交换机类型
    QueueMessageExpires: 864000000,         // 消息过期时间
    Serialize:           &eventbus.JSONSerialize{}, // 序列化器
    Logger:              &eventbus.StdLogger{},     // 日志器
    PoolMinOpen:         1,                 // 最小连接池大小
    PoolMaxOpen:         10,                // 最大连接池大小
    
    // 重试机制配置
    MaxRetries:         3,                  // 连接最大重试次数
    RetryInterval:      time.Second * 2,    // 重试间隔
    BackoffMultiplier:  2.0,                // 指数退避倍数
    MaxBackoff:         time.Minute,        // 最大退避时间
    MessageMaxRetries:  2,                  // 消息最大重试次数
    SkipBadMessages:    true,               // 跳过无法处理的消息
    DeadLetterExchange: "my_app.dlx",       // 死信交换机（可选）
}

bus, err := eventbus.NewRabbitMQ(conn, options)
```

## 后端对比

| 特性 | Redis Streams | Redis Queue | RabbitMQ | NATS Core | NATS JetStream |
|------|--------------|-------------|----------|-----------|----------------|
| 性能 | 高 | 很高 | 中等 | 很高 | 高 |
| 功能丰富度 | 中等 | 简单 | 很高 | 中等 | 高 |
| 消息持久化 | ✅ | ❌ | ✅ | ❌ | ✅ |
| 消息确认 | ✅ | ❌ | ✅ | ❌ | ✅ |
| 消费组 | ✅ | 模拟 | ✅ | ✅ | ✅ |
| 历史回放 | ✅ | ❌ | ❌ | ❌ | ✅ |
| 连接重试 | ✅ | ✅ | ✅ | ✅ | ✅ |
| 消息重试 | ✅ | ✅ | ✅ | ✅ | ✅ |
| 死信队列 | ✅ | ✅ | ✅ | ✅ | ✅ |
| 错误隔离 | ✅ | ✅ | ✅ | ✅ | ✅ |
| 云原生 | ❌ | ❌ | ❌ | ✅ | ✅ |
| 水平扩展 | 中等 | 中等 | 中等 | 很高 | 很高 |
| 适用场景 | 流式处理 | 简单队列 | 企业应用 | 微服务 | 数据流平台 |

### 选择建议

- **Redis Streams**: 适合需要消息持久化和历史回放的场景，具备完整的重试机制和死信队列支持
- **Redis Queue**: 适合对性能要求高、消息处理简单的场景，现已支持重试和死信队列
- **RabbitMQ**: 适合企业级应用，需要复杂路由和可靠性保证，具备全面的错误处理能力
- **NATS Core**: 适合云原生微服务架构，提供超高性能的消息传递
- **NATS JetStream**: 适合需要持久化的云原生数据流平台，支持流式处理和消息重放

### 🚀 新特性亮点

- **统一死信格式**: 所有后端使用统一的 `dlq_` 前缀死信消息格式，便于监控和处理
- **智能重试**: 连接失败时使用指数退避策略，避免雪崩效应
- **消息级重试**: 单条消息处理失败时精确重试，不影响其他消息
- **死信队列管理**: 支持死信队列长度限制和TTL设置
- **错误隔离**: 单个消息处理错误不会中断整个订阅循环
- **云原生支持**: 新增NATS Core和JetStream支持，适应现代微服务架构
- **统一体验**: 所有后端实现提供一致的重试和错误处理机制

## 项目结构

```
eventbus/
├── eventbus.go          # 核心接口定义
├── message.go           # 消息结构和死信处理
├── context.go           # 上下文处理
├── serializer.go        # 序列化器
├── logger.go            # 日志器
├── redis.go             # Redis Streams 实现
├── redisqueue.go        # Redis Queue 实现
├── nats.go              # NATS Core 实现
├── nats_jetstream.go    # NATS JetStream 实现
├── rabbitmq.go          # RabbitMQ 实现
├── examples/            # 示例代码
│   ├── redis/
│   ├── redisqueue/
│   └── rabbitmq/
└── *_test.go            # 测试文件
```

## 示例程序

### 基础示例

```bash
# Redis Streams 基础示例
cd examples/redis
go run main.go

# Redis Queue 基础示例
cd examples/redisqueue  
go run main.go

# RabbitMQ 基础示例
cd examples/rabbitmq
go run main.go

# NATS Core 示例
cd examples/nats
go run main.go

# NATS JetStream 示例
cd examples/nats_jetstream
go run main.go
```

### 重试机制示例

```bash
# Redis Streams 重试示例
cd examples/redis_retry
go run main.go

# Redis Queue 重试示例
cd examples/redis_queue_retry
go run main.go

# RabbitMQ 重试示例
cd examples/rabbitmq_retry
go run main.go

# 自定义消息头和死信队列示例
cd examples/custom_header
go run main.go

# Redis 死信处理优化示例
cd examples/redis_deadletter
go run main.go
```

## 依赖

```go
require (
    github.com/go-redis/redis/v8 v8.11.5
    github.com/streadway/amqp v1.0.0
    github.com/nats-io/nats.go v1.31.0
    github.com/rs/xid v1.5.0
    github.com/sirupsen/logrus v1.8.1
    go.uber.org/zap v1.24.0
    github.com/nilorg/pkg v0.0.0-20221209071251-2bf5826d6883
)
```

## 文档

- [API 文档](API.md) - 详细的 API 接口说明
- [示例代码](examples/) - 各种使用场景的示例

## 测试

```bash
# 运行所有测试
go test ./...

# 运行特定测试
go test -v ./redisqueue_test.go

# 基准测试
go test -bench=.
```

## 日志器支持

支持多种日志库：

- 标准库 `log`
- [logrus](https://github.com/sirupsen/logrus)
- [zap](https://github.com/uber-go/zap)
- [nilorg/pkg/zlog](https://github.com/nilorg/pkg)

## 序列化器

内置 JSON 序列化器，可扩展支持：

- JSON (默认)
- Protobuf
- MessagePack
- 自定义序列化器

## 贡献

欢迎提交 Pull Request 和 Issue！

1. Fork 项目
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 打开 Pull Request

## 许可证

本项目基于 MIT 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

## 相关项目

- [nilorg/pkg](https://github.com/nilorg/pkg) - Go 工具包
- [nilorg/sdk](https://github.com/nilorg/sdk) - Go SDK

---

如果这个项目对你有帮助，请给个 ⭐ Star 支持一下！