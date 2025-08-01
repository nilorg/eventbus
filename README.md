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

🚀 **核心功能**
- 同步/异步消息发布订阅
- 消费组支持
- 消息头自定义
- 可插拔序列化器
- 多种日志器支持
- 优雅的错误处理

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

### 自定义配置

#### Redis Streams 配置

```go
options := &eventbus.RedisOptions{
    ReadCount: 10,                          // 每次读取消息数量
    ReadBlock: time.Second * 5,            // 阻塞读取超时
    Serialize: &eventbus.JSONSerialize{},   // 序列化器
    Logger:    &eventbus.StdLogger{},       // 日志器
}

bus, err := eventbus.NewRedis(client, options)
```

#### Redis Queue 配置

```go
options := &eventbus.RedisQueueOptions{
    PollInterval: time.Second * 2,          // 轮询间隔
    Serialize:    &eventbus.JSONSerialize{}, // 序列化器
    Logger:       &eventbus.StdLogger{},     // 日志器
}

bus, err := eventbus.NewRedisQueue(client, options)
```

## 后端对比

| 特性 | Redis Streams | Redis Queue | RabbitMQ |
|------|--------------|-------------|----------|
| 性能 | 高 | 很高 | 中等 |
| 功能丰富度 | 中等 | 简单 | 很高 |
| 消息持久化 | ✅ | ❌ | ✅ |
| 消息确认 | ✅ | ❌ | ✅ |
| 消费组 | ✅ | 模拟 | ✅ |
| 历史回放 | ✅ | ❌ | ❌ |
| 适用场景 | 流式处理 | 简单队列 | 企业应用 |

### 选择建议

- **Redis Streams**: 适合需要消息持久化和历史回放的场景
- **Redis Queue**: 适合对性能要求高、消息处理简单的场景  
- **RabbitMQ**: 适合企业级应用，需要复杂路由和可靠性保证

## 项目结构

```
eventbus/
├── eventbus.go          # 核心接口定义
├── message.go           # 消息结构
├── context.go           # 上下文处理
├── serializer.go        # 序列化器
├── logger.go            # 日志器
├── redis.go             # Redis Streams 实现
├── redisqueue.go        # Redis Queue 实现
├── rabbitmq.go          # RabbitMQ 实现
├── examples/            # 示例代码
│   ├── redis/
│   ├── redisqueue/
│   └── rabbitmq/
└── *_test.go            # 测试文件
```

## 示例程序

### 运行 Redis Streams 示例

```bash
cd examples/redis
go run main.go
```

### 运行 Redis Queue 示例

```bash
cd examples/redisqueue  
go run main.go
```

### 运行 RabbitMQ 示例

```bash
cd examples/rabbitmq
go run main.go
```

## 依赖

```go
require (
    github.com/go-redis/redis/v8 v8.11.5
    github.com/streadway/amqp v1.0.0
    github.com/sirupsen/logrus v1.8.1
    go.uber.org/zap v1.24.0
    github.com/nilorg/pkg v0.0.0-20221209071251-2bf5826d6883
)
```

## 文档

- [API 文档](API.md) - 详细的 API 接口说明
- [Redis Queue 详细文档](REDISQUEUE.md) - Redis Queue 实现的详细说明
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