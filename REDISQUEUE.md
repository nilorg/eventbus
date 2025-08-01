# Redis Queue EventBus

这是基于 Redis List 数据结构实现的事件总线，提供了与现有 Redis Streams 实现相同的接口，但使用不同的底层机制。

## 特性

### 1. 核心功能
- **发布订阅**: 支持同步和异步的消息发布与订阅
- **消费组支持**: 通过 GroupID 实现消息分组消费
- **消息持久化**: 基于 Redis List，消息持久化存储
- **阻塞消费**: 使用 BRPOP 实现高效的阻塞式消费

### 2. 与 Redis Streams 的区别

| 特性 | Redis Streams | Redis Queue (List) |
|------|--------------|-------------------|
| 数据结构 | Stream | List |
| 消费方式 | XReadGroup | BRPOP |
| 消息顺序 | 严格有序 | FIFO 队列 |
| 消费组 | 内置支持 | 通过队列名称模拟 |
| 消息确认 | XAck | 自动确认 |
| 历史消息 | 支持重新消费 | 消费后移除 |
| 性能 | 适合复杂场景 | 更轻量级 |

### 3. 主要优势
- **轻量级**: 相比 Streams，List 操作更简单直接
- **高性能**: BRPOP/LPUSH 操作非常高效
- **简单可靠**: 消息消费后自动从队列移除，避免消息堆积
- **兼容性好**: 与现有 EventBus 接口完全兼容

## 使用方法

### 基本用法

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
        DB:   0,
    })
    defer client.Close()

    // 创建Redis Queue事件总线
    bus, err := eventbus.NewRedisQueue(client)
    if err != nil {
        log.Fatal(err)
    }

    ctx := context.Background()
    topic := "my_topic"

    // 订阅消息
    go func() {
        bus.Subscribe(ctx, topic, func(ctx context.Context, msg *eventbus.Message) error {
            log.Printf("Received: %+v", msg.Value)
            return nil
        })
    }()

    // 发布消息
    err = bus.Publish(ctx, topic, map[string]interface{}{
        "id":   1,
        "data": "hello world",
    })
    if err != nil {
        log.Fatal(err)
    }
}
```

### 消费组用法

```go
// 创建带GroupID的上下文
groupCtx := eventbus.NewGroupIDContext(ctx, "group1")

// 消费组订阅
bus.Subscribe(groupCtx, topic, handler)

// 发布到特定消费组
bus.Publish(groupCtx, topic, message)
```

### 配置选项

```go
options := &eventbus.RedisQueueOptions{
    PollInterval: time.Second,           // 轮询间隔
    Serialize:    &eventbus.JSONSerialize{}, // 序列化器
    Logger:       &eventbus.StdLogger{},     // 日志器
}

bus, err := eventbus.NewRedisQueue(client, options)
```

## 架构设计

### 1. 消息流转

```
发布者 -> LPUSH -> Redis List -> BRPOP -> 消费者
```

### 2. 消费组实现

- 默认队列: `{topic}`
- 消费组队列: `{topic}:group:{groupId}`

### 3. 消息结构

```go
type QueueMessage struct {
    ID      string                     `json:"id"`       // 消息ID
    Topic   string                     `json:"topic"`    // 主题
    Header  map[string]string          `json:"header"`   // 消息头
    Value   interface{}                `json:"value"`    // 消息内容
    GroupID string                     `json:"group_id"` // 消费组ID
}
```

## 高级功能

### 1. 消息重分发

```go
// 将消息从一个队列转移到另一个队列
err := bus.RedistributeMessages(ctx, "from_queue", "to_queue", 10)
```

### 2. 队列监控

```go
// 获取队列长度
length, err := bus.GetQueueLength(ctx, "my_topic")

// 查看队列中的消息
messages, err := bus.PeekQueue(ctx, "my_topic", 0, 9)
```

## 性能考虑

### 1. 优化建议
- 合理设置 `PollInterval` 以平衡性能和资源消耗
- 对于高并发场景，考虑使用连接池
- 监控队列长度，避免消息堆积

### 2. 性能对比
- 相比 Redis Streams，List 操作的延迟更低
- 内存使用更少，特别是在大量消息的场景下
- 适合对消息处理实时性要求较高的场景

## 注意事项

### 1. 消息可靠性
- 消息一旦被消费就会从队列中移除
- 如果消费者处理失败，消息会丢失
- 建议在业务层实现错误重试机制

### 2. 消费组限制
- 消费组通过队列名称区分，不是真正的消费组
- 同一消费组内的多个消费者会竞争消息
- 需要外部机制来保证消费组内的负载均衡

### 3. 扩展性
- 支持水平扩展，多个消费者可以并行处理
- 队列数量会随着消费组数量增加
- 需要定期清理无用的队列

## 示例程序

运行示例程序：

```bash
cd examples/redisqueue
go run main.go
```

示例程序展示了：
- 简单的发布订阅
- 消费组的使用
- 异步消息处理
- 优雅的关闭处理

## 适用场景

### 适合的场景
- 实时消息处理
- 简单的任务队列
- 轻量级的事件总线
- 对消息顺序要求不是特别严格的场景

### 不适合的场景
- 需要消息持久化和重播的场景
- 复杂的消息路由需求
- 需要严格消息确认机制的场景
