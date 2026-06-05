# 自定义序列化器示例

## 概述

本示例展示了如何实现一个支持**加密**和**压缩**的自定义序列化器，演示了 eventbus 序列化器的强大扩展能力。

## 功能特性

### 1. 数据压缩 (gzip)
- **压缩算法**: gzip
- **压缩级别**: 可配置 (DefaultCompression, BestSpeed, BestCompression)
- **效果**: 减少数据大小约 14% (实测：186字节 → 160字节)

### 2. 数据加密 (AES-GCM)
- **加密算法**: AES-GCM (Galois/Counter Mode)
- **密钥长度**: 支持 16/24/32 字节 (AES-128/192/256)
- **认证加密**: 提供数据完整性验证
- **Nonce**: 自动生成随机 nonce，防止重放攻击
- **开销**: 增加数据大小约 15% (实测：186字节 → 214字节)

### 3. 灵活的组合方式
支持三种配置：
- **仅压缩**: 减少传输数据量
- **仅加密**: 保护数据安全
- **压缩+加密**: 先压缩再加密，最佳组合

### 4. 多种基础序列化器
- **JSON**: 易于调试，兼容性好
- **MessagePack**: 性能更高，数据更小

## 实现架构

### 序列化流程 (Marshal)
```
用户数据 → BaseSerializer(JSON/MessagePack) → bytes
         ↓
      Compression(gzip) → compressed bytes
         ↓
      Encryption(AES-GCM) → encrypted bytes
         ↓
      最终数据
```

### 反序列化流程 (Unmarshal)
```
encrypted bytes → Decryption(AES-GCM) → compressed bytes
               ↓
            Decompression(gzip) → bytes
               ↓
            BaseSerializer(JSON/MessagePack) → 用户数据
```

## 测试结果

### Test 1: 仅压缩
- **原始大小**: 186 bytes
- **压缩后**: 160 bytes
- **压缩率**: 86.02% (减少 14%)
- **ContentType**: `application/custom-json+gzip`

### Test 2: 仅加密
- **原始大小**: 186 bytes
- **加密后**: 214 bytes
- **大小增加**: 15.05% (包含 nonce + auth tag)
- **ContentType**: `application/custom-json+encrypted`

### Test 3: 压缩+加密
- **原始大小**: 186 bytes
- **处理后**: 188 bytes
- **最终大小**: 101.08% (压缩效果抵消加密开销)
- **ContentType**: `application/custom-json+gzip+encrypted`

### 性能对比

| 配置方式 | 数据大小 | 性能影响 | 适用场景 |
|---------|---------|---------|---------|
| 仅压缩 | 最小 | CPU开销低 | 网络带宽受限 |
| 仅加密 | 较大 | CPU开销中 | 数据安全优先 |
| 压缩+加密 | 中等 | CPU开销高 | 安全+效率平衡 |

## 关键代码

### 创建自定义序列化器

```go
// 仅压缩
opts := &CustomSerializeOptions{
    Compression:      CompressionGzip,
    Encryption:       EncryptionNone,
    BaseSerializer:   BaseJSON,
    CompressionLevel: gzip.BestCompression,
}
serializer, _ := NewCustomSerialize(opts)

// 仅加密
opts := &CustomSerializeOptions{
    Compression:    CompressionNone,
    Encryption:     EncryptionAES,
    EncryptionKey:  key, // 32 bytes key
    BaseSerializer: BaseJSON,
}
serializer, _ := NewCustomSerialize(opts)

// 压缩+加密（推荐）
opts := &CustomSerializeOptions{
    Compression:      CompressionGzip,
    Encryption:       EncryptionAES,
    EncryptionKey:    key,
    BaseSerializer:   BaseJSON,
    CompressionLevel: gzip.DefaultCompression,
}
serializer, _ := NewCustomSerialize(opts)
```

### 使用自定义序列化器

```go
busOpts := &eventbus.RedisOptions{
    Serialize:         serializer,
    Logger:            &eventbus.StdLogger{},
    MessageMaxRetries: 3,
    SkipBadMessages:   true,
}

bus, _ := eventbus.NewRedis(rdb, busOpts)

// 发布加密消息
event := SecureEvent{
    UserID: "user_123",
    Secret: "SENSITIVE_DATA",
}
bus.Publish(ctx, "secure_topic", event)
```

## ContentType 标记

自定义序列化器使用特殊的 ContentType 标记：

- `application/custom-json` - 仅 JSON
- `application/custom-json+gzip` - JSON + gzip压缩
- `application/custom-json+encrypted` - JSON + AES加密
- `application/custom-json+gzip+encrypted` - JSON + gzip + AES
- `application/custom-msgpack+gzip+encrypted` - MessagePack + gzip + AES

## 重要说明

### AutoConverter 限制

**当前问题**: AutoConverter 不支持自定义 ContentType

测试输出中的错误：
```
[Error] failed to convert message: unsupported content type
```

**原因**: `converter.go` 中的 AutoConverter 只识别：
- `application/json`
- `application/x-protobuf`
- `application/x-msgpack`

不识别自定义的 `application/custom-*` ContentType。

### 解决方案

1. **直接使用自定义序列化器**
   - Producer 和 Consumer 使用相同的序列化器配置
   - 不依赖 AutoConverter
   
2. **实现自定义 Converter**
   ```go
   type CustomConverter struct {
       serializer *CustomSerialize
   }
   
   func (c *CustomConverter) Convert(data []byte, contentType string) (*Message, error) {
       if contentType == c.serializer.ContentType() {
           // 直接反序列化为 Message
           var msg Message
           err := c.serializer.Unmarshal(data, &msg)
           return &msg, err
       }
       return nil, ErrUnsupportedContentType
   }
   
   // 使用
   bus.Subscribe(ctx, topic, handler, WithConverter(&CustomConverter{serializer}))
   ```

3. **扩展 AutoConverter**
   - 在 converter.go 中添加自定义 ContentType 支持
   - 需要修改 eventbus 包（不适合本示例）

## 安全建议

### 密钥管理
- **不要硬编码密钥**
- 使用环境变量或密钥管理系统
- 定期轮换密钥
- 不同环境使用不同密钥

```go
// 推荐：从环境变量读取
key := os.Getenv("ENCRYPTION_KEY")
if len(key) != 32 {
    log.Fatal("Invalid encryption key length")
}
```

### 性能优化
- 压缩级别：`gzip.DefaultCompression` 平衡性能和压缩率
- 小数据不建议压缩（压缩开销 > 收益）
- 大数据建议压缩（显著减少传输量）

### 适用场景
- **压缩**: 大数据、网络带宽受限
- **加密**: 敏感数据、安全传输
- **组合**: 大数据且需要加密

## 扩展可能性

### 1. 其他加密算法
- AES-CBC (需要手动处理 padding)
- ChaCha20-Poly1305 (性能更好)
- RSA (非对称加密，用于密钥交换)

### 2. 其他压缩算法
- zlib (兼容 gzip)
- lz4 (更快的压缩)
- zstd (更好的压缩率)

### 3. 添加签名
```go
type SignedSerializer struct {
    serializer Serializer
    secretKey  []byte
}

func (s *SignedSerializer) Marshal(v interface{}) ([]byte, error) {
    data, err := s.serializer.Marshal(v)
    if err != nil {
        return nil, err
    }
    
    // HMAC 签名
    mac := hmac.New(sha256.New, s.secretKey)
    mac.Write(data)
    signature := mac.Sum(nil)
    
    // data + signature
    return append(data, signature...), nil
}
```

### 4. 添加校验
```go
// CRC32 校验和
checksum := crc32.ChecksumIEEE(data)
// 添加到数据末尾
```

## 总结

✅ **自定义序列化器成功演示**：
- 数据压缩 (gzip)
- 数据加密 (AES-GCM)
- 自定义序列化流程
- 灵活配置选项
- 多种组合方式

✅ **性能数据**：
- 压缩效率：14% 数据减少
- 加密开销：15% 数据增加
- 组合效果：平衡安全与效率

✅ **适用场景**：
- 敏感数据传输
- 大数据传输
- 自定义安全需求

⚠️ **注意事项**：
- AutoConverter 不支持自定义 ContentType
- 需要手动管理密钥
- Producer/Consumer 必须使用相同配置

## 示例文件

- `examples/redis_serialize_custom/custom_serializer.go` - 自定义序列化器实现
- `examples/redis_serialize_custom/main.go` - 测试示例
- 运行：`go run examples/redis_serialize_custom/*.go`