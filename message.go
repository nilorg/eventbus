package eventbus

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/xid"
)

// MessageHeader message header
type MessageHeader map[string]string

// SetMessageHeader set message header func
type SetMessageHeader func(ctx context.Context) MessageHeader

type setMessageHeaderKey struct{}

// NewSetMessageHeaderContext ...
func NewSetMessageHeaderContext(ctx context.Context, f SetMessageHeader) context.Context {
	return context.WithValue(ctx, setMessageHeaderKey{}, f)
}

// FromSetMessageHeaderContext ...
func FromSetMessageHeaderContext(ctx context.Context) (f SetMessageHeader, ok bool) {
	f, ok = ctx.Value(setMessageHeaderKey{}).(SetMessageHeader)
	return
}

// Message 消息
type Message struct {
	Header MessageHeader `json:"header"`
	Value  interface{}   `json:"value"`
}

// CreateDeadLetterMessage 创建统一格式的死信消息
// 所有EventBus实现应该使用此函数来保持死信消息格式的一致性
func CreateDeadLetterMessage(originalTopic string, originalMsg *Message, errorReason string, deadLetterDestination string, source string) *Message {
	now := time.Now()

	// 获取原始消息ID，如果没有则生成一个
	originalID := ""
	if originalMsg != nil && originalMsg.Header != nil {
		if id, exists := originalMsg.Header["message_id"]; exists {
			originalID = id
		} else if id, exists := originalMsg.Header["message-id"]; exists {
			originalID = id
		}
	}
	if originalID == "" {
		originalID = xid.New().String()
	}

	// 创建死信消息值，使用统一的dlq_前缀格式
	dlqValue := map[string]interface{}{
		"dlq_original_id":     originalID,
		"dlq_original_topic":  originalTopic,
		"dlq_failed_at":       now.Unix(),
		"dlq_error_reason":    errorReason,
		"dlq_original_values": nil, // 将在下面设置
	}

	// 安全地获取原始消息值
	if originalMsg != nil {
		dlqValue["dlq_original_values"] = originalMsg.Value
	}

	// 创建死信消息
	dlqMsg := &Message{
		Header: make(MessageHeader),
		Value:  dlqValue,
	}

	// 添加死信相关头信息
	dlqMsg.Header["topic"] = deadLetterDestination
	dlqMsg.Header["dlq_original_topic"] = originalTopic
	dlqMsg.Header["dlq_original_id"] = originalID
	dlqMsg.Header["dlq_error_reason"] = errorReason
	dlqMsg.Header["dlq_failed_at"] = fmt.Sprintf("%d", now.Unix())
	dlqMsg.Header["dlq_source"] = source

	// 复制原始消息的重要头信息，添加前缀避免冲突
	if originalMsg != nil && originalMsg.Header != nil {
		for k, v := range originalMsg.Header {
			if k != "topic" { // 避免覆盖死信消息的topic
				dlqMsg.Header["dlq_original_"+k] = v
			}
		}
	}

	return dlqMsg
}
