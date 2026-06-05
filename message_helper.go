package eventbus

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/xid"
)

type MessageHeader map[string]string

type SetMessageHeader func(ctx context.Context) MessageHeader

type setMessageHeaderKey struct{}

func NewSetMessageHeaderContext(ctx context.Context, f SetMessageHeader) context.Context {
	return context.WithValue(ctx, setMessageHeaderKey{}, f)
}

func FromSetMessageHeaderContext(ctx context.Context) (f SetMessageHeader, ok bool) {
	f, ok = ctx.Value(setMessageHeaderKey{}).(SetMessageHeader)
	return
}

func extractHeaders(ctx context.Context, topic string) map[string]string {
	headers := make(map[string]string)
	headers["topic"] = topic
	headers["message_id"] = xid.New().String()
	headers["timestamp"] = time.Now().Format(time.RFC3339)

	if f, ok := FromSetMessageHeaderContext(ctx); ok {
		if customHeaders := f(ctx); customHeaders != nil {
			for k, v := range customHeaders {
				headers[k] = v
			}
		}
	}

	return headers
}

func CreateDeadLetterMessage(originalTopic string, originalMsg *Message, errorReason string, deadLetterDestination string, source string, serialize Serializer) *Message {
	now := time.Now()

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

	dlqMsg := &DeadLetterMessage{
		OriginalId:     originalID,
		OriginalTopic:  originalTopic,
		FailedAt:       now.Unix(),
		ErrorReason:    errorReason,
		OriginalValues: originalMsg.Value,
		OriginalHeader: originalMsg.Header,
		Source:         source,
	}

	dlqBytes, err := serialize.Marshal(dlqMsg)
	if err != nil {
		dlqBytes = []byte(fmt.Sprintf("failed to marshal dead letter message: %v", err))
	}

	result := &Message{
		Header: make(map[string]string),
		Value:  dlqBytes,
	}

	result.Header["topic"] = deadLetterDestination
	result.Header["dlq_original_topic"] = originalTopic
	result.Header["dlq_original_id"] = originalID
	result.Header["dlq_error_reason"] = errorReason
	result.Header["dlq_failed_at"] = fmt.Sprintf("%d", now.Unix())
	result.Header["dlq_source"] = source

	if originalMsg != nil && originalMsg.Header != nil {
		for k, v := range originalMsg.Header {
			if k != "topic" {
				result.Header["dlq_original_"+k] = v
			}
		}
	}

	return result
}