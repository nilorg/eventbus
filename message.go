package eventbus

import "context"

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
