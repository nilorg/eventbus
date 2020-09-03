package eventbus

import "context"

const (
	// MessageHeaderCallback 消息回调
	MessageHeaderCallback = "nilorg.eventbus.callback"
	// MessageHeaderUser 消息用户
	MessageHeaderUser = "nilorg.eventbus.user"
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

// IsCallback 存在回调
func (m *Message) IsCallback() bool {
	_, ok := m.Header[MessageHeaderCallback]
	return ok
}

// Callback 回调地址
func (m *Message) Callback() string {
	callback, _ := m.Header[MessageHeaderCallback]
	return callback
}

// IsUser 是否存在用户
func (m *Message) IsUser() bool {
	_, ok := m.Header[MessageHeaderUser]
	return ok
}

// User 用户
func (m *Message) User() string {
	user, _ := m.Header[MessageHeaderUser]
	return user
}
