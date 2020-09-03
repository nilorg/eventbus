package eventbus

const (
	// MessageHeaderCallback 消息回调
	MessageHeaderCallback = "nilorg.eventbus.callback"
	// MessageHeaderUser 消息用户
	MessageHeaderUser = "nilorg.eventbus.user"
)

// MessageHeader message header
type MessageHeader map[string]string

// Message 消息
type Message struct {
	Header MessageHeader `json:"header"`
	Value  interface{}   `json:"value"`
}

// ISCallback 存在回调
func (m *Message) ISCallback() bool {
	_, ok := m.Header[MessageHeaderCallback]
	return ok
}

// Callback 回调地址
func (m *Message) Callback() string {
	callback, _ := m.Header[MessageHeaderCallback]
	return callback
}

// ISUser 是否存在用户
func (m *Message) ISUser() bool {
	_, ok := m.Header[MessageHeaderUser]
	return ok
}

// User 用户
func (m *Message) User() string {
	user, _ := m.Header[MessageHeaderUser]
	return user
}
