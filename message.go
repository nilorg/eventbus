package eventbus

const (
	// MessageHeaderCallback 消息回调
	MessageHeaderCallback = "eventbus.callback"
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
