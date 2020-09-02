package eventbus

import "encoding/json"

// Serializer 序列化器
type Serializer interface {
	Unmarshal(data []byte, msg *Message) (err error)
	Marshal(msg *Message) (data []byte, err error)
	ContentType() string
}

// JSONSerialize json序列号
type JSONSerialize struct {
}

// Unmarshal ...
func (JSONSerialize) Unmarshal(data []byte, msg *Message) (err error) {
	return json.Unmarshal(data, msg)
}

// Marshal ...
func (JSONSerialize) Marshal(msg *Message) (data []byte, err error) {
	return json.Marshal(msg)
}

// ContentType ...
func (JSONSerialize) ContentType() string {
	return "application/json"
}
