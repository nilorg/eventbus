package eventbus

import "encoding/json"

// JSONSerialize json序列号
type JSONSerialize struct {
}

// Unmarshal ...
func (JSONSerialize) Unmarshal(data []byte, msg interface{}) (err error) {
	return json.Unmarshal(data, msg)
}

// Marshal ...
func (JSONSerialize) Marshal(msg interface{}) (data []byte, err error) {
	return json.Marshal(msg)
}

// ContentType ...
func (JSONSerialize) ContentType() string {
	return "application/json"
}
