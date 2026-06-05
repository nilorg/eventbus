package eventbus

import "github.com/vmihailenco/msgpack/v5"

type MessagePackSerialize struct{}

func (MessagePackSerialize) Unmarshal(data []byte, msg interface{}) (err error) {
	return msgpack.Unmarshal(data, msg)
}

func (MessagePackSerialize) Marshal(msg interface{}) (data []byte, err error) {
	return msgpack.Marshal(msg)
}

func (MessagePackSerialize) ContentType() string {
	return "application/x-msgpack"
}