package eventbus

// Serializer 序列化器
type Serializer interface {
	Unmarshal(data []byte, msg interface{}) (err error)
	Marshal(msg interface{}) (data []byte, err error)
	ContentType() string
}
