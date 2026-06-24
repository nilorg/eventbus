package eventbus

import (
	"encoding/json"
)

type MessageConverter interface {
	Convert(data []byte) (*Message, error)
}

type JSONConverter struct{}

func (c *JSONConverter) Convert(data []byte) (*Message, error) {
	var legacyMsg struct {
		Header map[string]string `json:"header"`
		Value  interface{}       `json:"value"`
	}
	if err := json.Unmarshal(data, &legacyMsg); err != nil {
		return nil, err
	}

	valueBytes, err := json.Marshal(legacyMsg.Value)
	if err != nil {
		return nil, err
	}

	return &Message{
		Header: legacyMsg.Header,
		Value:  valueBytes,
	}, nil
}

type AutoConverter struct {
	serializers map[string]Serializer
}

func NewAutoConverter() *AutoConverter {
	return &AutoConverter{
		serializers: map[string]Serializer{
			"application/json":       &JSONSerialize{},
			"application/x-protobuf": &ProtobufSerialize{},
			"application/x-msgpack":  &MessagePackSerialize{},
		},
	}
}

func (c *AutoConverter) Register(contentType string, serializer Serializer) {
	c.serializers[contentType] = serializer
}

func (c *AutoConverter) Convert(data []byte) (*Message, error) {
	for _, serializer := range c.serializers {
		var msg Message
		if err := serializer.Unmarshal(data, &msg); err == nil {
			return &msg, nil
		}
	}
	return nil, ErrUnsupportedContentType
}

type SubscribeOptions struct {
	Converter MessageConverter
}

type SubscribeOption func(*SubscribeOptions)

func WithConverter(converter MessageConverter) SubscribeOption {
	return func(opts *SubscribeOptions) {
		opts.Converter = converter
	}
}