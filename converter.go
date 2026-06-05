package eventbus

import (
	"encoding/json"

	"github.com/vmihailenco/msgpack/v5"
	"google.golang.org/protobuf/proto"
)

type MessageConverter interface {
	Convert(data []byte, contentType string) (*Message, error)
}

type JSONConverter struct{}

func (c *JSONConverter) Convert(data []byte, contentType string) (*Message, error) {
	if contentType != "application/json" {
		return nil, ErrUnsupportedContentType
	}

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

type AutoConverter struct{}

func (c *AutoConverter) Convert(data []byte, contentType string) (*Message, error) {
	switch contentType {
	case "application/json":
		var msg Message
		if err := json.Unmarshal(data, &msg); err == nil {
			return &msg, nil
		}
		return (&JSONConverter{}).Convert(data, contentType)
	case "application/x-protobuf":
		var msg Message
		if err := proto.Unmarshal(data, &msg); err != nil {
			return nil, err
		}
		return &msg, nil
	case "application/x-msgpack":
		var msg Message
		if err := msgpack.Unmarshal(data, &msg); err != nil {
			return nil, err
		}
		return &msg, nil
	default:
		return nil, ErrUnsupportedContentType
	}
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