package eventbus

import (
	"errors"
	"google.golang.org/protobuf/proto"
)

var (
	ErrNotProtoMessage       = errors.New("not a protobuf message")
	ErrUnsupportedContentType = errors.New("unsupported content type")
	ErrInvalidMessageFormat   = errors.New("invalid message format")
)

type ProtobufSerialize struct{}

func (s *ProtobufSerialize) Marshal(v interface{}) ([]byte, error) {
	msg, ok := v.(proto.Message)
	if !ok {
		return nil, ErrNotProtoMessage
	}
	return proto.Marshal(msg)
}

func (s *ProtobufSerialize) Unmarshal(data []byte, v interface{}) error {
	msg, ok := v.(proto.Message)
	if !ok {
		return ErrNotProtoMessage
	}
	return proto.Unmarshal(data, msg)
}

func (s *ProtobufSerialize) ContentType() string {
	return "application/x-protobuf"
}