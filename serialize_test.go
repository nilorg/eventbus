package eventbus

import (
	"testing"

	"github.com/nilorg/eventbus/proto"
	"github.com/stretchr/testify/assert"
)

func TestProtobufSerialize_MarshalUnmarshal(t *testing.T) {
	serialize := &ProtobufSerialize{}

	event := &proto.TestEvent{Message: "hello", Count: 42}

	data, err := serialize.Marshal(event)
	assert.NoError(t, err)
	assert.NotNil(t, data)

	var decoded proto.TestEvent
	err = serialize.Unmarshal(data, &decoded)
	assert.NoError(t, err)
	assert.Equal(t, event.Message, decoded.Message)
	assert.Equal(t, event.Count, decoded.Count)
}

func TestTwoStageSerialization(t *testing.T) {
	serialize := &ProtobufSerialize{}

	userEvent := &proto.UserCreated{UserId: "123", Name: "John"}

	eventBytes, err := serialize.Marshal(userEvent)
	assert.NoError(t, err)
	assert.NotNil(t, eventBytes)

	msg := &Message{
		Header: map[string]string{"type": "user.created", "trace_id": "abc123"},
		Value:  eventBytes,
	}

	msgBytes, err := serialize.Marshal(msg)
	assert.NoError(t, err)
	assert.NotNil(t, msgBytes)

	var decodedMsg Message
	err = serialize.Unmarshal(msgBytes, &decodedMsg)
	assert.NoError(t, err)
	assert.Equal(t, msg.Header["type"], decodedMsg.Header["type"])
	assert.Equal(t, msg.Header["trace_id"], decodedMsg.Header["trace_id"])

	var decodedEvent proto.UserCreated
	err = serialize.Unmarshal(decodedMsg.Value, &decodedEvent)
	assert.NoError(t, err)
	assert.Equal(t, userEvent.UserId, decodedEvent.UserId)
	assert.Equal(t, userEvent.Name, decodedEvent.Name)
}

func TestJSONSerialize_WithProtoMessage(t *testing.T) {
	serialize := &JSONSerialize{}

	msg := &Message{
		Header: map[string]string{"type": "test"},
		Value:  []byte("test value"),
	}

	data, err := serialize.Marshal(msg)
	assert.NoError(t, err)

	var decoded Message
	err = serialize.Unmarshal(data, &decoded)
	assert.NoError(t, err)
	assert.Equal(t, msg.Header["type"], decoded.Header["type"])
	assert.Equal(t, msg.Value, decoded.Value)
}

func TestMessagePackSerialize_MarshalUnmarshal(t *testing.T) {
	serialize := &MessagePackSerialize{}

	event := &proto.TestEvent{Message: "hello", Count: 42}

	data, err := serialize.Marshal(event)
	assert.NoError(t, err)
	assert.NotNil(t, data)

	var decoded proto.TestEvent
	err = serialize.Unmarshal(data, &decoded)
	assert.NoError(t, err)
	assert.Equal(t, event.Message, decoded.Message)
	assert.Equal(t, event.Count, decoded.Count)
}

func TestMessagePackSerialize_TwoStage(t *testing.T) {
	serialize := &MessagePackSerialize{}

	userEvent := &proto.UserCreated{UserId: "123", Name: "John"}

	eventBytes, err := serialize.Marshal(userEvent)
	assert.NoError(t, err)
	assert.NotNil(t, eventBytes)

	msg := &Message{
		Header: map[string]string{"type": "user.created", "trace_id": "abc123"},
		Value:  eventBytes,
	}

	msgBytes, err := serialize.Marshal(msg)
	assert.NoError(t, err)
	assert.NotNil(t, msgBytes)

	var decodedMsg Message
	err = serialize.Unmarshal(msgBytes, &decodedMsg)
	assert.NoError(t, err)
	assert.Equal(t, msg.Header["type"], decodedMsg.Header["type"])
	assert.Equal(t, msg.Header["trace_id"], decodedMsg.Header["trace_id"])

	var decodedEvent proto.UserCreated
	err = serialize.Unmarshal(decodedMsg.Value, &decodedEvent)
	assert.NoError(t, err)
	assert.Equal(t, userEvent.UserId, decodedEvent.UserId)
	assert.Equal(t, userEvent.Name, decodedEvent.Name)
}

func TestMessagePackSerialize_ContentType(t *testing.T) {
	serialize := &MessagePackSerialize{}
	assert.Equal(t, "application/x-msgpack", serialize.ContentType())
}