package eventbus

import "testing"

func TestMessageAssert(t *testing.T) {
	var v interface{}
	v = &Message{}
	if _, ok := v.(*Message); ok {
		t.Log("指针")
	}
	if _, ok := v.(Message); ok {
		t.Log("非指针")
	}
	v = Message{}
	if _, ok := v.(*Message); ok {
		t.Log("指针")
	}
	if _, ok := v.(Message); ok {
		t.Log("非指针")
	}
}
