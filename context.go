package eventbus

import "context"

type groupIDKey struct{}

// NewGroupIDContext ...
func NewGroupIDContext(parent context.Context, groupID string) context.Context {
	return context.WithValue(parent, groupIDKey{}, groupID)
}

// FromGroupIDContext ...
func FromGroupIDContext(ctx context.Context) (groupID string, ok bool) {
	groupID, ok = ctx.Value(groupIDKey{}).(string)
	return
}
