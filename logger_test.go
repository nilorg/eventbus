package eventbus

import (
	"context"
	"testing"
)

func TestLogger(t *testing.T) {
	std := StdLogger{}
	ctx := context.Background()
	std.Debugf(ctx, "test logger %s", "Debugf")
	std.Debugln(ctx, "test logger", "Debugln")

	std.Infof(ctx, "test logger %s", "Infof")
	std.Infoln(ctx, "test logger", "Infoln")

	std.Warnf(ctx, "test logger %s", "Warnf")
	std.Warnln(ctx, "test logger", "Warnln")

	std.Warningf(ctx, "test logger %s", "Warningf")
	std.Warningln(ctx, "test logger", "Warningln")

	std.Errorf(ctx, "test logger %s", "Errorf")
	std.Errorln(ctx, "test logger", "Errorln")
}
