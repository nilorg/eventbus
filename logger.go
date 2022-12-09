package eventbus

import (
	"context"
	"log"

	"github.com/nilorg/pkg/zlog"
	"github.com/sirupsen/logrus"
	"go.uber.org/zap"
)

// Logger logger
type Logger interface {
	// Debugf 测试
	Debugf(ctx context.Context, format string, args ...interface{})
	// Debugln 测试
	Debugln(ctx context.Context, args ...interface{})
	// Infof 信息
	Infof(ctx context.Context, format string, args ...interface{})
	// Infoln 消息
	Infoln(ctx context.Context, args ...interface{})
	// Warnf 警告
	Warnf(ctx context.Context, format string, args ...interface{})
	// Warnln 警告
	Warnln(ctx context.Context, args ...interface{})
	// Warningf 警告
	Warningf(ctx context.Context, format string, args ...interface{})
	// Warningln 警告
	Warningln(ctx context.Context, args ...interface{})
	// Errorf 错误
	Errorf(ctx context.Context, format string, args ...interface{})
	// Errorln 错误
	Errorln(ctx context.Context, args ...interface{})
}

// StdLogger ...
type StdLogger struct {
}

// Debugf 测试
func (StdLogger) Debugf(ctx context.Context, format string, args ...interface{}) {
	log.Printf("[Debug] "+format, args...)
}

// Debugln 测试
func (StdLogger) Debugln(ctx context.Context, args ...interface{}) {
	nArgs := []interface{}{
		"[Debug]",
	}
	nArgs = append(nArgs, args...)
	log.Println(nArgs...)
}

// Infof 信息
func (StdLogger) Infof(ctx context.Context, format string, args ...interface{}) {
	log.Printf("[INFO] "+format, args...)
}

// Infoln 消息
func (StdLogger) Infoln(ctx context.Context, args ...interface{}) {
	nArgs := []interface{}{
		"[INFO]",
	}
	nArgs = append(nArgs, args...)
	log.Println(nArgs...)
}

// Warnf 警告
func (StdLogger) Warnf(ctx context.Context, format string, args ...interface{}) {
	log.Printf("[Warn] "+format, args...)
}

// Warnln 警告
func (StdLogger) Warnln(ctx context.Context, args ...interface{}) {
	nArgs := []interface{}{
		"[Warn]",
	}
	nArgs = append(nArgs, args...)
	log.Println(nArgs...)
}

// Warningf 警告
func (StdLogger) Warningf(ctx context.Context, format string, args ...interface{}) {
	log.Printf("[Warning] "+format, args...)
}

// Warningln 警告
func (StdLogger) Warningln(ctx context.Context, args ...interface{}) {
	nArgs := []interface{}{
		"[Warning]",
	}
	nArgs = append(nArgs, args...)
	log.Println(nArgs...)
}

// Errorf 错误
func (StdLogger) Errorf(ctx context.Context, format string, args ...interface{}) {
	log.Printf("[Error] "+format, args...)
}

// Errorln 错误
func (StdLogger) Errorln(ctx context.Context, args ...interface{}) {
	nArgs := []interface{}{
		"[Error]",
	}
	nArgs = append(nArgs, args...)
	log.Println(nArgs...)
}

// LogrusLogger ...
type LogrusLogger struct {
	log *logrus.Logger
}

// NewLogrusLogger ...
func NewLogrusLogger(log *logrus.Logger) Logger {
	return &LogrusLogger{
		log: log,
	}
}

// Debugf 测试
func (l *LogrusLogger) Debugf(ctx context.Context, format string, args ...interface{}) {
	l.log.WithContext(ctx).Debugf(format, args...)
}

// Debugln 测试
func (l *LogrusLogger) Debugln(ctx context.Context, args ...interface{}) {
	l.log.WithContext(ctx).Debugln(args...)
}

// Infof 信息
func (l *LogrusLogger) Infof(ctx context.Context, format string, args ...interface{}) {
	l.log.WithContext(ctx).Debugf(format, args...)
}

// Infoln 消息
func (l *LogrusLogger) Infoln(ctx context.Context, args ...interface{}) {
	l.log.WithContext(ctx).Infoln(args...)
}

// Warnf 警告
func (l *LogrusLogger) Warnf(ctx context.Context, format string, args ...interface{}) {
	l.log.WithContext(ctx).Warnf(format, args...)
}

// Warnln 警告
func (l *LogrusLogger) Warnln(ctx context.Context, args ...interface{}) {
	l.log.WithContext(ctx).Warnln(args...)
}

// Warningf 警告
func (l *LogrusLogger) Warningf(ctx context.Context, format string, args ...interface{}) {
	l.log.WithContext(ctx).Warningf(format, args...)
}

// Warningln 警告
func (l *LogrusLogger) Warningln(ctx context.Context, args ...interface{}) {
	l.log.WithContext(ctx).Warningln(args...)
}

// Errorf 错误
func (l *LogrusLogger) Errorf(ctx context.Context, format string, args ...interface{}) {
	l.log.WithContext(ctx).Errorf(format, args...)
}

// Errorln 错误
func (l *LogrusLogger) Errorln(ctx context.Context, args ...interface{}) {
	l.log.WithContext(ctx).Errorln(args...)
}

// ZapLogger ...
type ZapLogger struct {
	log *zap.Logger
}

// NewZapLogger ...
func NewZapLogger(log *zap.Logger) Logger {
	return &ZapLogger{
		log: log,
	}
}

// Debugf 测试
func (l *ZapLogger) Debugf(ctx context.Context, format string, args ...interface{}) {
	l.log.Sugar().Debugf(format, args...)
}

// Debugln 测试
func (l *ZapLogger) Debugln(ctx context.Context, args ...interface{}) {
	l.log.Sugar().Debugln(args...)
}

// Infof 信息
func (l *ZapLogger) Infof(ctx context.Context, format string, args ...interface{}) {
	l.log.Sugar().Debugf(format, args...)
}

// Infoln 消息
func (l *ZapLogger) Infoln(ctx context.Context, args ...interface{}) {
	l.log.Sugar().Infoln(args...)
}

// Warnf 警告
func (l *ZapLogger) Warnf(ctx context.Context, format string, args ...interface{}) {
	l.log.Sugar().Warnf(format, args...)
}

// Warnln 警告
func (l *ZapLogger) Warnln(ctx context.Context, args ...interface{}) {
	l.log.Sugar().Warnln(args...)
}

// Warningf 警告
func (l *ZapLogger) Warningf(ctx context.Context, format string, args ...interface{}) {
	l.log.Sugar().Warnf(format, args...)
}

// Warningln 警告
func (l *ZapLogger) Warningln(ctx context.Context, args ...interface{}) {
	l.log.Sugar().Warnln(args...)
}

// Errorf 错误
func (l *ZapLogger) Errorf(ctx context.Context, format string, args ...interface{}) {
	l.log.Sugar().Errorf(format, args...)
}

// Errorln 错误
func (l *ZapLogger) Errorln(ctx context.Context, args ...interface{}) {
	l.log.Sugar().Errorln(args...)
}

// ZLogger ...
type ZLogger struct {
}

// NewZapLogger ...
func NewZLogger() Logger {
	return &ZapLogger{}
}

// Debugf 测试
func (*ZLogger) Debugf(ctx context.Context, format string, args ...interface{}) {
	zlog.WithSugared(ctx).Debugf(format, args...)
}

// Debugln 测试
func (*ZLogger) Debugln(ctx context.Context, args ...interface{}) {
	zlog.WithSugared(ctx).Debugln(args...)
}

// Infof 信息
func (*ZLogger) Infof(ctx context.Context, format string, args ...interface{}) {
	zlog.WithSugared(ctx).Debugf(format, args...)
}

// Infoln 消息
func (*ZLogger) Infoln(ctx context.Context, args ...interface{}) {
	zlog.WithSugared(ctx).Infoln(args...)
}

// Warnf 警告
func (*ZLogger) Warnf(ctx context.Context, format string, args ...interface{}) {
	zlog.WithSugared(ctx).Warnf(format, args...)
}

// Warnln 警告
func (*ZLogger) Warnln(ctx context.Context, args ...interface{}) {
	zlog.WithSugared(ctx).Warnln(args...)
}

// Warningf 警告
func (*ZLogger) Warningf(ctx context.Context, format string, args ...interface{}) {
	zlog.WithSugared(ctx).Warnf(format, args...)
}

// Warningln 警告
func (*ZLogger) Warningln(ctx context.Context, args ...interface{}) {
	zlog.WithSugared(ctx).Warnln(args...)
}

// Errorf 错误
func (*ZLogger) Errorf(ctx context.Context, format string, args ...interface{}) {
	zlog.WithSugared(ctx).Errorf(format, args...)
}

// Errorln 错误
func (*ZLogger) Errorln(ctx context.Context, args ...interface{}) {
	zlog.WithSugared(ctx).Errorln(args...)
}
