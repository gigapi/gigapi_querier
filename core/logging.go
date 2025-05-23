package core

import (
	"context"
	config2 "github.com/gigapi/gigapi-config/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const LoggerKey = "LOGGER"

func getLogger(ctx context.Context) *zap.Logger {
	if ctx == nil {
		return zap.NewNop()
	}

	logger, ok := ctx.Value(LoggerKey).(*zap.Logger)
	if !ok || logger == nil {
		return zap.NewNop()
	}

	return logger
}

func WithLogger(parent context.Context, logger *zap.Logger) context.Context {
	return context.WithValue(parent, LoggerKey, logger)
}

func WithDefaultLogger(parent context.Context, reqId string) context.Context {
	return WithLogger(parent, DefaultLogger(reqId))
}

func DefaultLogger(reqId string) *zap.Logger {
	config := zap.NewProductionConfig()

	// Get log level from environment variable
	logLevel := config2.Config.Loglevel
	if level, err := zapcore.ParseLevel(logLevel); logLevel != "" && err == nil {
		config.Level.SetLevel(level)
	} else {
		config.Level.SetLevel(zapcore.InfoLevel)
	}

	logger, _ := config.Build()
	return logger.With(zap.String("req_id", reqId))
}

func Infof(ctx context.Context, tpl string, args ...any) {
	getLogger(ctx).Sugar().Infof(tpl, args...)
}

func Errorf(ctx context.Context, tpl string, args ...any) {
	getLogger(ctx).Sugar().Errorf(tpl, args...)
}

func Debugf(ctx context.Context, tpl string, args ...any) {
	getLogger(ctx).Sugar().Debugf(tpl, args...)
}
