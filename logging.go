package main

import (
	"context"
	"go.uber.org/zap/zapcore"
	"os"

	"go.uber.org/zap"
)

const LoggerKey = "LOGGER"

func getLogger(ctx context.Context) *zap.Logger {
	logger, _ := ctx.Value(LoggerKey).(*zap.Logger)
	if logger == nil {
		// Fallback to a no-op logger if not found in context
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
	logLevel := os.Getenv("LOGLEVEL")
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
