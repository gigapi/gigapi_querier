package main

import (
	"context"
	"github.com/gigapi/gigapi-querier/core"
)

// Re-export core logging functions for backward compatibility
func Infof(ctx context.Context, tpl string, args ...any) {
	core.Infof(ctx, tpl, args...)
}

func Errorf(ctx context.Context, tpl string, args ...any) {
	core.Errorf(ctx, tpl, args...)
}

func Debugf(ctx context.Context, tpl string, args ...any) {
	core.Debugf(ctx, tpl, args...)
}

func WithDefaultLogger(parent context.Context, reqId string) context.Context {
	return core.WithDefaultLogger(parent, reqId)
}

func GetRequestID(ctx context.Context) string {
	// Implementation of GetRequestID function
	return "" // Placeholder return, actual implementation needed
}
