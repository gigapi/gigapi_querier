package core

import (
	"context"
)

// QueryClient defines the interface for querying data
type QueryClient interface {
	// Query executes a query and returns the results
	Query(ctx context.Context, query, dbName string) ([]map[string]interface{}, error)
	
	// Initialize sets up the query client
	Initialize() error
	
	// Close releases resources
	Close() error
} 