package iceberg

import (
	"context"
	"fmt"
	"strings"

	"github.com/gigapi/gigapi-querier/core"
)

// TableOperations handles operations on Iceberg tables
type TableOperations struct {
	Catalog *Catalog
	QueryClient core.QueryClient
}

// NewTableOperations creates a new TableOperations instance
func NewTableOperations(catalog *Catalog, queryClient core.QueryClient) *TableOperations {
	return &TableOperations{
		Catalog: catalog,
		QueryClient: queryClient,
	}
}

// ExecuteQuery executes a query on an Iceberg table
func (t *TableOperations) ExecuteQuery(ctx context.Context, namespace, name string, icebergQuery string) ([]map[string]interface{}, error) {
	// Just pass the query through to QueryClient
	core.Infof(ctx, "Executing query: %s", icebergQuery)
	return t.QueryClient.Query(ctx, icebergQuery, namespace)
}

// GetTableSchema returns the schema of an Iceberg table using DuckDB's DESCRIBE
func (t *TableOperations) GetTableSchema(namespace, name string) (*Schema, error) {
	// Use DuckDB's DESCRIBE to get the schema
	describeQuery := fmt.Sprintf("DESCRIBE SELECT * FROM %s.%s", namespace, name)
	results, err := t.QueryClient.Query(nil, describeQuery, namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %v", err)
	}

	// Convert results to Schema
	schema := &Schema{
		Type:   "struct",
		Fields: make([]Field, 0, len(results)),
	}

	for _, row := range results {
		field := Field{
			Name:     row["column_name"].(string),
			Type:     row["column_type"].(string),
			Required: true, // DuckDB doesn't provide nullability info in DESCRIBE
		}
		schema.Fields = append(schema.Fields, field)
	}

	return schema, nil
}

// GetTablePartitionSpec returns the partition specification of an Iceberg table
func (t *TableOperations) GetTablePartitionSpec(namespace, name string) ([]PartitionSpec, error) {
	// Since we're using our existing infrastructure, we don't need partition specs
	// Return an empty slice as we don't use Iceberg's partitioning
	return []PartitionSpec{}, nil
}

// GetCurrentSchema returns the current schema of an Iceberg table by querying the actual data files
func (t *TableOperations) GetCurrentSchema(ctx context.Context, namespace, name string) (*Schema, error) {
	// Get the table files
	files, err := t.Catalog.GetTableFiles(ctx, namespace, name)
	if err != nil {
		return nil, fmt.Errorf("failed to get table files: %v", err)
	}

	// Build the files list for the query
	var filesList strings.Builder
	for i, file := range files {
		if i > 0 {
			filesList.WriteString(", ")
		}
		filesList.WriteString(fmt.Sprintf("'%s'", file))
	}

	// Construct the DESCRIBE query
	describeQuery := fmt.Sprintf("DESCRIBE SELECT * FROM read_parquet([%s], union_by_name=true)", filesList.String())

	// Execute the query
	results, err := t.QueryClient.Query(ctx, describeQuery, namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %v", err)
	}

	// Convert results to Schema
	schema := &Schema{
		Type:   "struct",
		Fields: make([]Field, 0, len(results)),
	}

	for _, row := range results {
		field := Field{
			Name:     row["column_name"].(string),
			Type:     row["column_type"].(string),
			Required: true, // DuckDB doesn't provide nullability info in DESCRIBE
		}
		schema.Fields = append(schema.Fields, field)
	}

	return schema, nil
} 