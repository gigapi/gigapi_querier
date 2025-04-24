package iceberg

import (
	"context"
	"fmt"
	"strings"
)

// TableOperations handles operations on Iceberg tables
type TableOperations struct {
	Catalog *Catalog
	QueryClient *QueryClient
}

// NewTableOperations creates a new TableOperations instance
func NewTableOperations(catalog *Catalog, queryClient *QueryClient) *TableOperations {
	return &TableOperations{
		Catalog: catalog,
		QueryClient: queryClient,
	}
}

// TranslateIcebergQuery translates an Iceberg query to our internal query format
func (t *TableOperations) TranslateIcebergQuery(ctx context.Context, namespace, name string, icebergQuery string) (string, error) {
	// Parse the Iceberg query to extract components
	// This is a simplified version - in reality, you'd want to use a proper SQL parser
	parts := strings.SplitN(icebergQuery, " FROM ", 2)
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid query format")
	}

	selectClause := parts[0]
	fromClause := parts[1]

	// Extract table name and any conditions
	tableParts := strings.SplitN(fromClause, " WHERE ", 2)
	tableName := strings.TrimSpace(tableParts[0])
	whereClause := ""
	if len(tableParts) > 1 {
		whereClause = tableParts[1]
	}

	// Get the table files
	files, err := t.Catalog.GetTableFiles(namespace, name)
	if err != nil {
		return "", fmt.Errorf("failed to get table files: %v", err)
	}

	// Build the internal query
	var filesList strings.Builder
	for i, file := range files {
		if i > 0 {
			filesList.WriteString(", ")
		}
		filesList.WriteString(fmt.Sprintf("'%s'", file))
	}

	// Construct the internal query
	internalQuery := fmt.Sprintf("%s FROM read_parquet([%s], union_by_name=true)", selectClause, filesList.String())
	if whereClause != "" {
		internalQuery += " WHERE " + whereClause
	}

	return internalQuery, nil
}

// ExecuteQuery executes a query on an Iceberg table
func (t *TableOperations) ExecuteQuery(ctx context.Context, namespace, name string, icebergQuery string) ([]map[string]interface{}, error) {
	// Translate the Iceberg query to our internal format
	internalQuery, err := t.TranslateIcebergQuery(ctx, namespace, name, icebergQuery)
	if err != nil {
		return nil, err
	}

	// Execute the query using our existing QueryClient
	return t.QueryClient.Query(ctx, internalQuery, namespace)
}

// GetTableSchema returns the schema of an Iceberg table
func (t *TableOperations) GetTableSchema(namespace, name string) (*Schema, error) {
	metadata, err := t.Catalog.GetTableMetadata(namespace, name)
	if err != nil {
		return nil, err
	}

	return &metadata.Schema, nil
}

// GetTablePartitionSpec returns the partition specification of an Iceberg table
func (t *TableOperations) GetTablePartitionSpec(namespace, name string) ([]PartitionSpec, error) {
	metadata, err := t.Catalog.GetTableMetadata(namespace, name)
	if err != nil {
		return nil, err
	}

	return metadata.PartitionSpec, nil
} 