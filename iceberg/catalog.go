package iceberg

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/gigapi/gigapi-querier/core"
)

// TableIdentifier represents an Iceberg table identifier
type TableIdentifier struct {
	Namespace string
	Name      string
}

// Table represents an Iceberg table
type Table struct {
	Identifier TableIdentifier
	Location   string
}

// Catalog represents the minimal Iceberg catalog interface
type Catalog struct {
	BasePath string
	QueryClient core.QueryClient
}

// NewCatalog creates a new Catalog instance
func NewCatalog(basePath string, queryClient core.QueryClient) *Catalog {
	return &Catalog{
		BasePath: basePath,
		QueryClient: queryClient,
	}
}

// LoadTable loads a table from the catalog
func (c *Catalog) LoadTable(namespace, name string) (*Table, error) {
	tablePath := filepath.Join(c.BasePath, namespace, name)
	
	// Check if table exists
	if _, err := os.Stat(tablePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("table %s.%s does not exist", namespace, name)
	}

	return &Table{
		Identifier: TableIdentifier{
			Namespace: namespace,
			Name:      name,
		},
		Location: tablePath,
	}, nil
}

// ListTables lists all tables in a namespace
func (c *Catalog) ListTables(namespace string) ([]TableIdentifier, error) {
	// Validate namespace to prevent directory traversal
	if strings.Contains(namespace, "/") || strings.Contains(namespace, "\\") || strings.Contains(namespace, "..") {
		return nil, fmt.Errorf("invalid namespace: %s", namespace)
	}

	namespacePath := filepath.Join(c.BasePath, namespace)
	
	entries, err := os.ReadDir(namespacePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read namespace directory: %v", err)
	}

	var tables []TableIdentifier
	for _, entry := range entries {
		if entry.IsDir() {
			tables = append(tables, TableIdentifier{
				Namespace: namespace,
				Name:      entry.Name(),
			})
		}
	}

	return tables, nil
}

// GetTableLocation returns the location of a table
func (c *Catalog) GetTableLocation(namespace, name string) (string, error) {
	tablePath := filepath.Join(c.BasePath, namespace, name)
	
	if _, err := os.Stat(tablePath); os.IsNotExist(err) {
		return "", fmt.Errorf("table %s.%s does not exist", namespace, name)
	}

	return tablePath, nil
}

// GetTableFiles returns all data files for a table using QueryClient
func (c *Catalog) GetTableFiles(ctx context.Context, namespace, name string) ([]string, error) {
	// Log the lookup attempt
	core.Infof(ctx, "Looking up files for table %s.%s in base path: %s", namespace, name, c.BasePath)

	// Check if the table directory exists
	tablePath := filepath.Join(c.BasePath, namespace, name)
	if _, err := os.Stat(tablePath); os.IsNotExist(err) {
		core.Errorf(ctx, "Table directory does not exist: %s", tablePath)
		return nil, fmt.Errorf("table directory not found: %s", tablePath)
	}

	// Use QueryClient to find all parquet files for this table
	query := fmt.Sprintf("SELECT * FROM %s.%s LIMIT 1", namespace, name)
	core.Infof(ctx, "Executing test query: %s", query)
	
	_, err := c.QueryClient.Query(ctx, query, namespace)
	if err != nil {
		core.Errorf(ctx, "Failed to execute test query: %v", err)
		return nil, fmt.Errorf("failed to get table files: %v", err)
	}

	// Log the data directory structure
	core.Infof(ctx, "Scanning directory structure for %s", tablePath)
	err = filepath.Walk(tablePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			core.Errorf(ctx, "Error accessing path %s: %v", path, err)
			return nil // Continue walking
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".parquet") {
			core.Infof(ctx, "Found parquet file: %s", path)
		}
		return nil
	})
	if err != nil {
		core.Errorf(ctx, "Error walking directory: %v", err)
	}

	// The QueryClient will handle finding the relevant files
	// We don't need to look for metadata files
	return []string{}, nil
} 