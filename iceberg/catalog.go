package iceberg

import (
	"fmt"
	"os"
	"path/filepath"
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
	Metadata   *TableMetadata
}

// Catalog represents the minimal Iceberg catalog interface
type Catalog struct {
	BasePath string
	MetadataReader *MetadataReader
	ManifestProcessor *ManifestProcessor
}

// NewCatalog creates a new Catalog instance
func NewCatalog(basePath string) *Catalog {
	return &Catalog{
		BasePath: basePath,
		MetadataReader: NewMetadataReader(basePath),
		ManifestProcessor: NewManifestProcessor(basePath),
	}
}

// LoadTable loads a table from the catalog
func (c *Catalog) LoadTable(namespace, name string) (*Table, error) {
	tablePath := filepath.Join(c.BasePath, namespace, name)
	
	// Check if table exists
	if _, err := os.Stat(tablePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("table %s.%s does not exist", namespace, name)
	}

	// Read metadata
	metadata, err := c.MetadataReader.ReadMetadata(name)
	if err != nil {
		return nil, fmt.Errorf("failed to read table metadata: %v", err)
	}

	return &Table{
		Identifier: TableIdentifier{
			Namespace: namespace,
			Name:      name,
		},
		Location: tablePath,
		Metadata: metadata,
	}, nil
}

// ListTables lists all tables in a namespace
func (c *Catalog) ListTables(namespace string) ([]TableIdentifier, error) {
	namespacePath := filepath.Join(c.BasePath, namespace)
	
	entries, err := os.ReadDir(namespacePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read namespace directory: %v", err)
	}

	var tables []TableIdentifier
	for _, entry := range entries {
		if entry.IsDir() {
			// Check if it's a valid table by looking for metadata
			metadataPath := filepath.Join(namespacePath, entry.Name(), "metadata")
			if _, err := os.Stat(metadataPath); err == nil {
				tables = append(tables, TableIdentifier{
					Namespace: namespace,
					Name:      entry.Name(),
				})
			}
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

// GetTableMetadata returns the metadata for a table
func (c *Catalog) GetTableMetadata(namespace, name string) (*TableMetadata, error) {
	return c.MetadataReader.ReadMetadata(name)
}

// GetTableFiles returns all data files for a table
func (c *Catalog) GetTableFiles(namespace, name string) ([]string, error) {
	metadata, err := c.MetadataReader.ReadMetadata(name)
	if err != nil {
		return nil, err
	}

	snapshotID, manifestList, err := c.MetadataReader.GetCurrentSnapshot(name)
	if err != nil {
		return nil, err
	}

	return c.ManifestProcessor.GetDataFiles(name, manifestList)
} 