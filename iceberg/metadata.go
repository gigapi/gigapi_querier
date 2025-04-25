package iceberg

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// isValidTableName validates that the table name is a single path component
func isValidTableName(name string) bool {
	if name == "" || strings.Contains(name, "/") || strings.Contains(name, "\\") || strings.Contains(name, "..") {
		return false
	}
	return true
}

// TableMetadata represents the Iceberg table metadata structure
type TableMetadata struct {
	FormatVersion int    `json:"format-version"`
	TableUUID     string `json:"table-uuid"`
	Location      string `json:"location"`
	LastUpdatedMs int64  `json:"last-updated-ms"`
	LastColumnID  int    `json:"last-column-id"`
	Schema        Schema `json:"schema"`
	PartitionSpec []PartitionSpec `json:"partition-spec"`
	Properties    map[string]string `json:"properties"`
	CurrentSnapshotID int64 `json:"current-snapshot-id"`
	Snapshots     []Snapshot `json:"snapshots"`
}

// Schema represents the Iceberg table schema
type Schema struct {
	Type   string   `json:"type"`
	Fields []Field  `json:"fields"`
}

// Field represents a field in the Iceberg schema
type Field struct {
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Type        string `json:"type"`
	Required    bool   `json:"required"`
	Doc         string `json:"doc,omitempty"`
}

// PartitionSpec represents the Iceberg partition specification
type PartitionSpec struct {
	SpecID   int    `json:"spec-id"`
	Fields   []PartitionField `json:"fields"`
}

// PartitionField represents a field in the partition specification
type PartitionField struct {
	SourceID int    `json:"source-id"`
	FieldID  int    `json:"field-id"`
	Name     string `json:"name"`
	Transform string `json:"transform"`
}

// Snapshot represents an Iceberg table snapshot
type Snapshot struct {
	SnapshotID        int64  `json:"snapshot-id"`
	TimestampMs       int64  `json:"timestamp-ms"`
	ManifestList      string `json:"manifest-list"`
	SchemaID          int    `json:"schema-id"`
	Summary           SnapshotSummary `json:"summary"`
}

// SnapshotSummary represents the summary of a snapshot
type SnapshotSummary struct {
	Operation string `json:"operation"`
}

// MetadataReader handles reading and parsing Iceberg metadata files
type MetadataReader struct {
	BasePath string
}

// NewMetadataReader creates a new MetadataReader
func NewMetadataReader(basePath string) *MetadataReader {
	return &MetadataReader{
		BasePath: basePath,
	}
}

// ReadMetadata reads and parses the Iceberg metadata file
func (r *MetadataReader) ReadMetadata(tableName string) (*TableMetadata, error) {
	// Validate tableName to prevent directory traversal
	if !isValidTableName(tableName) {
		return nil, fmt.Errorf("invalid table name: %s", tableName)
	}

	metadataPath := filepath.Join(r.BasePath, tableName, "metadata", "v1.metadata.json")
	
	data, err := os.ReadFile(metadataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata file: %v", err)
	}

	var metadata TableMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("failed to parse metadata file: %v", err)
	}

	return &metadata, nil
}

// GetCurrentSnapshot returns the current snapshot ID and manifest list
func (r *MetadataReader) GetCurrentSnapshot(tableName string) (int64, string, error) {
	metadata, err := r.ReadMetadata(tableName)
	if err != nil {
		return 0, "", err
	}

	if metadata.CurrentSnapshotID == 0 {
		return 0, "", fmt.Errorf("no current snapshot found")
	}

	// Find the current snapshot
	for _, snapshot := range metadata.Snapshots {
		if snapshot.SnapshotID == metadata.CurrentSnapshotID {
			return snapshot.SnapshotID, snapshot.ManifestList, nil
		}
	}

	return 0, "", fmt.Errorf("current snapshot not found in snapshots list")
} 