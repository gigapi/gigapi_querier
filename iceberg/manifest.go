package iceberg

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// ManifestList represents the Iceberg manifest list structure
type ManifestList struct {
	Manifests []ManifestEntry `json:"manifests"`
}

// ManifestEntry represents an entry in the manifest list
type ManifestEntry struct {
	ManifestPath string `json:"manifest_path"`
	ManifestLength int64 `json:"manifest_length"`
	PartitionSpecID int `json:"partition_spec_id"`
	AddedSnapshotID int64 `json:"added_snapshot_id"`
	AddedFilesCount int `json:"added_files_count"`
	ExistingFilesCount int `json:"existing_files_count"`
	DeletedFilesCount int `json:"deleted_files_count"`
	PartitionSummaries []PartitionSummary `json:"partition_summaries"`
	Status string `json:"status"`
	DataFile DataFile `json:"data_file"`
}

// PartitionSummary represents a summary of partition values
type PartitionSummary struct {
	ContainsNull bool `json:"contains_null"`
	LowerBound string `json:"lower_bound"`
	UpperBound string `json:"upper_bound"`
}

// Manifest represents an Iceberg manifest file structure
type Manifest struct {
	Schema Schema `json:"schema"`
	PartitionSpecID int `json:"partition_spec_id"`
	Content int `json:"content"`
	SequenceNumber int64 `json:"sequence_number"`
	MinSequenceNumber int64 `json:"min_sequence_number"`
	AddedSnapshotID int64 `json:"added_snapshot_id"`
	AddedFilesCount int `json:"added_files_count"`
	ExistingFilesCount int `json:"existing_files_count"`
	DeletedFilesCount int `json:"deleted_files_count"`
	Entries []ManifestEntry `json:"entries"`
}

// DataFile represents a data file entry in a manifest
type DataFile struct {
	Path string `json:"path"`
	Format string `json:"format"`
	Partition map[string]string `json:"partition"`
	RecordCount int64 `json:"record_count"`
	FileSizeInBytes int64 `json:"file_size_in_bytes"`
	ColumnSizes map[int]int64 `json:"column_sizes"`
	ValueCounts map[int]int64 `json:"value_counts"`
	NullValueCounts map[int]int64 `json:"null_value_counts"`
	LowerBounds map[int][]byte `json:"lower_bounds"`
	UpperBounds map[int][]byte `json:"upper_bounds"`
	KeyMetadata []byte `json:"key_metadata"`
	SplitOffsets []int64 `json:"split_offsets"`
	EqualityIds []int `json:"equality_ids"`
	SortOrderId int `json:"sort_order_id"`
}

// ManifestProcessor handles reading and processing Iceberg manifest files
type ManifestProcessor struct {
	BasePath string
}

// NewManifestProcessor creates a new ManifestProcessor
func NewManifestProcessor(basePath string) *ManifestProcessor {
	return &ManifestProcessor{
		BasePath: basePath,
	}
}

// ReadManifestList reads and parses the manifest list file
func (p *ManifestProcessor) ReadManifestList(tableName, manifestListPath string) (*ManifestList, error) {
	// Validate inputs
	if strings.Contains(tableName, "..") || strings.Contains(manifestListPath, "..") ||
		strings.ContainsAny(tableName, "/\\") || strings.ContainsAny(manifestListPath, "/\\") {
		return nil, fmt.Errorf("invalid tableName or manifestListPath")
	}

	fullPath := filepath.Join(p.BasePath, tableName, manifestListPath)
	absPath, err := filepath.Abs(fullPath)
	if err != nil || !strings.HasPrefix(absPath, p.BasePath) {
		return nil, fmt.Errorf("invalid resolved path")
	}

	data, err := os.ReadFile(absPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest list: %v", err)
	}

	var manifestList ManifestList
	if err := json.Unmarshal(data, &manifestList); err != nil {
		return nil, fmt.Errorf("failed to parse manifest list: %v", err)
	}

	return &manifestList, nil
}

// ReadManifest reads and parses a single manifest file
func (p *ManifestProcessor) ReadManifest(tableName, manifestPath string) (*Manifest, error) {
	// Validate inputs
	if strings.Contains(tableName, "..") || strings.Contains(manifestPath, "..") ||
		strings.ContainsAny(tableName, "/\\") || strings.ContainsAny(manifestPath, "/\\") {
		return nil, fmt.Errorf("invalid tableName or manifestPath")
	}

	fullPath := filepath.Join(p.BasePath, tableName, manifestPath)
	absPath, err := filepath.Abs(fullPath)
	if err != nil || !strings.HasPrefix(absPath, p.BasePath) {
		return nil, fmt.Errorf("invalid resolved path")
	}

	data, err := os.ReadFile(absPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest: %v", err)
	}

	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("failed to parse manifest: %v", err)
	}

	return &manifest, nil
}

// GetDataFiles returns all data files referenced in the manifest list
func (p *ManifestProcessor) GetDataFiles(tableName, manifestListPath string) ([]string, error) {
	manifestList, err := p.ReadManifestList(tableName, manifestListPath)
	if err != nil {
		return nil, err
	}

	var dataFiles []string

	for _, entry := range manifestList.Manifests {
		manifest, err := p.ReadManifest(tableName, entry.ManifestPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read manifest %s: %v", entry.ManifestPath, err)
		}

		for _, manifestEntry := range manifest.Entries {
			if manifestEntry.Status == "ADDED" || manifestEntry.Status == "EXISTING" {
				dataFiles = append(dataFiles, manifestEntry.DataFile.Path)
			}
		}
	}

	return dataFiles, nil
} 