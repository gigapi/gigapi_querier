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
	fullPath := filepath.Join(p.BasePath, tableName, manifestListPath)
	
	data, err := os.ReadFile(fullPath)
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
	fullPath := filepath.Join(p.BasePath, tableName, manifestPath)
	
	data, err := os.ReadFile(fullPath)
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