package icecube

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

// ListTables returns all tables in a namespace (database)
func (c *Catalog) ListTables(namespace string) ([]string, error) {
	dbPath := filepath.Join(c.RootPath, namespace)
	entries, err := os.ReadDir(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read namespace directory: %v", err)
	}

	var tables []string
	for _, entry := range entries {
		if entry.IsDir() {
			tables = append(tables, entry.Name())
		}
	}
	return tables, nil
}

// GetTableMetadata returns combined metadata for a table
func (c *Catalog) GetTableMetadata(namespace, table string) (*TableMetadata, error) {
	tablePath := filepath.Join(c.RootPath, namespace, table)
	
	// Walk through date/hour partitions to find metadata.json files
	var combined TableMetadata
	combined.Type = table
	
	err := filepath.Walk(tablePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		
		if info.Name() == "metadata.json" {
			data, err := ioutil.ReadFile(path)
			if err != nil {
				return fmt.Errorf("failed to read metadata file: %v", err)
			}
			
			var partitionMeta TableMetadata
			if err := json.Unmarshal(data, &partitionMeta); err != nil {
				return fmt.Errorf("failed to parse metadata: %v", err)
			}
			
			// Merge metadata
			combined.ParquetSizeBytes += partitionMeta.ParquetSizeBytes
			combined.RowCount += partitionMeta.RowCount
			if combined.MinTime == 0 || partitionMeta.MinTime < combined.MinTime {
				combined.MinTime = partitionMeta.MinTime
			}
			if partitionMeta.MaxTime > combined.MaxTime {
				combined.MaxTime = partitionMeta.MaxTime
			}
			if partitionMeta.WalSequence > combined.WalSequence {
				combined.WalSequence = partitionMeta.WalSequence
			}
			combined.Files = append(combined.Files, partitionMeta.Files...)
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to walk table directory: %v", err)
	}

	return &combined, nil
}

// GetPartitionMetadata returns metadata for a specific partition
func (c *Catalog) GetPartitionMetadata(namespace, table, date, hour string) (*TableMetadata, error) {
	metadataPath := filepath.Join(c.RootPath, namespace, table, 
		fmt.Sprintf("date=%s", date), 
		fmt.Sprintf("hour=%s", hour),
		"metadata.json")
	
	data, err := ioutil.ReadFile(metadataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read partition metadata: %v", err)
	}
	
	var metadata TableMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("failed to parse metadata: %v", err)
	}
	
	return &metadata, nil
} 