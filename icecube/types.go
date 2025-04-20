package icecube

import "time"

// TableFormat represents supported table formats
type TableFormat string

const (
	FormatParquet TableFormat = "parquet"
	FormatDelta   TableFormat = "delta"
)

// FileInfo represents common file metadata across formats
type FileInfo struct {
	Path      string
	SizeBytes int64
	RowCount  int64
	MinTime   int64
	MaxTime   int64
	Format    TableFormat
	Metadata  map[string]interface{}
}

// TableMetadata represents common table metadata
type TableMetadata struct {
	Name             string
	Format           TableFormat
	PartitionColumns []string
	Schema           map[string]string
	Statistics       TableStatistics
}

// TableStatistics holds table-level statistics
type TableStatistics struct {
	NumFiles        int64
	TotalRows       int64
	TotalSizeBytes  int64
	MinTime         int64
	MaxTime         int64
}

// Storage defines the interface for different table storage formats
type Storage interface {
	ListTables(namespace string) ([]string, error)
	GetTableMetadata(namespace, table string) (*TableMetadata, error)
	GetFiles(namespace, table string, startTime, endTime *time.Time) ([]FileInfo, error)
	BuildQuery(files []FileInfo, startTime, endTime *time.Time, filters map[string]string) (string, error)
}

// FileMetadata represents metadata for a single parquet file
type FileMetadata struct {
	ID        int64  `json:"id"`
	Path      string `json:"path"`
	SizeBytes int64  `json:"size_bytes"`
	RowCount  int64  `json:"row_count"`
	ChunkTime int64  `json:"chunk_time"`
	MinTime   int64  `json:"min_time"`
	MaxTime   int64  `json:"max_time"`
	Range     string `json:"range"`
	Type      string `json:"type"`
}

// Catalog manages table metadata
type Catalog struct {
	RootPath string
} 