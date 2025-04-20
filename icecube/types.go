package icecube

// TableMetadata represents metadata for a table
type TableMetadata struct {
	Type             string         `json:"type"`
	ParquetSizeBytes int64         `json:"parquet_size_bytes"`
	RowCount         int64         `json:"row_count"`
	MinTime          int64         `json:"min_time"`
	MaxTime          int64         `json:"max_time"`
	WalSequence      int64         `json:"wal_sequence"`
	Files            []FileMetadata `json:"files"`
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