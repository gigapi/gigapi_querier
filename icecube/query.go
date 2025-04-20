package icecube

import (
	"fmt"
	"strings"
	"time"
)

// QueryOptions represents options for querying table data
type QueryOptions struct {
	StartTime *time.Time
	EndTime   *time.Time
	Filters   map[string]string
}

// Int64ToTime converts nanosecond timestamp to *time.Time
func Int64ToTime(ts *int64) *time.Time {
	if ts == nil {
		return nil
	}
	t := time.Unix(0, *ts)
	return &t
}

// GetQueryableFiles returns a list of parquet files that match the query criteria
func (c *Catalog) GetQueryableFiles(namespace, table string, opts QueryOptions) ([]string, error) {
	metadata, err := c.GetTableMetadata(namespace, table)
	if err != nil {
		return nil, err
	}

	var files []string
	for _, file := range metadata.Files {
		// Apply time range filter
		if opts.StartTime != nil && file.MaxTime < opts.StartTime.UnixNano() {
			continue
		}
		if opts.EndTime != nil && file.MinTime > opts.EndTime.UnixNano() {
			continue
		}
		files = append(files, file.Path)
	}

	return files, nil
}

// GetDuckDBQuery returns a DuckDB-compatible query for the table
func (c *Catalog) GetDuckDBQuery(namespace, table string, opts QueryOptions) (string, error) {
	files, err := c.GetQueryableFiles(namespace, table, opts)
	if err != nil {
		return "", err
	}

	// Build file list
	var fileList strings.Builder
	for i, file := range files {
		if i > 0 {
			fileList.WriteString(", ")
		}
		fileList.WriteString(fmt.Sprintf("'%s'", file))
	}

	query := fmt.Sprintf("SELECT * FROM read_parquet([%s], union_by_name=true)", fileList.String())

	// Add time conditions
	var conditions []string
	if opts.StartTime != nil {
		conditions = append(conditions, 
			fmt.Sprintf("time >= epoch_ns(TIMESTAMP '%s')", 
				opts.StartTime.Format("2006-01-02 15:04:05.999999999")))
	}
	if opts.EndTime != nil {
		conditions = append(conditions, 
			fmt.Sprintf("time <= epoch_ns(TIMESTAMP '%s')", 
				opts.EndTime.Format("2006-01-02 15:04:05.999999999")))
	}

	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}

	return query, nil
} 