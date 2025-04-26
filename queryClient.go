// queryClient.go
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
	"github.com/gigapi/gigapi-querier/core"
)

var db *sql.DB

// Ensure QueryClient implements core.QueryClient interface
var _ core.QueryClient = (*QueryClient)(nil)

// QueryClient handles parsing SQL and querying parquet files
type QueryClient struct {
	DataDir          string
	DB               *sql.DB
	DefaultTimeRange int64 // 10 minutes in nanoseconds
}

// NewQueryClient creates a new QueryClient
func NewQueryClient(dataDir string) *QueryClient {
	return &QueryClient{
		DataDir:          dataDir,
		DefaultTimeRange: 10 * 60 * 1000000000, // 10 minutes in nanoseconds
	}
}

// Initialize sets up the DuckDB connection
func (q *QueryClient) Initialize() error {
	var err error
	db, err = sql.Open("duckdb", "?access_mode=READ_WRITE")
	if err != nil {
		return fmt.Errorf("failed to initialize DuckDB: %v", err)
	}
	q.DB = db
	return nil
}

// ParsedQuery contains the parsed components of a SQL query
type ParsedQuery struct {
	Columns         string
	DbName          string
	Measurement     string
	TimeRange       TimeRange
	WhereConditions string
	OrderBy         string
	GroupBy         string
	Having          string
	Limit           int
}

// TimeRange represents a query time range
type TimeRange struct {
	Start         *int64
	End           *int64
	TimeCondition string
}

// Parse SQL query to extract components
func (q *QueryClient) ParseQuery(sql, dbName string) (*ParsedQuery, error) {
	// Normalize whitespace
	sql = regexp.MustCompile(`\s+`).ReplaceAllString(sql, " ")
	sql = strings.TrimSpace(sql)

	// Extract columns
	columnsPattern := regexp.MustCompile(`(?i)SELECT\s+(.*?)\s+FROM`)
	columnsMatch := columnsPattern.FindStringSubmatch(sql)
	columns := "*"
	if len(columnsMatch) > 1 {
		columns = strings.TrimSpace(columnsMatch[1])
	}

	// Extract measurement name
	fromPattern := regexp.MustCompile(`(?i)FROM\s+(?:(\w+)\.)?(\w+)`)
	fromMatch := fromPattern.FindStringSubmatch(sql)
	if len(fromMatch) < 3 {
		return nil, fmt.Errorf("invalid query: FROM clause not found or invalid")
	}

	// If db name is in the query, use it, otherwise use the provided dbName
	queryDbName := dbName
	if fromMatch[1] != "" {
		queryDbName = fromMatch[1]
	}
	measurement := fromMatch[2]

	// Extract WHERE clause
	whereClause := ""
	whereParts := strings.Split(sql, " WHERE ")
	if len(whereParts) >= 2 {
		whereClause = whereParts[1]

		// Remove other clauses
		for _, clause := range []string{" GROUP BY ", " ORDER BY ", " LIMIT ", " HAVING "} {
			if idx := strings.Index(strings.ToUpper(whereClause), clause); idx != -1 {
				whereClause = whereClause[:idx]
			}
		}
	}

	// Extract time range
	timeRange := q.extractTimeRange(whereClause)

	// Extract other clauses
	orderBy := ""
	orderByPattern := regexp.MustCompile(`(?i)ORDER\s+BY\s+(.*?)(?:\s+(?:LIMIT|GROUP|HAVING|$))`)
	orderByMatch := orderByPattern.FindStringSubmatch(sql)
	if len(orderByMatch) > 1 {
		orderBy = strings.TrimSpace(orderByMatch[1])
	}

	groupBy := ""
	groupByPattern := regexp.MustCompile(`(?i)GROUP\s+BY\s+(.*?)(?:\s+(?:ORDER|LIMIT|HAVING|$))`)
	groupByMatch := groupByPattern.FindStringSubmatch(sql)
	if len(groupByMatch) > 1 {
		groupBy = strings.TrimSpace(groupByMatch[1])
	}

	having := ""
	havingPattern := regexp.MustCompile(`(?i)HAVING\s+(.*?)(?:\s+(?:ORDER|LIMIT|$))`)
	havingMatch := havingPattern.FindStringSubmatch(sql)
	if len(havingMatch) > 1 {
		having = strings.TrimSpace(havingMatch[1])
	}

	limit := 0
	limitPattern := regexp.MustCompile(`(?i)LIMIT\s+(\d+)`)
	limitMatch := limitPattern.FindStringSubmatch(sql)
	if len(limitMatch) > 1 {
		fmt.Sscanf(limitMatch[1], "%d", &limit)
	}

	return &ParsedQuery{
		Columns:         columns,
		DbName:          queryDbName,
		Measurement:     measurement,
		TimeRange:       timeRange,
		WhereConditions: whereClause,
		OrderBy:         orderBy,
		GroupBy:         groupBy,
		Having:          having,
		Limit:           limit,
	}, nil
}

// Extract time range from WHERE clause
func (q *QueryClient) extractTimeRange(whereClause string) TimeRange {
	timeRange := TimeRange{
		Start:         nil,
		End:           nil,
		TimeCondition: "",
	}

	if whereClause == "" {
		return timeRange
	}

	// Match time patterns
	timePatterns := []*regexp.Regexp{
		regexp.MustCompile(`time\s*(>=|>)\s*'([^']+)'`),                    // time >= '2023-01-01T00:00:00'
		regexp.MustCompile(`time\s*(<=|<)\s*'([^']+)'`),                    // time <= '2023-01-01T00:00:00'
		regexp.MustCompile(`time\s*=\s*'([^']+)'`),                         // time = '2023-01-01T00:00:00'
		regexp.MustCompile(`time\s+BETWEEN\s+'([^']+)'\s+AND\s+'([^']+)'`), // time BETWEEN '...' AND '...'
	}

	// Check for BETWEEN pattern first
	if match := timePatterns[3].FindStringSubmatch(whereClause); len(match) > 2 {
		startTime, err := time.Parse(time.RFC3339Nano, match[1])
		if err == nil {
			startNanos := startTime.UnixNano()
			timeRange.Start = &startNanos
		}

		endTime, err := time.Parse(time.RFC3339Nano, match[2])
		if err == nil {
			endNanos := endTime.UnixNano()
			timeRange.End = &endNanos
		}

		timeRange.TimeCondition = fmt.Sprintf("time BETWEEN '%s' AND '%s'", match[1], match[2])
		return timeRange
	}

	// Check for >= pattern
	if match := timePatterns[0].FindStringSubmatch(whereClause); len(match) > 2 {
		startTime, err := time.Parse(time.RFC3339Nano, match[2])
		if err == nil {
			startNanos := startTime.UnixNano()
			timeRange.Start = &startNanos
		}
		timeRange.TimeCondition = fmt.Sprintf("time %s '%s'", match[1], match[2])
	}

	// Check for <= pattern
	if match := timePatterns[1].FindStringSubmatch(whereClause); len(match) > 2 {
		endTime, err := time.Parse(time.RFC3339Nano, match[2])
		if err == nil {
			endNanos := endTime.UnixNano()
			timeRange.End = &endNanos
		}

		if timeRange.TimeCondition != "" {
			timeRange.TimeCondition = fmt.Sprintf("%s AND time %s '%s'", timeRange.TimeCondition, match[1], match[2])
		} else {
			timeRange.TimeCondition = fmt.Sprintf("time %s '%s'", match[1], match[2])
		}
	}

	// Check for = pattern
	if match := timePatterns[2].FindStringSubmatch(whereClause); len(match) > 1 {
		exactTime, err := time.Parse(time.RFC3339Nano, match[1])
		if err == nil {
			exactNanos := exactTime.UnixNano()
			timeRange.Start = &exactNanos
			timeRange.End = &exactNanos
		}
		timeRange.TimeCondition = fmt.Sprintf("time = '%s'", match[1])
	}

	return timeRange
}

// MetadataFile represents a metadata.json file structure
type MetadataFile struct {
	Type             string        `json:"type"`
	ParquetSizeBytes int           `json:"parquet_size_bytes"`
	RowCount         int           `json:"row_count"`
	MinTime          int64         `json:"min_time"`
	MaxTime          int64         `json:"max_time"`
	Files            []ParquetFile `json:"files"`
}

// ParquetFile represents a single parquet file entry in metadata
type ParquetFile struct {
	Path      string `json:"path"`
	SizeBytes int    `json:"size_bytes"`
	RowCount  int    `json:"row_count"`
	MinTime   int64  `json:"min_time"`
	MaxTime   int64  `json:"max_time"`
}

func (q *QueryClient) enumFolderWithMetadata(metadataPath string, timeRange TimeRange) ([]string, error) {
	metadataBytes, err := os.ReadFile(metadataPath)
	if err != nil {
		return nil, err
	}

	var metadata MetadataFile
	if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
		return nil, err
	}

	// Skip if metadata time range doesn't overlap with requested time range
	if timeRange.Start != nil && timeRange.End != nil &&
		(metadata.MaxTime < *timeRange.Start || metadata.MinTime > *timeRange.End) {
		return []string{}, nil
	}

	var res []string

	// Check each file in metadata
	for _, file := range metadata.Files {
		// Skip if file time range doesn't overlap with requested time range
		if timeRange.Start != nil && timeRange.End != nil &&
			(file.MaxTime < *timeRange.Start || file.MinTime > *timeRange.End) {
			continue
		}

		// Check if the file exists at the given path
		if _, err := os.Stat(file.Path); err == nil {
			res = append(res, file.Path)
		}
	}
	return res, nil
}

func (q *QueryClient) enumFolderNoMetadata(path string) ([]string, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	res := make([]string, len(entries))
	for i, entry := range entries {
		name := entry.Name()
		if strings.HasSuffix(name, ".parquet") {
			res[i] = filepath.Join(path, entries[i].Name())
		}
	}
	return res, nil
}

// Find relevant parquet files based on time range
func (q *QueryClient) FindRelevantFiles(ctx context.Context, dbName, measurement string,
	timeRange TimeRange) ([]string, error) {
	// If no time range specified, get all files
	if timeRange.Start == nil && timeRange.End == nil {
		Infof(ctx, "No time range specified, getting all files for %s.%s", dbName, measurement)
		return q.findAllFiles(ctx, dbName, measurement)
	}

	var relevantFiles []string
	Infof(ctx, "Getting relevant files for %s.%s within time range %v to %v", dbName, measurement,
		time.Unix(0, *timeRange.Start), time.Unix(0, *timeRange.End))
	start := time.Now()
	defer func() {
		Infof(ctx, "Found %d files in: %v", len(relevantFiles), time.Since(start))
	}()

	// Convert nanosecond timestamps to time.Time for directory parsing
	var startDate, endDate time.Time
	if timeRange.Start != nil {
		startDate = time.Unix(0, *timeRange.Start)
	} else {
		startDate = time.Unix(0, 0) // Beginning of epoch
	}

	if timeRange.End != nil {
		endDate = time.Unix(0, *timeRange.End)
	} else {
		endDate = time.Now() // Current time
	}

	// Get all date directories that might contain relevant data
	Infof(ctx, "Looking for date directories between %v and %v", startDate, endDate)
	dateDirectories, err := q.getDateDirectoriesInRange(dbName, measurement, startDate, endDate)
	if err != nil {
		Errorf(ctx, "Failed to get date directories: %v", err)
		return nil, err
	}
	Infof(ctx, "Found %d date directories", len(dateDirectories))

	for _, dateDir := range dateDirectories {
		// For each date directory, get all hour directories
		datePath := filepath.Join(q.DataDir, dbName, measurement, dateDir)
		Infof(ctx, "Processing date directory: %s", datePath)
		
		hourDirs, err := q.getHourDirectoriesInRange(datePath, startDate, endDate)
		if err != nil {
			Errorf(ctx, "Failed to get hour directories for %s: %v", datePath, err)
			continue // Skip this directory on error
		}
		Infof(ctx, "Found %d hour directories in %s", len(hourDirs), dateDir)

		for _, hourDir := range hourDirs {
			hourPath := filepath.Join(datePath, hourDir)
			Infof(ctx, "Processing hour directory: %s", hourPath)

			// Read metadata.json
			metadataPath := filepath.Join(hourPath, "metadata.json")
			if _, err := os.Stat(metadataPath); err == nil {
				Infof(ctx, "Found metadata.json in %s", hourPath)
				_relevantFiles, err := q.enumFolderWithMetadata(metadataPath, timeRange)
				if err == nil {
					Infof(ctx, "Found %d files in metadata.json", len(_relevantFiles))
					relevantFiles = append(relevantFiles, _relevantFiles...)
					continue
				}
				Errorf(ctx, "Failed to read metadata.json: %v", err)
			}

			_relevantFiles, err := q.enumFolderNoMetadata(hourPath)
			if err == nil {
				Infof(ctx, "Found %d files without metadata", len(_relevantFiles))
				relevantFiles = append(relevantFiles, _relevantFiles...)
				continue
			}
			Errorf(ctx, "Failed to enumerate folder: %v", err)
		}
	}

	if len(relevantFiles) == 0 {
		Errorf(ctx, "No files found in any directory for %s.%s", dbName, measurement)
	}

	return relevantFiles, nil
}

// Find all files for a measurement
func (q *QueryClient) findAllFiles(ctx context.Context, dbName, measurement string) ([]string, error) {
	var allFiles []string
	basePath := filepath.Join(q.DataDir, dbName, measurement)

	Debugf(ctx, "Getting all files for %s.%s", dbName, measurement)
	start := time.Now()
	defer func() {
		Debugf(ctx, "Found %d files in: %v", len(allFiles), time.Since(start))
	}()

	if _, err := os.Stat(basePath); os.IsNotExist(err) {
		return allFiles, nil
	}

	// Recursively find all parquet files
	err := filepath.Walk(basePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Skip errors
		}

		if info.Name() == "tmp" && info.IsDir() {
			// Skipping tmp directory as it may include half-created parquet files
			return filepath.SkipDir
		}

		if info.IsDir() {
			if _, err := os.Stat(filepath.Join(path, "metadata.json")); err == nil {
				metadataBytes, err := os.ReadFile(filepath.Join(path, "metadata.json"))
				if err == nil {
					var metadata MetadataFile
					if err := json.Unmarshal(metadataBytes, &metadata); err == nil {
						for _, file := range metadata.Files {
							if _, err := os.Stat(file.Path); err == nil {
								allFiles = append(allFiles, file.Path)
							} else {
								// Try relative path
								relPath := filepath.Join(filepath.Dir(path), filepath.Base(file.Path))
								if _, err := os.Stat(relPath); err == nil {
									allFiles = append(allFiles, relPath)
								}
							}
						}
					}
				}
				return filepath.SkipDir
			}
		}

		if !info.IsDir() {
			if strings.HasSuffix(info.Name(), ".parquet") {
				// Direct parquet file
				allFiles = append(allFiles, path)
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return allFiles, nil
}

// Get date directories in range
func (q *QueryClient) getDateDirectoriesInRange(dbName, measurement string, startDate, endDate time.Time) ([]string, error) {
	basePath := filepath.Join(q.DataDir, dbName, measurement)
	if _, err := os.Stat(basePath); os.IsNotExist(err) {
		return nil, err
	}

	entries, err := os.ReadDir(basePath)
	if err != nil {
		return nil, err
	}

	var dateDirs []string
	datePattern := regexp.MustCompile(`^date=(.+)$`)

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		matches := datePattern.FindStringSubmatch(entry.Name())
		if len(matches) < 2 {
			continue
		}

		dateStr := matches[1]
		dirDate, err := time.Parse("2006-01-02", dateStr)
		if err != nil {
			continue
		}

		// Check if directory date is within range
		dirDateOnly := time.Date(dirDate.Year(), dirDate.Month(), dirDate.Day(), 0, 0, 0, 0, time.UTC)
		startDateOnly := time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 0, 0, 0, 0, time.UTC)
		endDateOnly := time.Date(endDate.Year(), endDate.Month(), endDate.Day(), 0, 0, 0, 0, time.UTC)

		if !dirDateOnly.Before(startDateOnly) && !dirDateOnly.After(endDateOnly) {
			dateDirs = append(dateDirs, entry.Name())
		}
	}

	return dateDirs, nil
}

// Get hour directories in range
func (q *QueryClient) getHourDirectoriesInRange(datePath string, startDate, endDate time.Time) ([]string, error) {
	if _, err := os.Stat(datePath); os.IsNotExist(err) {
		return nil, err
	}

	entries, err := os.ReadDir(datePath)
	if err != nil {
		return nil, err
	}

	var hourDirs []string
	hourPattern := regexp.MustCompile(`^hour=(\d+)$`)
	datePattern := regexp.MustCompile(`^date=(.+)$`)

	dateMatches := datePattern.FindStringSubmatch(filepath.Base(datePath))
	if len(dateMatches) < 2 {
		return hourDirs, nil
	}

	dateStr := dateMatches[1]
	dirDate, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		return hourDirs, nil
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		matches := hourPattern.FindStringSubmatch(entry.Name())
		if len(matches) < 2 {
			continue
		}

		hour := 0
		fmt.Sscanf(matches[1], "%d", &hour)

		// Same day comparison logic
		if dirDate.Year() == startDate.Year() && dirDate.Month() == startDate.Month() && dirDate.Day() == startDate.Day() &&
			dirDate.Year() == endDate.Year() && dirDate.Month() == endDate.Month() && dirDate.Day() == endDate.Day() {
			// Same day, filter by hour
			if hour >= startDate.Hour() && hour <= endDate.Hour() {
				hourDirs = append(hourDirs, entry.Name())
			}
		} else if dirDate.Year() == startDate.Year() && dirDate.Month() == startDate.Month() && dirDate.Day() == startDate.Day() {
			// Start date, include hours >= start hour
			if hour >= startDate.Hour() {
				hourDirs = append(hourDirs, entry.Name())
			}
		} else if dirDate.Year() == endDate.Year() && dirDate.Month() == endDate.Month() && dirDate.Day() == endDate.Day() {
			// End date, include hours <= end hour
			if hour <= endDate.Hour() {
				hourDirs = append(hourDirs, entry.Name())
			}
		} else {
			// Not start or end date, include all hours
			hourDirs = append(hourDirs, entry.Name())
		}
	}

	return hourDirs, nil
}

// Query executes a query against the database
func (c *QueryClient) Query(ctx context.Context, query, dbName string) ([]map[string]interface{}, error) {
	// Ensure we have a context
	if ctx == nil {
		ctx = context.Background()
	}

	// Check for special commands
	query = strings.TrimSpace(query)
	upperQuery := strings.ToUpper(query)

	// Handle special commands
	switch upperQuery {
	case "SHOW DATABASES":
		entries, err := os.ReadDir(c.DataDir)
		if err != nil {
			return nil, fmt.Errorf("failed to read data directory: %v", err)
		}

		results := make([]map[string]interface{}, 0)
		for _, entry := range entries {
			if entry.IsDir() {
				results = append(results, map[string]interface{}{
					"database_name": entry.Name(),
				})
			}
		}
		return results, nil

	case "SHOW TABLES":
		// List directories inside the database folder
		dbPath := filepath.Join(c.DataDir, dbName)
		entries, err := os.ReadDir(dbPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read database directory: %v", err)
		}

		results := make([]map[string]interface{}, 0)
		for _, entry := range entries {
			if entry.IsDir() {
				results = append(results, map[string]interface{}{
					"table_name": entry.Name(),
				})
			}
		}
		return results, nil
	}

	// Handle regular queries through DuckDB
	// Parse the query
	parsed, err := c.ParseQuery(query, dbName)
	if err != nil {
		return nil, err
	}

	// Find relevant files
	files, err := c.FindRelevantFiles(ctx, parsed.DbName, parsed.Measurement, parsed.TimeRange)
	if err != nil || len(files) == 0 {
		return nil, fmt.Errorf("no relevant files found for query")
	}

	start := time.Now()

	// Build the DuckDB query
	var filesList strings.Builder
	for i, file := range files {
		if i > 0 {
			filesList.WriteString(", ")
		}
		filesList.WriteString(fmt.Sprintf("'%s'", file))
	}

	// Split the original query and rebuild with file list
	originalParts := strings.SplitN(query, " FROM ", 2)
	var duckdbQuery string

	if len(originalParts) >= 2 {
		// Extract table name pattern to replace
		tablePattern := fmt.Sprintf(`(?:%s\.)?%s\b`, parsed.DbName, parsed.Measurement)
		tableRegex := regexp.MustCompile(tablePattern)
		restOfQuery := tableRegex.ReplaceAllString(originalParts[1], "")

		if len(strings.TrimSpace(restOfQuery)) > 0 {
			// Fix timestamp format - ensure timestamps have quotes
			timestampRegex := regexp.MustCompile(`([^'])(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?)`)
			restOfQuery = timestampRegex.ReplaceAllString(restOfQuery, "$1'$2'")

			duckdbQuery = fmt.Sprintf("%s FROM read_parquet([%s], union_by_name=true) %s",
				originalParts[0], filesList.String(), restOfQuery)
		} else {
			duckdbQuery = fmt.Sprintf("%s FROM read_parquet([%s], union_by_name=true)",
				originalParts[0], filesList.String())
		}
	} else {
		// Fallback to manually constructing the query
		duckdbQuery = fmt.Sprintf("SELECT %s FROM read_parquet([%s], union_by_name=true)",
			parsed.Columns, filesList.String())

		// Add WHERE conditions
		if parsed.TimeRange.TimeCondition != "" || len(parsed.WhereConditions) > 0 {
			var conditions []string

			if parsed.TimeRange.TimeCondition != "" {
				conditions = append(conditions, parsed.TimeRange.TimeCondition)
			}

			if len(parsed.WhereConditions) > 0 {
				// Fix timestamp format in WHERE clause
				timestampRegex := regexp.MustCompile(`([^'])(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?)`)
				processedCond := timestampRegex.ReplaceAllString(parsed.WhereConditions, "$1'$2'")
				conditions = append(conditions, processedCond)
			}

			if len(conditions) > 0 {
				duckdbQuery += " WHERE " + strings.Join(conditions, " AND ")
			}
		}

		// Add GROUP BY, HAVING, ORDER BY, and LIMIT
		if len(parsed.GroupBy) > 0 {
			duckdbQuery += " GROUP BY " + parsed.GroupBy
		}

		if len(parsed.Having) > 0 {
			duckdbQuery += " HAVING " + parsed.Having
		}

		if len(parsed.OrderBy) > 0 {
			duckdbQuery += " ORDER BY " + parsed.OrderBy
		}

		if parsed.Limit > 0 {
			duckdbQuery += fmt.Sprintf(" LIMIT %d", parsed.Limit)
		}
	}

	Debugf(ctx, "Created DuckDB query in: %v. Query: %s", time.Since(start), duckdbQuery)
	start = time.Now()

	// Execute the query
	stmt, err := c.DB.Prepare(duckdbQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare query: %v", err)
	}
	defer stmt.Close()

	rows, err := stmt.Query()
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %v", err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %v", err)
	}

	Debugf(ctx, "Retrieved first query result in: %v", time.Since(start))
	start = time.Now()

	// Prepare result structure
	var result []map[string]interface{}

	// Process rows
	for rows.Next() {
		// Create a slice of interface{} to hold the values
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))

		// Set up pointers to each interface{}
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// Scan the row into the values
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("error scanning row: %v", err)
		}

		// Create a map for this row
		row := make(map[string]interface{})

		// Set each column in the map
		for i, col := range columns {
			val := values[i]

			// Handle special cases for counts
			if strings.Contains(col, "count") && val == nil {
				row[col] = 0
			} else {
				row[col] = val
			}
		}

		result = append(result, row)
	}

	Debugf(ctx, "Got query result in: %v", time.Since(start))

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %v", err)
	}

	return result, nil
}

// Close releases resources
func (q *QueryClient) Close() error {
	if q.DB != nil {
		return q.DB.Close()
	}
	return nil
}
