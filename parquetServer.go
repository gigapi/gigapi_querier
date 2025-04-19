package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
)

// ParquetServer handles HTTP requests for virtual parquet files
type ParquetServer struct {
	queryClient *QueryClient
}

// NewParquetServer creates a new ParquetServer instance
func NewParquetServer(dataDir string) (*ParquetServer, error) {
	qc := NewQueryClient(dataDir)
	if err := qc.Initialize(); err != nil {
		return nil, fmt.Errorf("failed to initialize query client: %v", err)
	}

	return &ParquetServer{
		queryClient: qc,
	}, nil
}

// Start initializes and starts the HTTP server
func (s *ParquetServer) Start(port int) error {
	r := mux.NewRouter()

	// Route for accessing Arrow streaming - handle both GET and HEAD
	r.HandleFunc("/arrow/{db}/{measurement}", s.handleArrowRequest).Methods("GET", "HEAD")
	
	// Route for getting schema information
	r.HandleFunc("/schema/{db}/{measurement}", s.handleSchemaRequest).Methods("GET")

	addr := fmt.Sprintf(":%d", port)
	fmt.Printf("Starting Arrow Server on %s\n", addr)
	return http.ListenAndServe(addr, r)
}

func (s *ParquetServer) handleSchemaRequest(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	dbName := vars["db"]
	measurement := vars["measurement"]

	// Find the first parquet file to extract schema
	files, err := s.queryClient.FindRelevantFiles(dbName, measurement, TimeRange{})
	if err != nil || len(files) == 0 {
		http.Error(w, "No files found", http.StatusNotFound)
		return
	}

	// Use DuckDB to get schema information
	query := fmt.Sprintf("DESCRIBE SELECT * FROM read_parquet('%s')", files[0])
	schema, err := s.queryClient.Query(query, dbName)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get schema: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(schema)
}

func (s *ParquetServer) handleArrowRequest(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	dbName := vars["db"]
	measurement := vars["measurement"]

	// Log incoming request
	log.Printf("Arrow request: %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)

	// Parse query parameters for filtering
	timeRange := s.parseTimeRange(r.URL.Query())
	filters := s.parseFilters(r.URL.Query())

	// Verify that we have data available
	files, err := s.queryClient.FindRelevantFiles(dbName, measurement, timeRange)
	if err != nil {
		log.Printf("Error finding files: %v", err)
		http.Error(w, fmt.Sprintf("Error finding files: %v", err), http.StatusInternalServerError)
		return
	}
	
	if len(files) == 0 {
		log.Printf("No files found for db=%s measurement=%s timeRange=%+v", dbName, measurement, timeRange)
		http.Error(w, "No data found", http.StatusNotFound)
		return
	}

	log.Printf("Found %d files for request", len(files))

	// Set headers for Arrow streaming
	w.Header().Set("Content-Type", "application/x-arrow")
	
	// For HEAD requests, we're done
	if r.Method == "HEAD" {
		return
	}

	// Build and execute query
	config := s.parseStreamConfig(r.URL.Query())
	query := s.buildVirtualParquetQuery(dbName, measurement, timeRange, filters)

	log.Printf("Executing query: %s", query)

	if err := s.queryClient.StreamParquetResultsWithConfig(query, dbName, w, config); err != nil {
		log.Printf("Error streaming results: %v", err)
		http.Error(w, fmt.Sprintf("Failed to stream results: %v", err), http.StatusInternalServerError)
		return
	}
}

func (s *ParquetServer) buildVirtualParquetQuery(dbName, measurement string, timeRange TimeRange, filters map[string]string) string {
	// Use the existing QueryClient functionality
	parsed := &ParsedQuery{
		DbName:      dbName,
		Measurement: measurement,
		TimeRange:   timeRange,
	}

	// Find relevant files
	files, err := s.queryClient.FindRelevantFiles(dbName, measurement, timeRange)
	if err != nil {
		return ""
	}

	// Build file list for DuckDB
	var filesList strings.Builder
	for i, file := range files {
		if i > 0 {
			filesList.WriteString(", ")
		}
		filesList.WriteString(fmt.Sprintf("'%s'", file))
	}

	// Build the query using read_parquet
	query := fmt.Sprintf("SELECT * FROM read_parquet([%s], union_by_name=true)", filesList.String())

	// Add time conditions
	var conditions []string
	if timeRange.Start != nil {
		startTime := time.Unix(0, *timeRange.Start).UTC()
		conditions = append(conditions, fmt.Sprintf("time >= epoch_ns(TIMESTAMP '%s')", 
			startTime.Format("2006-01-02 15:04:05.999999999")))
	}
	if timeRange.End != nil {
		endTime := time.Unix(0, *timeRange.End).UTC()
		conditions = append(conditions, fmt.Sprintf("time <= epoch_ns(TIMESTAMP '%s')", 
			endTime.Format("2006-01-02 15:04:05.999999999")))
	}

	// Add other filters
	for col, val := range filters {
		if col == "time" {
			continue
		}
		val = strings.TrimSpace(val)
		if val == "" {
			continue
		}
		if _, err := strconv.ParseFloat(val, 64); err == nil {
			conditions = append(conditions, fmt.Sprintf("%s = %s", col, val))
		} else {
			val = strings.ReplaceAll(val, "'", "''")
			conditions = append(conditions, fmt.Sprintf("%s = '%s'", col, val))
		}
	}

	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}

	return query
}

func (s *ParquetServer) parseFilters(params map[string][]string) map[string]string {
	filters := make(map[string]string)
	for key, values := range params {
		if key != "start" && key != "end" && key != "limit" && key != "offset" {
			filters[key] = values[0]
		}
	}
	return filters
}

func (s *ParquetServer) parseTimeRange(params map[string][]string) TimeRange {
	timeRange := TimeRange{}

	if startStrArr, ok := params["start"]; ok && len(startStrArr) > 0 {
		startStr := startStrArr[0]
		// Try different time formats
		startTime, err := parseTimeWithFormats(startStr)
		if err == nil {
			startNanos := startTime.UnixNano()
			timeRange.Start = &startNanos
		} else {
			log.Printf("Failed to parse start time '%s': %v", startStr, err)
		}
	}

	if endStrArr, ok := params["end"]; ok && len(endStrArr) > 0 {
		endStr := endStrArr[0]
		// Try different time formats
		endTime, err := parseTimeWithFormats(endStr)
		if err == nil {
			endNanos := endTime.UnixNano()
			timeRange.End = &endNanos
		} else {
			log.Printf("Failed to parse end time '%s': %v", endStr, err)
		}
	}

	return timeRange
}

func parseTimeWithFormats(timeStr string) (time.Time, error) {
	formats := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, timeStr); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("could not parse time with any known format")
}

func (s *ParquetServer) parseStreamConfig(params map[string][]string) StreamConfig {
	config := DefaultStreamConfig()

	// Parse chunk size
	if chunkSize, ok := params["chunk_size"]; ok && len(chunkSize) > 0 {
		if size, err := strconv.Atoi(chunkSize[0]); err == nil {
			config.ChunkSize = size
		}
	}

	return config
} 