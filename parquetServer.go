package main

import (
	"encoding/json"
	"fmt"
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

	// Route for accessing virtual parquet files
	r.HandleFunc("/parquet/{db}/{measurement}", s.handleParquetRequest).Methods("GET")
	
	// Route for getting schema information
	r.HandleFunc("/schema/{db}/{measurement}", s.handleSchemaRequest).Methods("GET")

	addr := fmt.Sprintf(":%d", port)
	fmt.Printf("Starting Parquet Server on %s\n", addr)
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

func (s *ParquetServer) handleParquetRequest(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	dbName := vars["db"]
	measurement := vars["measurement"]

	// Parse query parameters for filtering
	timeRange := s.parseTimeRange(r.URL.Query())
	filters := s.parseFilters(r.URL.Query())

	// Build the query using our existing QueryClient format
	query := s.buildVirtualParquetQuery(dbName, measurement, timeRange, filters)

	// Set response headers for streaming parquet
	w.Header().Set("Content-Type", "application/vnd.apache.parquet")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s_%s.parquet", 
		measurement, time.Now().Format("20060102150405")))

	// Execute query with parquet output format
	// Note: We'll need to modify QueryClient to support streaming parquet output
	if err := s.queryClient.StreamParquetResults(query, dbName, w); err != nil {
		http.Error(w, fmt.Sprintf("Failed to stream results: %v", err), http.StatusInternalServerError)
		return
	}
}

func (s *ParquetServer) buildVirtualParquetQuery(dbName, measurement string, timeRange TimeRange, filters map[string]string) string {
	// Start with basic SELECT
	query := fmt.Sprintf("SELECT * FROM %s.%s", dbName, measurement)

	// Build WHERE conditions
	var conditions []string

	// Add time range conditions
	if timeRange.Start != nil {
		startTime := time.Unix(0, *timeRange.Start).Format(time.RFC3339Nano)
		conditions = append(conditions, fmt.Sprintf("time >= '%s'", startTime))
	}
	if timeRange.End != nil {
		endTime := time.Unix(0, *timeRange.End).Format(time.RFC3339Nano)
		conditions = append(conditions, fmt.Sprintf("time <= '%s'", endTime))
	}

	// Add other filters
	for col, val := range filters {
		if col == "time" {
			conditions = append(conditions, fmt.Sprintf("%s = '%s'", col, val))
		} else if _, err := strconv.ParseFloat(val, 64); err == nil {
			conditions = append(conditions, fmt.Sprintf("%s = %s", col, val))
		} else {
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

	if startStr := params.Get("start"); startStr != "" {
		startTime, err := time.Parse(time.RFC3339Nano, startStr)
		if err == nil {
			startNanos := startTime.UnixNano()
			timeRange.Start = &startNanos
		}
	}

	if endStr := params.Get("end"); endStr != "" {
		endTime, err := time.Parse(time.RFC3339Nano, endStr)
		if err == nil {
			endNanos := endTime.UnixNano()
			timeRange.End = &endNanos
		}
	}

	return timeRange
} 