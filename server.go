// server.go
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gorilla/mux"
)

// Server represents the API server
type Server struct {
	QueryClient   *QueryClient
	ParquetServer *ParquetServer
}

// NewServer creates a new server instance
func NewServer(dataDir string) (*Server, error) {
	client := NewQueryClient(dataDir)
	err := client.Initialize()
	if err != nil {
		return nil, err
	}

	// Initialize ParquetServer
	parquetServer, err := NewParquetServer(dataDir)
	if err != nil {
		client.Close()
		return nil, err
	}

	return &Server{
		QueryClient:   client,
		ParquetServer: parquetServer,
	}, nil
}

// QueryRequest represents a query API request
type QueryRequest struct {
	Query string `json:"query"`
	DB    string `json:"db,omitempty"`
}

// QueryResponse represents a query API response
type QueryResponse struct {
	Results []map[string]interface{} `json:"results"`
}

// ErrorResponse represents an API error response
type ErrorResponse struct {
	Error string `json:"error"`
}

// handleQuery handles the /query endpoint
func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	// Only allow POST
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse request body
	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendErrorResponse(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.Query == "" {
		sendErrorResponse(w, "Missing query parameter", http.StatusBadRequest)
		return
	}

	// Extract database name from query param or body
	dbName := r.URL.Query().Get("db")
	if dbName == "" {
		dbName = req.DB
	}
	if dbName == "" {
		dbName = "mydb" // Default
	}

	log.Printf("Executing query for database '%s': %s", dbName, req.Query)

	// Execute query
	results, err := s.QueryClient.Query(req.Query, dbName)
	if err != nil {
		log.Printf("Query error: %v", err)
		sendErrorResponse(w, fmt.Sprintf("Query execution failed: %v", err), http.StatusInternalServerError)
		return
	}

	// Process results to handle special types for JSON
	processedResults := processResultsForJSON(results)

	// Send response
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(QueryResponse{
		Results: processedResults,
	})
}

// processResultsForJSON prepares results for JSON serialization
func processResultsForJSON(results []map[string]interface{}) []map[string]interface{} {
	processedResults := make([]map[string]interface{}, len(results))

	for i, row := range results {
		processedRow := make(map[string]interface{})

		for key, value := range row {
			// Handle different types of values
			switch v := value.(type) {
			case nil:
				processedRow[key] = nil
			case int64:
				// Convert int64 to string for JSON
				processedRow[key] = strconv.FormatInt(v, 10)
			case time.Time:
				// Format time values
				processedRow[key] = v.Format(time.RFC3339Nano)
			default:
				processedRow[key] = v
			}
		}

		processedResults[i] = processedRow
	}

	return processedResults
}

// Send an error response in JSON format
func sendErrorResponse(w http.ResponseWriter, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(ErrorResponse{
		Error: message,
	})
}

// Health check endpoint
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "ok",
		"timestamp": time.Now().Format(time.RFC3339),
	})
}

// Close the server and release resources
func (s *Server) Close() error {
	if err := s.QueryClient.Close(); err != nil {
		return err
	}
	if s.ParquetServer != nil && s.ParquetServer.queryClient != nil {
		return s.ParquetServer.queryClient.Close()
	}
	return nil
}

func main() {
	// Add command line flags
	queryFlag := flag.String("query", "", "Execute a single query and exit")
	dbFlag := flag.String("db", "mydb", "Database name to query")
	flag.Parse()

	// Get configuration from environment variables
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		dataDir = "./data"
	}

	// Create QueryClient
	client := NewQueryClient(dataDir)
	err := client.Initialize()
	if err != nil {
		log.Fatalf("Failed to initialize query client: %v", err)
	}
	defer client.Close()

	// If query flag is provided, execute query and exit
	if *queryFlag != "" {
		results, err := client.Query(*queryFlag, *dbFlag)
		if err != nil {
			log.Fatalf("Query error: %v", err)
		}

		// Process and print results as JSON
		processedResults := processResultsForJSON(results)
		jsonData, err := json.MarshalIndent(processedResults, "", "  ")
		if err != nil {
			log.Fatalf("Failed to marshal results: %v", err)
		}
		fmt.Println(string(jsonData))
		return
	}

	// Create server for HTTP mode
	server, err := NewServer(dataDir)
	if err != nil {
		log.Fatalf("Failed to initialize server: %v", err)
	}
	defer server.Close()

	// Set up routes
	http.HandleFunc("/health", server.handleHealth)
	http.HandleFunc("/query", server.handleQuery)

	// Set up parquet routes
	router := mux.NewRouter()
	router.HandleFunc("/parquet/{db}/{measurement}", server.ParquetServer.handleParquetRequest).Methods("GET", "HEAD")
	router.HandleFunc("/schema/{db}/{measurement}", server.ParquetServer.handleSchemaRequest).Methods("GET")

	// Create a new mux that combines both standard handlers and the router
	mux := http.NewServeMux()
	mux.Handle("/parquet/", router)
	mux.Handle("/schema/", router)
	mux.HandleFunc("/health", server.handleHealth)
	mux.HandleFunc("/query", server.handleQuery)

	// Start server with combined mux
	log.Printf("GigAPI server running at http://localhost:%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
