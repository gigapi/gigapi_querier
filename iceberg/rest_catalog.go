package iceberg

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gigapi/gigapi-querier/core"
)

// RESTCatalogServer implements the Iceberg REST Catalog API
type RESTCatalogServer struct {
	Catalog *Catalog
	QueryClient core.QueryClient
}

// NewRESTCatalogServer creates a new REST Catalog server
func NewRESTCatalogServer(catalog *Catalog, queryClient core.QueryClient) *RESTCatalogServer {
	return &RESTCatalogServer{
		Catalog: catalog,
		QueryClient: queryClient,
	}
}

// CatalogConfig represents the catalog configuration
type CatalogConfig struct {
	Overrides map[string]string `json:"overrides"`
	Defaults  map[string]string `json:"defaults"`
	Endpoints []string          `json:"endpoints"`
}

// NamespacesList represents a list of namespaces
type NamespacesList struct {
	Namespaces []string `json:"namespaces"`
}

// NamespaceMetadata represents namespace metadata
type NamespaceMetadata struct {
	Properties map[string]string `json:"properties"`
}

// TablesList represents a list of tables
type TablesList struct {
	Tables []string `json:"tables"`
}

// RESTTableMetadata represents table metadata for REST API
type RESTTableMetadata struct {
	TableName  string        `json:"name"`
	Schema     []SchemaField `json:"schema"`
	Snapshots  []RESTSnapshot `json:"snapshots"`
}

// SchemaField represents a field in the schema
type SchemaField struct {
	ID   int    `json:"id"`
	Type string `json:"type"`
	Name string `json:"name"`
}

// RESTSnapshot represents a table snapshot for REST API
type RESTSnapshot struct {
	SnapshotID   int64 `json:"snapshot_id"`
	TimestampMs  int64 `json:"timestamp_ms"`
}

// PlanTableScanRequest represents a scan planning request
type PlanTableScanRequest struct {
	SnapshotID      int64 `json:"snapshot_id"`
	StartSnapshotID int64 `json:"start_snapshot_id,omitempty"`
	EndSnapshotID   int64 `json:"end_snapshot_id,omitempty"`
}

// PlanTableScanResponse represents a scan planning response
type PlanTableScanResponse struct {
	Status string `json:"status"`
	Tasks  []Task `json:"tasks"`
}

// Task represents a scan task
type Task struct {
	TaskID int    `json:"task_id"`
	Files  []File `json:"files"`
}

// File represents a file in a scan task
type File struct {
	FilePath string `json:"file_path"`
	Start    int64  `json:"start"`
	Length   int64  `json:"length"`
}

// RegisterHandlers registers the REST Catalog API handlers
func (s *RESTCatalogServer) RegisterHandlers(mux *http.ServeMux) {
	// Configuration API
	mux.HandleFunc("/v1/config", s.handleGetConfig)

	// Catalog API
	mux.HandleFunc("/v1/", s.handleCatalogRequest)
}

// handleGetConfig handles the GET /v1/config endpoint
func (s *RESTCatalogServer) handleGetConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	config := CatalogConfig{
		Overrides: map[string]string{
			"warehouse": s.Catalog.BasePath,
		},
		Defaults: map[string]string{
			"clients": "4",
		},
		Endpoints: []string{
			"GET /v1/{prefix}/namespaces/{namespace}",
			"GET /v1/{prefix}/namespaces",
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(config)
}

// handleCatalogRequest handles all catalog API requests
func (s *RESTCatalogServer) handleCatalogRequest(w http.ResponseWriter, r *http.Request) {
	// Parse the path
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/"), "/")
	if len(parts) < 2 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	namespace := parts[1]
	table := ""
	if len(parts) > 2 {
		table = parts[2]
	}

	switch {
	case r.Method == http.MethodGet && len(parts) == 2:
		// GET /v1/{prefix}/namespaces
		s.handleListNamespaces(w, r, namespace)
	case r.Method == http.MethodGet && len(parts) == 3:
		// GET /v1/{prefix}/namespaces/{namespace}
		s.handleGetNamespaceMetadata(w, r, namespace)
	case r.Method == http.MethodGet && len(parts) == 4 && parts[3] == "tables":
		// GET /v1/{prefix}/namespaces/{namespace}/tables
		s.handleListTables(w, r, namespace)
	case r.Method == http.MethodGet && len(parts) == 4:
		// GET /v1/{prefix}/namespaces/{namespace}/tables/{table}
		s.handleGetTableMetadata(w, r, namespace, table)
	case r.Method == http.MethodPost && len(parts) == 5 && parts[4] == "plan":
		// POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan
		s.handlePlanTableScan(w, r, namespace, table)
	default:
		http.Error(w, "Not found", http.StatusNotFound)
	}
}

// handleListNamespaces handles the GET /v1/{prefix}/namespaces endpoint
func (s *RESTCatalogServer) handleListNamespaces(w http.ResponseWriter, r *http.Request, parent string) {
	// For now, we'll just return the default namespace
	response := NamespacesList{
		Namespaces: []string{"mydb"},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleGetNamespaceMetadata handles the GET /v1/{prefix}/namespaces/{namespace} endpoint
func (s *RESTCatalogServer) handleGetNamespaceMetadata(w http.ResponseWriter, r *http.Request, namespace string) {
	response := NamespaceMetadata{
		Properties: map[string]string{
			"owner":       "default",
			"created_at": time.Now().Format(time.RFC3339),
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleListTables handles the GET /v1/{prefix}/namespaces/{namespace}/tables endpoint
func (s *RESTCatalogServer) handleListTables(w http.ResponseWriter, r *http.Request, namespace string) {
	tables, err := s.Catalog.ListTables(namespace)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	tableNames := make([]string, len(tables))
	for i, table := range tables {
		tableNames[i] = fmt.Sprintf("%s.%s", table.Namespace, table.Name)
	}

	response := TablesList{
		Tables: tableNames,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleGetTableMetadata handles the GET /v1/{prefix}/namespaces/{namespace}/tables/{table} endpoint
func (s *RESTCatalogServer) handleGetTableMetadata(w http.ResponseWriter, r *http.Request, namespace, table string) {
	// Get table schema
	schema, err := s.Catalog.GetTableSchema(namespace, table)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Convert schema to REST format
	schemaFields := make([]SchemaField, len(schema.Fields))
	for i, field := range schema.Fields {
		schemaFields[i] = SchemaField{
			ID:   i + 1,
			Type: field.Type,
			Name: field.Name,
		}
	}

	// Create a dummy snapshot for now
	snapshots := []RESTSnapshot{
		{
			SnapshotID:   1,
			TimestampMs: time.Now().UnixMilli(),
		},
	}

	response := RESTTableMetadata{
		TableName:  fmt.Sprintf("%s.%s", namespace, table),
		Schema:     schemaFields,
		Snapshots:  snapshots,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handlePlanTableScan handles the POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan endpoint
func (s *RESTCatalogServer) handlePlanTableScan(w http.ResponseWriter, r *http.Request, namespace, table string) {
	var request PlanTableScanRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Get table files
	files, err := s.Catalog.GetTableFiles(r.Context(), namespace, table)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Create tasks
	tasks := make([]Task, 1)
	taskFiles := make([]File, len(files))
	for i, file := range files {
		taskFiles[i] = File{
			FilePath: file,
			Start:    0,
			Length:   0, // We don't have file sizes yet
		}
	}
	tasks[0] = Task{
		TaskID: 1,
		Files:  taskFiles,
	}

	response := PlanTableScanResponse{
		Status: "completed",
		Tasks:  tasks,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
} 