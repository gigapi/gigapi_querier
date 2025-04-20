package icecube

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/gorilla/mux"
)

// TableFormat represents supported table formats
type TableFormat string

const (
	FormatParquet TableFormat = "parquet"
	FormatDelta   TableFormat = "delta"
)

// Storage defines the interface for different table storage formats
type Storage interface {
	// List tables in a namespace
	ListTables(namespace string) ([]string, error)
	
	// Get table metadata
	GetTableMetadata(namespace, table string) (*TableMetadata, error)
	
	// Get files that match time range
	GetFiles(namespace, table string, startTime, endTime *time.Time) ([]FileInfo, error)
	
	// Build query for the storage format
	BuildQuery(files []FileInfo, startTime, endTime *time.Time, filters map[string]string) (string, error)
}

// API handles REST endpoints for the IceCube catalog
type API struct {
	storages map[TableFormat]Storage
}

// NewAPI creates a new IceCube API instance
func NewAPI(rootPath string) *API {
	return &API{
		storages: map[TableFormat]Storage{
			FormatParquet: NewParquetStorage(rootPath),
			FormatDelta:   NewDeltaStorage(rootPath),
		},
	}
}

// RegisterRoutes adds IceCube routes to the router
func (a *API) RegisterRoutes(r *mux.Router) {
	// Namespace routes under /icecube/v1
	sub := r.PathPrefix("/icecube/v1").Subrouter()
	
	// List tables in a namespace
	sub.HandleFunc("/namespaces/{namespace}/tables", a.listTables).Methods("GET")
	
	// Get table metadata
	sub.HandleFunc("/namespaces/{namespace}/tables/{table}", a.getTableMetadata).Methods("GET")
	
	// List files in a table
	sub.HandleFunc("/namespaces/{namespace}/tables/{table}/files", a.listTableFiles).Methods("GET")
}

func (a *API) listTables(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := vars["namespace"]
	
	var allTables []string
	for _, storage := range a.storages {
		tables, err := storage.ListTables(namespace)
		if err != nil {
			continue
		}
		allTables = append(allTables, tables...)
	}

	json.NewEncoder(w).Encode(allTables)
}

func (a *API) getTableMetadata(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := vars["namespace"]
	table := vars["table"]
	
	// Try each storage format
	for _, s := range a.storages {
		if metadata, err := s.GetTableMetadata(namespace, table); err == nil {
			json.NewEncoder(w).Encode(metadata)
			return
		}
	}
	
	http.Error(w, "Table not found", http.StatusNotFound)
}

func (a *API) listTableFiles(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := vars["namespace"]
	table := vars["table"]

	// Try each storage format
	for _, s := range a.storages {
		// Just check if table exists
		if _, err := s.GetTableMetadata(namespace, table); err == nil {
			files, err := s.GetFiles(namespace, table, nil, nil)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			json.NewEncoder(w).Encode(files)
			return
		}
	}

	http.Error(w, "Table not found", http.StatusNotFound)
} 