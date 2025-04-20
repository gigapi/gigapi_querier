package icecube

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

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
		if metadata, err := s.GetTableMetadata(namespace, table); err == nil {
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