package icecube

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

// API handles REST endpoints for the IceCube catalog
type API struct {
	catalog *Catalog
}

// NewAPI creates a new IceCube API instance
func NewAPI(rootPath string) *API {
	return &API{
		catalog: &Catalog{RootPath: rootPath},
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
	
	tables, err := a.catalog.ListTables(namespace)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(tables)
}

func (a *API) getTableMetadata(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := vars["namespace"]
	table := vars["table"]

	metadata, err := a.catalog.GetTableMetadata(namespace, table)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(metadata)
}

func (a *API) listTableFiles(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := vars["namespace"]
	table := vars["table"]

	metadata, err := a.catalog.GetTableMetadata(namespace, table)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(metadata.Files)
} 