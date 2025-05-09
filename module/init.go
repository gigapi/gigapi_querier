package module

import (
	"github.com/gigapi/gigapi-querier/querier"
	"github.com/gigapi/gigapi/v2/modules"
	"net/http"
	"os"
)

var server *querier.Server

func WithNoError(hndl func(w http.ResponseWriter, r *http.Request),
) func(w http.ResponseWriter, r *http.Request) error {
	return func(w http.ResponseWriter, r *http.Request) error {
		hndl(w, r)
		return nil
	}
}

func Init(api modules.Api) {
	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		dataDir = os.Getenv("GIGAPI_ROOT")
		if dataDir == "" {
			dataDir = "./data"
		}
	}
	var err error
	server, err = querier.NewServer(dataDir)
	if err != nil {
		panic(err)
	}
	api.RegisterRoute(&modules.Route{
		Path:    "/",
		Methods: []string{"GET", "OPTIONS"},
		Handler: WithNoError(server.HandleUI),
	})
	api.RegisterRoute(&modules.Route{
		Path:    "/query",
		Methods: []string{"POST", "OPTIONS"},
		Handler: WithNoError(server.HandleQuery),
	})
}

func Close() {
	server.Close()
}
