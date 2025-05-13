package module

import (
	"github.com/gigapi/gigapi-querier/querier"
	"github.com/gigapi/gigapi/v2/config"
	"github.com/gigapi/gigapi/v2/modules"
	"github.com/spf13/afero"
	"io/fs"
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
		dataDir = config.Config.Gigapi.Root
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
		Path:    "/query",
		Methods: []string{"GET", "POST", "OPTIONS"},
		Handler: WithNoError(server.HandleQuery),
	})
	api.RegisterRoute(&modules.Route{
		Path:    "/query ",
		Methods: []string{"GET", "POST", "OPTIONS"},
		Handler: WithNoError(server.HandleQuery),
	})
	api.RegisterRoute(&modules.Route{
		Path:    "/",
		Methods: []string{"GET", "OPTIONS"},
		Handler: WithNoError(server.HandleUI),
	})
	afero.Walk(server.UIFS, ".", func(path string, d fs.FileInfo, err error) error {
		if d == nil || len(path) <= 5 {
			return nil
		}
		api.RegisterRoute(&modules.Route{
			Path:    "/" + path[len("dist/"):],
			Methods: []string{"GET"},
			Handler: WithNoError(server.HandleUI),
		})
		return nil
	})
}

func Close() {
	server.Close()
}
