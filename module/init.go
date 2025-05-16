package module

import (
	"github.com/gigapi/gigapi-config/config"
	"github.com/gigapi/gigapi-querier/querier"
	"github.com/gigapi/gigapi/v2/modules"
	"net/http"
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
	if config.Config.Gigapi.Mode != "readonly" && config.Config.Gigapi.Mode != "aio" {
		return
	}
	var err error
	server, err = querier.NewServer(querier.GetRootDir())
	if err != nil {
		panic(err)
	}
	api.RegisterRoute(&modules.Route{
		Path:    "/query",
		Methods: []string{"GET", "POST", "OPTIONS"},
		Handler: WithNoError(server.HandleQuery),
	})
}

func Close() {
	server.Close()
}
