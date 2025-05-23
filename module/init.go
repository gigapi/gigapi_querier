package module

import (
	"context"
	"github.com/gigapi/gigapi-config/config"
	"github.com/gigapi/gigapi-querier/core"
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
	flightsqlPort := config.Config.FlightSql.Port

	ctx := core.WithDefaultLogger(context.Background(), "main")
	dataDir := querier.GetRootDir()
	client := querier.NewQueryClient(dataDir)
	err = client.Initialize()
	if err != nil {
		core.Errorf(ctx, "Failed to initialize query client: %v", err)
		os.Exit(1)
	}
	go func() {
		core.Infof(ctx, "FlightSQL server running on port %s", flightsqlPort)
		err = querier.StartFlightSQLServer(flightsqlPort, client)
		if err != nil {
			core.Errorf(ctx, "Failed to start FlightSQL server: %v", err)
			os.Exit(1)
		}
	}()
}

func Close() {
	server.Close()
	querier.StopFlightSQLServer()
}
