package querier

import "net/http"

type formatterFn func(data []map[string]any, w http.ResponseWriter) error

var formatters = map[string]formatterFn{
	"json":   JsonFormatter,
	"ndjson": NDJsonFormatter,
}
