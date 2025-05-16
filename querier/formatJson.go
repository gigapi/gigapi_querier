package querier

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"
)

func JsonFormatter(data []map[string]any, w http.ResponseWriter) error {
	// Process results to Handle special types for JSON
	processedResults := ProcessResultsForJSON(data)

	// Send response
	w.Header().Set("Content-Type", "application/json")
	return json.NewEncoder(w).Encode(QueryResponse{
		Results: processedResults,
	})
}

func NDJsonFormatter(data []map[string]any, w http.ResponseWriter) error {
	// Process results to Handle special types for NDJSON
	processedResults := ProcessResultsForJSON(data)

	// Send response
	w.Header().Set("Content-Type", "application/x-ndjson")
	for _, result := range processedResults {
		err := json.NewEncoder(w).Encode(result)
		if err != nil {
			return err
		}
		_, err = w.Write([]byte("\n"))
		if err != nil {
			return err
		}
	}
	return nil
}

// ProcessResultsForJSON prepares results for JSON serialization
func ProcessResultsForJSON(results []map[string]interface{}) []map[string]interface{} {
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
