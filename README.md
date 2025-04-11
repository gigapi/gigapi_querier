![image](https://github.com/user-attachments/assets/fa3788a2-9a5b-47bf-b6ef-f818ba62a404)

# GigAPI Query Engine (go)

GigAPI Go provides a SQL interface to query time-series using GigAPI Catalog Metadata and DuckDB

## Quick Start

```bash
# Build from source
go build -o gigapi *.go

# Start the server
PORT=8080 DATA_DIR=./data ./gigapi
```

### Configuration

- `PORT`: Server port (default: 8080)
- `DATA_DIR`: Path to data directory (default: ./data)

## API Endpoints

### Query Data

```bash
POST /query?db=mydb
Content-Type: application/json

{
  "query": "SELECT time, location, temperature FROM weather WHERE time >= '2025-04-01T00:00:00'"
}
```

Series can be used with or without time ranges, ie for calculating averages, etc.

```bash
$ curl -X POST "http://localhost:9999/query?db=mydb"   -H "Content-Type: application/json"  \
  -d '{"query": "SELECT count(*), avg(temperature) FROM weather"}'
```
```json
{"results":[{"avg(temperature)":87.025,"count_star()":"40"}]}
```

## Data Structure

```
/data
  /mydb
    /weather
      /date=2025-04-10
        /hour=14
          *.parquet
          metadata.json
```

## Query Processing Logic

1. Parse SQL query to extract measurement name and time range
2. Find relevant parquet files using metadata
3. Use DuckDB to execute optimized queries against selected files
4. Post-process results to handle BigInt timestamps


## Notes for Developers

- File paths in metadata.json may contain absolute paths; the system handles both absolute and relative paths
- Time fields are converted from nanosecond BigInt to ISO strings
- Add `?debug=true` to query requests for detailed troubleshooting information

-----

## License

<img src="https://upload.wikimedia.org/wikipedia/commons/thumb/0/06/AGPLv3_Logo.svg/2560px-AGPLv3_Logo.svg.png" width=200>

> Gigapipe is released under the GNU Affero General Public License v3.0 ©️ HEPVEST BV, All Rights Reserved.
