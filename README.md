![image](https://github.com/user-attachments/assets/fa3788a2-9a5b-47bf-b6ef-f818ba62a404)

# GigAPI Query Engine (go)

GigAPI Go provides a SQL interface to query time-series using GigAPI Catalog Metadata and DuckDB

> [!WARNING]  
> GigAPI is an open beta developed in public. Bugs and changes should be expected. Use at your own risk.
> 

## <img src="https://github.com/user-attachments/assets/a9aa3ebd-9164-476d-aedf-97b817078350" width=24 /> Quick Start

### Docker
Run `gigapi-querier` using Docker making sure the proper `data` folder or `S3` bucket is provided
```yaml
gigapi-querier:
  image: ghcr.io/gigapi/gigapi-querier:latest
  container_name: gigapi-querier
  hostname: gigapi-querier
  volumes:
    - ./data:/data
  ports:
    - "8080:8080"
    - "8081:8081"
  environment:
    - DATA_DIR=/data
    - PORT=8080
    - ICEBERG_PORT=8081
```

### Build
```bash
# Build from source
go build -o gigapi *.go

# Start the server
PORT=8080 ICEBERG_PORT=8081 DATA_DIR=./data ./gigapi
```

### Configuration

- `PORT`: Main server port (default: 8080)
- `ICEBERG_PORT`: Iceberg API server port (default: 8081)
- `DATA_DIR`: Path to data directory (default: ./data)
- `DISABLE_UI`: Disable web UI (optional)

## <img src="https://github.com/user-attachments/assets/a9aa3ebd-9164-476d-aedf-97b817078350" width=24 /> API Endpoints

### Query Data

```bash
$ curl -X POST "http://localhost:8080/query?db=mydb" \
  -H "Content-Type: application/json"  \
  -d '{"query": "SELECT time, location, temperature FROM weather WHERE time >= '2025-04-01T00:00:00'"}'
```

### Iceberg Tables

```bash
# List tables
$ curl "http://localhost:8081/iceberg/tables?namespace=mydb"

# Get table schema
$ curl "http://localhost:8081/iceberg/schema?namespace=mydb&table=mytable"

# Query table
$ curl -X POST "http://localhost:8081/iceberg/query" \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM table", "namespace": "mydb", "table": "mytable"}'
```

The GigAPI Querier can also be used in CLI mode to execute an individual query

```bash
$ ./gigapi --query "SELECT count(*), avg(temperature) FROM weather" --db mydb
```

A quick and dirty query user-interface is also provided for testing
![image](https://github.com/user-attachments/assets/a9f09b3f-10fc-42e3-9092-770252e0d8d3)

## <img src="https://github.com/user-attachments/assets/a9aa3ebd-9164-476d-aedf-97b817078350" width=24 /> Data Structure

```
/data
  /mydb
    /weather
      /date=2025-04-10
        /hour=14
          *.parquet
          metadata.json
```

## <img src="https://github.com/user-attachments/assets/a9aa3ebd-9164-476d-aedf-97b817078350" width=24 /> Query Processing Logic

1. Parse SQL query to extract measurement name and time range
2. Find relevant parquet files using metadata
3. Use DuckDB to execute optimized queries against selected files
4. Post-process results to handle BigInt timestamps

## <img src="https://github.com/user-attachments/assets/a9aa3ebd-9164-476d-aedf-97b817078350" width=24 /> Notes for Developers

- File paths in metadata.json may contain absolute paths; the system handles both absolute and relative paths
- Time fields are converted from nanosecond BigInt to ISO strings
- Add `?debug=true` to query requests for detailed troubleshooting information
- Iceberg support uses existing QueryClient and DuckDB for all operations

-----

## License

<img src="https://upload.wikimedia.org/wikipedia/commons/thumb/0/06/AGPLv3_Logo.svg/2560px-AGPLv3_Logo.svg.png" width=200>

> Gigapipe is released under the GNU Affero General Public License v3.0 ©️ HEPVEST BV, All Rights Reserved.
