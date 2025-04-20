# IceCube - Lightweight Iceberg & Delta Lake REST Catalog

IceCube is a lightweight implementation of catalog protocols for both Apache Iceberg and Delta Lake formats. It provides metadata management and query optimization for DuckDB and other compatible clients.

## API Endpoints

### List Tables
```bash
curl http://localhost:9999/icecube/v1/namespaces/hep/tables
```

### Get Table Metadata
```bash
curl http://localhost:9999/icecube/v1/namespaces/hep/tables/hep_1
```

### List Files
```bash
curl http://localhost:9999/icecube/v1/namespaces/hep/tables/hep_1/files
```

## DuckDB Integration

### Setup
```sql
-- For Iceberg tables
INSTALL iceberg;
LOAD iceberg;
SET iceberg_catalog='rest';
SET iceberg_rest_catalog_url='http://localhost:9999/icecube/v1';

-- For Delta Lake tables
INSTALL delta;
LOAD delta;
```

### Basic Queries

#### Iceberg Tables
```sql
-- List available tables
SELECT * FROM iceberg_tables();

-- Query with time range
SELECT * 
FROM iceberg_scan('hep.hep_1') 
WHERE time >= epoch_ns(TIMESTAMP '2025-04-18 12:00:00')
  AND time < epoch_ns(TIMESTAMP '2025-04-18 13:00:00')
LIMIT 5;

-- Get table metadata
SELECT * FROM iceberg_table_metadata('hep.hep_1');

-- List files with statistics
SELECT * FROM iceberg_files('hep.hep_1');
```

#### Delta Lake Tables
```sql
-- Query Delta table
SELECT * 
FROM delta_scan('hep.delta_table', 
    REST_CATALOG_URL='http://localhost:9999/icecube/v1')
WHERE time >= epoch_ns(TIMESTAMP '2025-04-18 12:00:00')
LIMIT 5;

-- Get Delta table history
SELECT * FROM delta_history('hep.delta_table');

-- Get table details
SELECT * FROM delta_table_details('hep.delta_table');
```

## File Structure

### Iceberg Structure
```
/data
  /mydb
    /weather
      /date=2025-04-10
        /hour=14
          *.parquet
          metadata.json
```

### Delta Lake Structure
```
/data
  /mydb
    /weather
      /_delta_log
        *.json           # Transaction logs
        *.checkpoint.parquet  # Checkpoints
      /date=2025-04-10
        /hour=14
          *.parquet      # Data files
```

## Delta Lake Support

### Transaction Log Format
```json
{
  "commitInfo": {
    "timestamp": 1650000000000,
    "operation": "WRITE",
    "operationParameters": {...}
  },
  "protocol": {
    "minReaderVersion": 1,
    "minWriterVersion": 2
  },
  "metaData": {
    "id": "table-id",
    "format": {
      "provider": "parquet"
    },
    "schemaString": "...",
    "partitionColumns": ["date", "hour"]
  },
  "add": {
    "path": "date=2025-04-10/hour=14/file.parquet",
    "size": 1234567,
    "modificationTime": 1650000000000,
    "dataChange": true,
    "stats": "{\"numRecords\":1000,...}"
  }
}
```

### Features
- Full Delta Lake protocol support
- Transaction log parsing and replay
- Checkpoint handling
- Statistics-based file pruning
- Time range optimization
- Partition pruning

### Limitations
- Read-only support (no writes)
- Basic schema evolution
- Single-table transactions only

## Benefits
- Works with both Iceberg and Delta Lake formats
- Provides file pruning based on time ranges
- Compatible with DuckDB extensions
- Lightweight metadata management
- Easy integration with existing systems

## Integration

IceCube integrates with the existing server infrastructure:

1. The main server initializes both ParquetServer and IceCube API
2. ParquetServer uses IceCube catalog for file discovery
3. QueryClient uses IceCube for optimized query planning
4. Supports both Iceberg and Delta Lake formats transparently

See `server.go`, `parquetServer.go`, and `queryClient.go` for implementation details.
