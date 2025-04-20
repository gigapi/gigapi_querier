# IceCube - Lightweight Iceberg REST Catalog

IceCube is a lightweight implementation of the Apache Iceberg REST catalog protocol, designed to work with existing hive-partitioned parquet files. It provides metadata management and query optimization for DuckDB and other Iceberg-compatible clients.

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
-- Install and load Iceberg extension
INSTALL iceberg;
LOAD iceberg;

-- Configure Iceberg to use our REST catalog
SET iceberg_catalog='rest';
SET iceberg_rest_catalog_url='http://localhost:9999/icecube/v1';
```

### Basic Queries
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

### Performance Testing

#### Time-Based Pruning
```sql
-- Test file pruning efficiency
SELECT COUNT(*) 
FROM iceberg_scan('hep.hep_1') 
WHERE time BETWEEN 
  epoch_ns(TIMESTAMP '2025-04-18 12:00:00') AND 
  epoch_ns(TIMESTAMP '2025-04-18 13:00:00');

-- Compare execution plans
EXPLAIN SELECT * 
FROM iceberg_scan('hep.hep_1') 
WHERE time >= epoch_ns(TIMESTAMP '2025-04-18 12:00:00');
```

### Comparison with Arrow API
```sql
-- Compare results between Iceberg and Arrow endpoints
WITH iceberg_data AS (
  SELECT * FROM iceberg_scan('hep.hep_1')
  WHERE time >= epoch_ns(TIMESTAMP '2025-04-18 12:00:00')
    AND time < epoch_ns(TIMESTAMP '2025-04-18 13:00:00')
),
arrow_data AS (
  SELECT * FROM read_parquet('http://localhost:9999/arrow/hep/hep_1?start=2025-04-18T12:00:00Z&end=2025-04-18T13:00:00Z')
)
SELECT 
  'iceberg' as source, COUNT(*) as row_count FROM iceberg_data
UNION ALL
SELECT 
  'arrow' as source, COUNT(*) as row_count FROM arrow_data;
```

## File Structure

IceCube works with the following directory structure:
```
/data
  /mydb
    /weather
      /date=2025-04-10
        /hour=14
          *.parquet
          metadata.json
        /hour=15
          *.parquet
          metadata.json
```

Example metadata.json:
```json
{
  "type": "hep_1",
  "parquet_size_bytes": 866559,
  "row_count": 3984,
  "min_time": 1745107202849660286,
  "max_time": 1745110788388557205,
  "wal_sequence": 0,
  "files": [
    {
      "id": 1746,
      "path": "/data/hep/hep_1/date=2025-04-20/hour=00/5640d9ee-1d7f-11f0-a328-0242ac1d0004.4.parquet",
      "size_bytes": 435581,
      "row_count": 2297,
      "chunk_time": 1745109319428876227,
      "min_time": 1745107202849660286,
      "max_time": 1745109263230086438,
      "range": "1h",
      "type": "compacted"
    }
  ]
}
```

## Benefits
- Works with existing hive-partitioned data
- Provides file pruning based on time ranges
- Compatible with DuckDB's Iceberg extension
- Lightweight metadata management
- Easy integration with existing systems

## Limitations
- Basic implementation of Iceberg spec
- Limited to time-based partitioning
- Focused on read-only workloads

## Integration

IceCube integrates with the existing server infrastructure:

1. The main server initializes both ParquetServer and IceCube API
2. ParquetServer uses IceCube catalog for file discovery
3. QueryClient uses IceCube for optimized query planning

See `server.go`, `parquetServer.go`, and `queryClient.go` for implementation details.
