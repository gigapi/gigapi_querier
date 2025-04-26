# GigAPI Iceberg API Implementation

This package implements a subset of the [Apache Iceberg](https://iceberg.apache.org/) table format and REST Catalog API, adapted for GigAPI's existing data structure and DuckDB-based query engine.

## Overview

The implementation provides:
- Basic Iceberg table operations
- REST Catalog API endpoints
- Schema discovery and management
- Table metadata handling
- Manifest list and file management

## Components

### Catalog (`catalog.go`)
Handles table management and file discovery:
```go
type Catalog struct {
    BasePath string
    QueryClient core.QueryClient
}
```

Key operations:
- `LoadTable(namespace, name string) (*Table, error)`
- `ListTables(namespace string) ([]TableIdentifier, error)`
- `GetTableFiles(ctx context.Context, namespace, name string) ([]string, error)`

### Table Operations (`table.go`)
Manages table-level operations:
```go
type TableOperations struct {
    Catalog *Catalog
    QueryClient core.QueryClient
}
```

Key operations:
- `ExecuteQuery(ctx context.Context, namespace, name string, icebergQuery string)`
- `GetTableSchema(namespace, name string) (*Schema, error)`
- `GetCurrentSchema(ctx context.Context, namespace, name string) (*Schema, error)`

### REST Catalog API (`rest_catalog.go`)
Implements the Iceberg REST Catalog API:

Endpoints:
- `GET /v1/config` - Get catalog configuration
- `GET /v1/{prefix}/namespaces` - List namespaces
- `GET /v1/{prefix}/namespaces/{namespace}` - Get namespace metadata
- `GET /v1/{prefix}/namespaces/{namespace}/tables` - List tables
- `GET /v1/{prefix}/namespaces/{namespace}/tables/{table}` - Get table metadata
- `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan` - Plan table scan

### Metadata Management (`metadata.go`)
Handles Iceberg metadata structures:
```go
type TableMetadata struct {
    FormatVersion int
    TableUUID     string
    Location      string
    Schema        Schema
    // ... other fields
}
```

### Manifest Management (`manifest.go`)
Manages manifest files and data file tracking:
```go
type ManifestList struct {
    Manifests []ManifestEntry
}

type Manifest struct {
    Schema Schema
    Entries []ManifestEntry
    // ... other fields
}
```

## Current Implementation Status

### Implemented Features
- Basic table operations through DuckDB
- Schema discovery and management
- REST Catalog API endpoints
- File discovery and management
- Query execution through existing infrastructure

### Limitations
- Simplified metadata handling
- No partition evolution support
- Limited snapshot management
- No schema evolution support yet
- No transaction support

## Usage Example

```go
// Initialize components
catalog := NewCatalog(basePath, queryClient)
tableOps := NewTableOperations(catalog, queryClient)
restCatalog := NewRESTCatalogServer(catalog, queryClient)

// Execute a query
results, err := tableOps.ExecuteQuery(ctx, "mydb", "mytable", 
    "SELECT * FROM table WHERE time > '2024-01-01'")

// Get table schema
schema, err := tableOps.GetTableSchema("mydb", "mytable")
```

## Future Enhancements

- [ ] Full schema evolution support
- [ ] Partition evolution
- [ ] Transaction management
- [ ] Snapshot isolation
- [ ] Table maintenance operations
- [ ] Schema validation
- [ ] Data file statistics
- [ ] Optimistic concurrency control

## Notes for Developers

1. The implementation uses DuckDB as the underlying query engine
2. File paths in metadata can be absolute or relative
3. Time fields are handled in nanosecond precision
4. Schema discovery uses DuckDB's DESCRIBE functionality
5. The REST API follows Iceberg's specification with adaptations for our use case

## References

- [Apache Iceberg Spec](https://iceberg.apache.org/spec/)
- [Iceberg REST Catalog API](https://iceberg.apache.org/docs/latest/api/#rest-catalog-api)
- [Table Evolution](https://iceberg.apache.org/docs/latest/evolution/) 