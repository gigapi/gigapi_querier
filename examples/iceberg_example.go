package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/gigapi/gigapi-querier/iceberg"
)

func main() {
	// Create a new catalog
	catalog := iceberg.NewCatalog("./data")

	// Create a new query client
	queryClient := iceberg.NewQueryClient("./data")
	if err := queryClient.Initialize(); err != nil {
		log.Fatalf("Failed to initialize query client: %v", err)
	}
	defer queryClient.Close()

	// Create table operations
	tableOps := iceberg.NewTableOperations(catalog, queryClient)

	// Example: List tables in a namespace
	tables, err := catalog.ListTables("mydb")
	if err != nil {
		log.Fatalf("Failed to list tables: %v", err)
	}

	fmt.Println("Available tables:")
	for _, table := range tables {
		fmt.Printf("- %s.%s\n", table.Namespace, table.Name)
	}

	// Example: Get table schema
	schema, err := tableOps.GetTableSchema("mydb", "mytable")
	if err != nil {
		log.Fatalf("Failed to get table schema: %v", err)
	}

	fmt.Println("\nTable schema:")
	for _, field := range schema.Fields {
		fmt.Printf("- %s: %s\n", field.Name, field.Type)
	}

	// Example: Execute a query
	ctx := context.Background()
	results, err := tableOps.ExecuteQuery(ctx, "mydb", "mytable", "SELECT * FROM mytable WHERE time >= '2023-01-01T00:00:00Z'")
	if err != nil {
		log.Fatalf("Failed to execute query: %v", err)
	}

	fmt.Println("\nQuery results:")
	for _, row := range results {
		fmt.Printf("%+v\n", row)
	}
} 