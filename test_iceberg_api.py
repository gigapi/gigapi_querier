#!/usr/bin/env python3

import requests
import json
from typing import Dict, List, Any
import sys

class IcebergAPIClient:
    def __init__(self, base_url: str = "http://localhost:8081"):
        self.base_url = base_url.rstrip('/')
        
    def list_tables(self, namespace: str = "mydb") -> List[Dict[str, Any]]:
        """List all tables in a namespace"""
        url = f"{self.base_url}/iceberg/tables"
        params = {"namespace": namespace}
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()["results"]
    
    def get_table_schema(self, namespace: str, table: str) -> List[Dict[str, Any]]:
        """Get the schema of a table"""
        url = f"{self.base_url}/iceberg/schema"
        params = {"namespace": namespace, "table": table}
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()["results"]
    
    def execute_query(self, namespace: str, table: str, query: str) -> List[Dict[str, Any]]:
        """Execute a query on a table"""
        url = f"{self.base_url}/iceberg/query"
        data = {
            "namespace": namespace,
            "table": table,
            "query": query
        }
        response = requests.post(url, json=data)
        response.raise_for_status()
        return response.json()["results"]

def main():
    # Initialize client
    client = IcebergAPIClient()
    
    try:
        # Test listing tables
        print("\n=== Listing Tables ===")
        tables = client.list_tables()
        print(f"Found {len(tables)} tables:")
        for table in tables:
            print(f"- {table['namespace']}.{table['name']}")
        
        if not tables:
            print("No tables found. Please ensure you have data in your namespace.")
            return
        
        # Get first table for testing
        test_table = tables[0]
        namespace = test_table["namespace"]
        table_name = test_table["name"]
        
        # Test getting schema
        print(f"\n=== Getting Schema for {namespace}.{table_name} ===")
        schema = client.get_table_schema(namespace, table_name)
        print("Schema:")
        for field in schema:
            print(f"- {field['name']}: {field['type']}")
        
        # Test executing queries
        print(f"\n=== Executing Queries on {namespace}.{table_name} ===")
        
        # Test count query
        count_query = f"SELECT count(*) FROM {table_name}"
        print(f"\nExecuting: {count_query}")
        count_results = client.execute_query(namespace, table_name, count_query)
        print("Count results:", count_results)
        
        # Test sample data query
        sample_query = f"SELECT * FROM {table_name} LIMIT 5"
        print(f"\nExecuting: {sample_query}")
        sample_results = client.execute_query(namespace, table_name, sample_query)
        print("Sample results:", json.dumps(sample_results, indent=2))
        
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main() 