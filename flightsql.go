package main

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/flight"
	flightgen "github.com/apache/arrow/go/v14/arrow/flight/gen/flight"
	"github.com/apache/arrow/go/v14/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/arrow/ipc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// FlightSQLServer implements the FlightSQL server interface
type FlightSQLServer struct {
	flightgen.UnimplementedFlightServiceServer
	flightsql.BaseServer
	queryClient *QueryClient
	mem        memory.Allocator
}

// mustEmbedUnimplementedFlightServiceServer implements the FlightServiceServer interface
func (s *FlightSQLServer) mustEmbedUnimplementedFlightServiceServer() {}

// NewFlightSQLServer creates a new FlightSQL server instance
func NewFlightSQLServer(queryClient *QueryClient) *FlightSQLServer {
	return &FlightSQLServer{
		queryClient: queryClient,
		mem:        memory.DefaultAllocator,
	}
}

// PollFlightInfo implements the FlightService interface
func (s *FlightSQLServer) PollFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.PollInfo, error) {
	// For now, we don't support polling flight info
	return nil, nil
}

// ListFlights implements the FlightService interface
func (s *FlightSQLServer) ListFlights(criteria *flight.Criteria, stream flight.FlightService_ListFlightsServer) error {
	// For now, we don't support listing flights
	return nil
}

// ListActions implements the FlightService interface
func (s *FlightSQLServer) ListActions(request *flight.Empty, stream flight.FlightService_ListActionsServer) error {
	// For now, we don't support any actions
	return nil
}

// Handshake implements the FlightService interface
func (s *FlightSQLServer) Handshake(stream flight.FlightService_HandshakeServer) error {
	// For now, we'll just echo back any handshake request
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		err = stream.Send(&flight.HandshakeResponse{
			Payload: req.Payload,
		})
		if err != nil {
			return err
		}
	}
}

// GetSchema implements the FlightService interface
func (s *FlightSQLServer) GetSchema(ctx context.Context, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	// For now, we don't support schema requests
	return nil, fmt.Errorf("schema requests not supported")
}

// GetFlightInfo implements the FlightService interface
func (s *FlightSQLServer) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	// For now, we don't support any other flight info requests
	return nil, fmt.Errorf("unsupported flight descriptor type")
}

// GetFlightInfoStatement implements the FlightSQL server interface for executing SQL statements
func (s *FlightSQLServer) GetFlightInfoStatement(ctx context.Context, cmd *flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	// Extract query from command
	query := string(desc.Cmd)
	
	// Execute the query using our existing QueryClient
	results, err := s.queryClient.Query(ctx, query, "mydb") // Using default database for now
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	// Convert results to Arrow format
	_, recordBatch, err := convertResultsToArrow(results)
	if err != nil {
		return nil, fmt.Errorf("failed to convert results to Arrow format: %w", err)
	}

	// Create a ticket for the results
	ticket := &flight.Ticket{
		Ticket: []byte("query-results"),
	}

	// Create the flight info
	info := &flight.FlightInfo{
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{
			{
				Ticket: ticket,
				Location: []*flight.Location{
					{
						Uri: "grpc://localhost:8082",
					},
				},
			},
		},
		TotalRecords: recordBatch.NumRows(),
		TotalBytes:   -1,
		Schema:       []byte{}, // Empty schema, will be sent in DoGet
	}

	return info, nil
}

// DoGet implements the FlightSQL server interface for retrieving data
func (s *FlightSQLServer) DoGet(ticket *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	// Get the schema and record batch from the ticket
	schema, recordBatch, err := s.getResultsFromTicket(ticket)
	if err != nil {
		return fmt.Errorf("failed to get results: %w", err)
	}

	// Write the schema
	writer := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
	err = writer.Write(recordBatch)
	if err != nil {
		return fmt.Errorf("failed to write record batch: %w", err)
	}

	return writer.Close()
}

// DoPut implements the FlightService interface
func (s *FlightSQLServer) DoPut(stream flight.FlightService_DoPutServer) error {
	// We don't support putting data yet
	return fmt.Errorf("put not supported")
}

// DoAction implements the FlightService interface
func (s *FlightSQLServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	// We don't support any actions yet
	return fmt.Errorf("action %s not supported", action.Type)
}

// DoExchange implements the FlightService interface
func (s *FlightSQLServer) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	// We don't support exchange yet
	return fmt.Errorf("exchange not supported")
}

// getResultsFromTicket retrieves the results associated with a ticket
func (s *FlightSQLServer) getResultsFromTicket(ticket *flight.Ticket) (*arrow.Schema, arrow.Record, error) {
	// For now, we'll just return an empty result
	// In a real implementation, we would store the results somewhere and retrieve them here
	fields := []arrow.Field{
		{Name: "dummy", Type: arrow.BinaryTypes.String},
	}
	schema := arrow.NewSchema(fields, nil)

	// Create an empty record batch
	builder := array.NewStringBuilder(s.mem)
	builder.AppendNull()
	arr := builder.NewArray()
	defer arr.Release()

	recordBatch := array.NewRecord(schema, []arrow.Array{arr}, 1)
	return schema, recordBatch, nil
}

// convertResultsToArrow converts our query results to Arrow format
func convertResultsToArrow(results []map[string]interface{}) (*arrow.Schema, arrow.Record, error) {
	if len(results) == 0 {
		return nil, nil, fmt.Errorf("no results to convert")
	}

	// Create Arrow schema from the first result
	fields := make([]arrow.Field, 0)
	for key := range results[0] {
		fields = append(fields, arrow.Field{
			Name: key,
			Type: arrow.BinaryTypes.String,
		})
	}
	schema := arrow.NewSchema(fields, nil)

	// Create Arrow arrays for each column
	allocator := memory.DefaultAllocator
	arrays := make([]arrow.Array, len(fields))
	for i, field := range fields {
		builder := array.NewStringBuilder(allocator)
		for _, row := range results {
			val := row[field.Name]
			if val == nil {
				builder.AppendNull()
			} else {
				builder.Append(fmt.Sprint(val))
			}
		}
		arrays[i] = builder.NewArray()
	}

	// Create record batch
	recordBatch := array.NewRecord(schema, arrays, int64(len(results)))
	return schema, recordBatch, nil
}

// StartFlightSQLServer starts the FlightSQL server
func StartFlightSQLServer(port int, queryClient *QueryClient) error {
	server := NewFlightSQLServer(queryClient)
	s := grpc.NewServer()
	flightgen.RegisterFlightServiceServer(s, server)
	reflection.Register(s)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	log.Printf("FlightSQL server listening on port %d", port)
	return s.Serve(lis)
} 