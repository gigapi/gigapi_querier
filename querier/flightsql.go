package querier

import (
	"context"
	"fmt"
	"log"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/flight"
	"github.com/apache/arrow/go/v14/arrow/flight/flightsql"
	flightgen "github.com/apache/arrow/go/v14/arrow/flight/gen/flight"
	"github.com/apache/arrow/go/v14/arrow/ipc"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/grpc/metadata"
)

// FlightSQLServer implements the FlightSQL server interface
type FlightSQLServer struct {
	flightgen.UnimplementedFlightServiceServer
	flightsql.BaseServer
	queryClient *QueryClient
	mem         memory.Allocator
	// Add result storage
	results     map[string]arrow.Record
	resultsLock sync.RWMutex
}

// mustEmbedUnimplementedFlightServiceServer implements the FlightServiceServer interface
func (s *FlightSQLServer) mustEmbedUnimplementedFlightServiceServer() {}

// NewFlightSQLServer creates a new FlightSQL server instance
func NewFlightSQLServer(queryClient *QueryClient) *FlightSQLServer {
	return &FlightSQLServer{
		queryClient: queryClient,
		mem:         memory.DefaultAllocator,
		results:     make(map[string]arrow.Record),
	}
}

// PollFlightInfo implements the FlightService interface
func (s *FlightSQLServer) PollFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.PollInfo, error) {
	log.Printf("PollFlightInfo called with descriptor type: %v, path: %v, cmd: %v",
		desc.Type, desc.Path, string(desc.Cmd))
	// For now, we don't support polling flight info
	return nil, nil
}

// ListFlights implements the FlightService interface
func (s *FlightSQLServer) ListFlights(criteria *flight.Criteria, stream flight.FlightService_ListFlightsServer) error {
	log.Printf("ListFlights called with criteria: %v", criteria)
	// For now, we don't support listing flights
	return nil
}

// ListActions implements the FlightService interface
func (s *FlightSQLServer) ListActions(request *flight.Empty, stream flight.FlightService_ListActionsServer) error {
	log.Printf("ListActions called")
	// For now, we don't support any actions
	return nil
}

// Handshake implements the FlightService interface
func (s *FlightSQLServer) Handshake(stream flight.FlightService_HandshakeServer) error {
	log.Printf("Handshake called")
	// For now, we'll just echo back any handshake request
	for {
		req, err := stream.Recv()
		if err != nil {
			log.Printf("Handshake receive error: %v", err)
			return err
		}

		err = stream.Send(&flight.HandshakeResponse{
			Payload: req.Payload,
		})
		if err != nil {
			log.Printf("Handshake send error: %v", err)
			return err
		}
	}
}

// GetSchema implements the FlightService interface
func (s *FlightSQLServer) GetSchema(ctx context.Context, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	log.Printf("GetSchema called with descriptor type: %v, path: %v, cmd: %v",
		desc.Type, desc.Path, string(desc.Cmd))
	// For now, we don't support schema requests
	return nil, fmt.Errorf("schema requests not supported")
}

// GetFlightInfo implements the FlightService interface
func (s *FlightSQLServer) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	log.Printf("GetFlightInfo called with descriptor type: %v, path: %v, cmd: %v",
		desc.Type, desc.Path, string(desc.Cmd))

	// Handle SQL query command
	if desc.Type == flight.DescriptorCMD {
		// Unmarshal the Any message
		any := &anypb.Any{}
		if err := proto.Unmarshal(desc.Cmd, any); err != nil {
			log.Printf("Failed to unmarshal Any message: %v", err)
			return nil, fmt.Errorf("failed to unmarshal command: %w", err)
		}

		// Check if this is a CommandStatementQuery
		if any.TypeUrl == "type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery" {
			// The query is in the Any message's value
			query := string(any.Value)
			// Clean up the query string
			query = strings.TrimSpace(query)
			query = strings.ReplaceAll(query, "\n", " ")
			query = strings.ReplaceAll(query, "\r", " ")
			query = strings.ReplaceAll(query, "\b", "") // Remove backspace characters
			query = regexp.MustCompile(`\s+`).ReplaceAllString(query, " ")
			// Remove any non-printable characters
			query = strings.Map(func(r rune) rune {
				if r < 32 || r > 126 {
					return -1
				}
				return r
			}, query)
			log.Printf("Executing SQL query: %v", query)

			// Get database name from metadata if available
			dbName := "default" // Default database name
			if md, ok := metadata.FromIncomingContext(ctx); ok {
				if bucket := md.Get("bucket"); len(bucket) > 0 {
					dbName = bucket[0]
					log.Printf("Using bucket from metadata: %s", dbName)
				} else if namespace := md.Get("database"); len(namespace) > 0 {
					dbName = namespace[0]
					log.Printf("Using database from metadata: %s", dbName)
				} else if namespace := md.Get("namespace"); len(namespace) > 0 {
					dbName = namespace[0]
					log.Printf("Using namespace from metadata: %s", dbName)
				}
			}

			// Execute the query using our existing QueryClient
			results, err := s.queryClient.Query(ctx, query, dbName)
			if err != nil {
				log.Printf("Query execution failed: %v", err)
				return nil, fmt.Errorf("failed to execute query: %w", err)
			}

			// Convert results to Arrow format
			_, recordBatch, err := convertResultsToArrow(results)
			if err != nil {
				log.Printf("Failed to convert results to Arrow format: %v", err)
				return nil, fmt.Errorf("failed to convert results to Arrow format: %w", err)
			}

			// Generate a unique ticket
			ticketID := fmt.Sprintf("query-%d", time.Now().UnixNano())

			// Store the results
			s.resultsLock.Lock()
			s.results[ticketID] = recordBatch
			s.resultsLock.Unlock()

			// Create a ticket for the results
			ticket := &flight.Ticket{
				Ticket: []byte(ticketID),
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

			log.Printf("Returning flight info with %d records", recordBatch.NumRows())
			return info, nil
		}
	}

	// For now, we don't support any other flight info requests
	return nil, fmt.Errorf("unsupported flight descriptor type: %v", desc.Type)
}

// GetFlightInfoStatement implements the FlightSQL server interface for executing SQL statements
func (s *FlightSQLServer) GetFlightInfoStatement(ctx context.Context, cmd *flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	log.Printf("GetFlightInfoStatement called with descriptor type: %v, path: %v, cmd: %v",
		desc.Type, desc.Path, string(desc.Cmd))

	// Extract query from command
	query := string(desc.Cmd)

	// Execute the query using our existing QueryClient
	results, err := s.queryClient.Query(ctx, query, "mydb") // Using default database for now
	if err != nil {
		log.Printf("Query execution failed: %v", err)
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	// Convert results to Arrow format
	_, recordBatch, err := convertResultsToArrow(results)
	if err != nil {
		log.Printf("Failed to convert results to Arrow format: %v", err)
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

	log.Printf("Returning flight info with %d records", recordBatch.NumRows())
	return info, nil
}

// DoGet implements the FlightSQL server interface for retrieving data
func (s *FlightSQLServer) DoGet(ticket *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	log.Printf("DoGet called with ticket: %v", string(ticket.Ticket))

	// Get the results from storage
	s.resultsLock.RLock()
	recordBatch, exists := s.results[string(ticket.Ticket)]
	s.resultsLock.RUnlock()

	if !exists {
		return fmt.Errorf("no results found for ticket: %s", string(ticket.Ticket))
	}

	// Get the schema from the record batch
	schema := recordBatch.Schema()

	// Write the schema
	writer := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
	err := writer.Write(recordBatch)
	if err != nil {
		log.Printf("Failed to write record batch: %v", err)
		return fmt.Errorf("failed to write record batch: %w", err)
	}

	// Clean up the stored results
	s.resultsLock.Lock()
	delete(s.results, string(ticket.Ticket))
	s.resultsLock.Unlock()

	log.Printf("Successfully wrote record batch with %d rows", recordBatch.NumRows())
	return writer.Close()
}

// DoPut implements the FlightService interface
func (s *FlightSQLServer) DoPut(stream flight.FlightService_DoPutServer) error {
	log.Printf("DoPut called")
	// We don't support putting data yet
	return fmt.Errorf("put not supported")
}

// DoAction implements the FlightService interface
func (s *FlightSQLServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	log.Printf("DoAction called with action type: %v", action.Type)
	// We don't support any actions yet
	return fmt.Errorf("action %s not supported", action.Type)
}

// DoExchange implements the FlightService interface
func (s *FlightSQLServer) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	log.Printf("DoExchange called")
	// We don't support exchange yet
	return fmt.Errorf("exchange not supported")
}

// convertResultsToArrow converts our query results to Arrow format
func convertResultsToArrow(results []map[string]interface{}) (*arrow.Schema, arrow.Record, error) {
	if len(results) == 0 {
		return nil, nil, fmt.Errorf("no results to convert")
	}

	// Create Arrow schema from the first result
	fields := make([]arrow.Field, 0)
	for key, val := range results[0] {
		var arrowType arrow.DataType
		switch v := val.(type) {
		case int, int32, int64:
			arrowType = arrow.PrimitiveTypes.Int64
		case float32, float64:
			arrowType = arrow.PrimitiveTypes.Float64
		case string:
			arrowType = arrow.BinaryTypes.String
		case bool:
			arrowType = arrow.FixedWidthTypes.Boolean
		case time.Time:
			arrowType = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
		case nil:
			// For NULL values, try to infer type from other rows
			arrowType = inferTypeFromColumn(key, results)
		default:
			// Try to infer type from the value's string representation
			strVal := fmt.Sprintf("%v", v)
			if _, err := strconv.ParseInt(strVal, 10, 64); err == nil {
				arrowType = arrow.PrimitiveTypes.Int64
			} else if _, err := strconv.ParseFloat(strVal, 64); err == nil {
				arrowType = arrow.PrimitiveTypes.Float64
			} else if _, err := strconv.ParseBool(strVal); err == nil {
				arrowType = arrow.FixedWidthTypes.Boolean
			} else if _, err := time.Parse(time.RFC3339Nano, strVal); err == nil {
				arrowType = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
			} else {
				log.Printf("Unknown type for value %v: %T, defaulting to string", v, v)
				arrowType = arrow.BinaryTypes.String
			}
		}
		fields = append(fields, arrow.Field{Name: key, Type: arrowType, Nullable: true})
	}
	schema := arrow.NewSchema(fields, nil)

	// Create Arrow arrays for each column
	allocator := memory.DefaultAllocator
	arrays := make([]arrow.Array, len(fields))

	for i, field := range fields {
		var builder array.Builder
		switch field.Type.ID() {
		case arrow.INT64:
			builder = array.NewInt64Builder(allocator)
			for _, row := range results {
				val := row[field.Name]
				if val == nil {
					builder.AppendNull()
					continue
				}
				switch v := val.(type) {
				case int:
					builder.(*array.Int64Builder).Append(int64(v))
				case int32:
					builder.(*array.Int64Builder).Append(int64(v))
				case int64:
					builder.(*array.Int64Builder).Append(v)
				case float64:
					builder.(*array.Int64Builder).Append(int64(v))
				case string:
					if num, err := strconv.ParseInt(v, 10, 64); err == nil {
						builder.(*array.Int64Builder).Append(num)
					} else {
						builder.(*array.Int64Builder).AppendNull()
					}
				default:
					// Try to convert to string and parse
					strVal := fmt.Sprintf("%v", v)
					if num, err := strconv.ParseInt(strVal, 10, 64); err == nil {
						builder.(*array.Int64Builder).Append(num)
					} else {
						builder.(*array.Int64Builder).AppendNull()
					}
				}
			}
		case arrow.FLOAT64:
			builder = array.NewFloat64Builder(allocator)
			for _, row := range results {
				val := row[field.Name]
				if val == nil {
					builder.AppendNull()
					continue
				}
				switch v := val.(type) {
				case float64:
					builder.(*array.Float64Builder).Append(v)
				case float32:
					builder.(*array.Float64Builder).Append(float64(v))
				case int:
					builder.(*array.Float64Builder).Append(float64(v))
				case int64:
					builder.(*array.Float64Builder).Append(float64(v))
				case string:
					if num, err := strconv.ParseFloat(v, 64); err == nil {
						builder.(*array.Float64Builder).Append(num)
					} else {
						builder.(*array.Float64Builder).AppendNull()
					}
				default:
					// Try to convert to string and parse
					strVal := fmt.Sprintf("%v", v)
					if num, err := strconv.ParseFloat(strVal, 64); err == nil {
						builder.(*array.Float64Builder).Append(num)
					} else {
						builder.(*array.Float64Builder).AppendNull()
					}
				}
			}
		case arrow.STRING:
			builder = array.NewStringBuilder(allocator)
			for _, row := range results {
				val := row[field.Name]
				if val == nil {
					builder.AppendNull()
					continue
				}
				builder.(*array.StringBuilder).Append(fmt.Sprintf("%v", val))
			}
		case arrow.BOOL:
			builder = array.NewBooleanBuilder(allocator)
			for _, row := range results {
				val := row[field.Name]
				if val == nil {
					builder.AppendNull()
					continue
				}
				switch v := val.(type) {
				case bool:
					builder.(*array.BooleanBuilder).Append(v)
				case string:
					if b, err := strconv.ParseBool(v); err == nil {
						builder.(*array.BooleanBuilder).Append(b)
					} else {
						builder.(*array.BooleanBuilder).AppendNull()
					}
				default:
					// Try to convert to string and parse
					strVal := fmt.Sprintf("%v", v)
					if b, err := strconv.ParseBool(strVal); err == nil {
						builder.(*array.BooleanBuilder).Append(b)
					} else {
						builder.(*array.BooleanBuilder).AppendNull()
					}
				}
			}
		case arrow.TIMESTAMP:
			builder = array.NewTimestampBuilder(allocator, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"})
			for _, row := range results {
				val := row[field.Name]
				if val == nil {
					builder.AppendNull()
					continue
				}
				switch v := val.(type) {
				case time.Time:
					// Convert to UTC if not already
					utcTime := v.UTC()
					builder.(*array.TimestampBuilder).Append(arrow.Timestamp(utcTime.UnixMicro()))
				case string:
					if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
						utcTime := t.UTC()
						builder.(*array.TimestampBuilder).Append(arrow.Timestamp(utcTime.UnixMicro()))
					} else {
						builder.(*array.TimestampBuilder).AppendNull()
					}
				default:
					// Try to convert to string and parse
					strVal := fmt.Sprintf("%v", v)
					if t, err := time.Parse(time.RFC3339Nano, strVal); err == nil {
						utcTime := t.UTC()
						builder.(*array.TimestampBuilder).Append(arrow.Timestamp(utcTime.UnixMicro()))
					} else {
						builder.(*array.TimestampBuilder).AppendNull()
					}
				}
			}
		default:
			builder = array.NewStringBuilder(allocator)
			for _, row := range results {
				val := row[field.Name]
				if val == nil {
					builder.AppendNull()
					continue
				}
				builder.(*array.StringBuilder).Append(fmt.Sprintf("%v", val))
			}
		}
		arrays[i] = builder.NewArray()
		builder.Release()
	}

	// Create record batch
	recordBatch := array.NewRecord(schema, arrays, int64(len(results)))
	return schema, recordBatch, nil
}

// inferTypeFromColumn attempts to infer the Arrow type for a column by looking at non-null values
func inferTypeFromColumn(columnName string, results []map[string]interface{}) arrow.DataType {
	for _, row := range results {
		if val := row[columnName]; val != nil {
			switch val.(type) {
			case int, int32, int64:
				return arrow.PrimitiveTypes.Int64
			case float32, float64:
				return arrow.PrimitiveTypes.Float64
			case string:
				return arrow.BinaryTypes.String
			case bool:
				return arrow.FixedWidthTypes.Boolean
			case time.Time:
				return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
			default:
				// Try to infer type from string representation
				strVal := fmt.Sprintf("%v", val)
				if _, err := strconv.ParseInt(strVal, 10, 64); err == nil {
					return arrow.PrimitiveTypes.Int64
				} else if _, err := strconv.ParseFloat(strVal, 64); err == nil {
					return arrow.PrimitiveTypes.Float64
				} else if _, err := strconv.ParseBool(strVal); err == nil {
					return arrow.FixedWidthTypes.Boolean
				} else if _, err := time.Parse(time.RFC3339Nano, strVal); err == nil {
					return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
				}
			}
		}
	}
	return arrow.BinaryTypes.String // Default to string if no non-null values found
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
