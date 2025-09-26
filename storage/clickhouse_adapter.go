package storage

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// ClickHouseAdapter adapts the ClickHouseClient to work with StreamHouse schemas
type ClickHouseAdapter struct {
	client *ClickHouseClient
}

// NewClickHouseAdapter creates a new ClickHouse adapter
func NewClickHouseAdapter(config ClickHouseConfig) (*ClickHouseAdapter, error) {
	client, err := NewClickHouseClient(config)
	if err != nil {
		return nil, err
	}

	return &ClickHouseAdapter{
		client: client,
	}, nil
}

// Connect establishes connection to ClickHouse
func (a *ClickHouseAdapter) Connect(config ClickHouseConfig) error {
	// Connection is already established in NewClickHouseAdapter
	return nil
}

// CreateTable creates a ClickHouse table from a StreamHouse DataSchema
func (a *ClickHouseAdapter) CreateTable(schema *DataSchema, tableName string) error {
	// Convert StreamHouse schema to ClickHouse schema
	chSchema := a.convertToClickHouseSchema(schema, tableName)

	// Create the table
	return a.client.CreateTable(chSchema)
}

// InsertBatch inserts a batch of StreamEvents into ClickHouse
func (a *ClickHouseAdapter) InsertBatch(tableName string, events []*StreamEvent) error {
	if len(events) == 0 {
		return nil
	}

	// Convert events to map format
	data := make([]map[string]interface{}, len(events))
	for i, event := range events {
		data[i] = a.convertEventToMap(event)
	}

	return a.client.InsertData(tableName, data)
}

// Query executes a query and returns results
func (a *ClickHouseAdapter) Query(query string, args ...interface{}) (interface{}, error) {
	return a.client.Query(query, args...)
}

// Close closes the ClickHouse connection
func (a *ClickHouseAdapter) Close() error {
	return a.client.Close()
}

// Health checks the health of the ClickHouse connection
func (a *ClickHouseAdapter) Health() error {
	return a.client.Ping()
}

// convertToClickHouseSchema converts a StreamHouse DataSchema to ClickHouse TableSchema
func (a *ClickHouseAdapter) convertToClickHouseSchema(schema *DataSchema, tableName string) TableSchema {
	fields := make(map[string]FieldDefinition)

	// Add standard fields
	fields["id"] = FieldDefinition{
		Type:    "String",
		Comment: "Event ID",
	}
	fields["timestamp"] = FieldDefinition{
		Type:    "DateTime64(3)",
		Comment: "Event timestamp",
	}
	fields["data_type"] = FieldDefinition{
		Type:    "String",
		Comment: "Data type identifier",
	}

	// Convert schema fields
	for name, field := range schema.Fields {
		chType := a.convertFieldType(field.Type)
		fields[name] = FieldDefinition{
			Type:     chType,
			Nullable: !field.Required,
			Comment:  field.Description,
		}
	}

	// Don't add metadata field here - it should come from the schema

	orderBy := []string{"timestamp", "id"}
	if len(schema.IndexFields) > 0 {
		orderBy = append(orderBy, schema.IndexFields...)
	}

	chSchema := TableSchema{
		Name:    tableName,
		Fields:  fields,
		Engine:  "MergeTree()",
		OrderBy: orderBy,
	}

	// Add partitioning if specified
	if len(schema.Partitions) > 0 {
		chSchema.PartitionBy = strings.Join(schema.Partitions, ", ")
	} else {
		// Default monthly partitioning by timestamp
		chSchema.PartitionBy = "toYYYYMM(timestamp)"
	}

	// Add TTL if specified
	if schema.TTL != nil {
		chSchema.TTL = schema.TTL
	}

	return chSchema
}

// convertFieldType converts StreamHouse field types to ClickHouse types
func (a *ClickHouseAdapter) convertFieldType(fieldType string) string {
	switch fieldType {
	case "string":
		return "String"
	case "int":
		return "Int64"
	case "float":
		return "Float64"
	case "bool":
		return "Bool"
	case "datetime":
		return "DateTime64(3)"
	case "json":
		return "String" // Store JSON as string
	default:
		return "String" // Default to string for unknown types
	}
}

// convertEventToMap converts a StreamEvent to a map for ClickHouse insertion
func (a *ClickHouseAdapter) convertEventToMap(event *StreamEvent) map[string]interface{} {
	result := make(map[string]interface{})

	// Add standard fields
	result["id"] = event.ID
	result["timestamp"] = event.Timestamp.Format("2006-01-02 15:04:05.999999999")
	result["data_type"] = event.DataType

	// Add event data
	for key, value := range event.Data {
		// Handle different timestamp formats
		if key == "timestamp" {
			if t, ok := value.(time.Time); ok {
				result[key] = t.Format("2006-01-02 15:04:05.999999999")
			} else if str, ok := value.(string); ok {
				// Try to parse RFC3339 timestamp and convert to ClickHouse format
				if t, err := time.Parse(time.RFC3339Nano, str); err == nil {
					result[key] = t.Format("2006-01-02 15:04:05.999999999")
				} else {
					result[key] = str // Use as-is if parsing fails
				}
			} else {
				result[key] = value
			}
		} else if key == "metadata" {
			// Handle metadata field - convert maps to JSON
			if metadataMap, ok := value.(map[string]interface{}); ok {
				if metadataJSON, err := json.Marshal(metadataMap); err == nil {
					result[key] = string(metadataJSON)
				} else {
					result[key] = fmt.Sprintf("%v", value)
				}
			} else {
				result[key] = value
			}
		} else if t, ok := value.(time.Time); ok {
			result[key] = t.Format("2006-01-02 15:04:05.999999999")
		} else {
			result[key] = value
		}
	}

	// Add metadata as JSON string if present
	if len(event.Metadata) > 0 {
		// Convert metadata to proper JSON string
		if metadataJSON, err := json.Marshal(event.Metadata); err == nil {
			result["metadata"] = string(metadataJSON)
		} else {
			// Fallback to string representation if JSON marshaling fails
			result["metadata"] = fmt.Sprintf("%v", event.Metadata)
		}
	}

	return result
}

// DataSchema represents a StreamHouse data schema (defined here for reference)
type DataSchema struct {
	Name        string
	Fields      map[string]FieldConfig
	TTL         *time.Duration
	Partitions  []string
	IndexFields []string
}

// FieldConfig represents field configuration (defined here for reference)
type FieldConfig struct {
	Type        string
	Required    bool
	Index       bool
	Description string
	Default     interface{}
}

// StreamEvent represents a stream event (defined here for reference)
type StreamEvent struct {
	ID        string
	Timestamp time.Time
	DataType  string
	Data      map[string]interface{}
	Metadata  map[string]interface{}
}
