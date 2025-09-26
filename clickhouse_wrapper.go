package streamhouse

import (
	"time"

	"github.com/parnexcodes/streamhouse/storage"
)

// clickHouseAdapterWrapper adapts the storage.ClickHouseAdapter to ClickHouseConnection
type clickHouseAdapterWrapper struct {
	adapter *storage.ClickHouseAdapter
}

func newClickHouseAdapterWrapper(adapter *storage.ClickHouseAdapter) ClickHouseConnection {
	return &clickHouseAdapterWrapper{adapter: adapter}
}

func (w *clickHouseAdapterWrapper) Connect(config ClickHouseConfig) error {
	// Connection is already established by storage.NewClickHouseAdapter
	return nil
}

func (w *clickHouseAdapterWrapper) CreateTable(schema *DataSchema, tableName string) error {
	// Convert root schema to storage schema
	s := storage.DataSchema{
		Name:        schema.Name,
		Fields:      make(map[string]storage.FieldConfig, len(schema.Fields)),
		TTL:         schema.TTL,
		Partitions:  append([]string(nil), schema.Partitions...),
		IndexFields: append([]string(nil), schema.IndexFields...),
	}
	for name, field := range schema.Fields {
		s.Fields[name] = storage.FieldConfig{
			Type:        string(field.Type),
			Required:    field.Required,
			Index:       field.Index,
			Description: field.Description,
			Default:     field.Default,
		}
	}
	return w.adapter.CreateTable(&s, tableName)
}

func (w *clickHouseAdapterWrapper) InsertBatch(tableName string, events []*StreamEvent) error {
	if len(events) == 0 {
		return nil
	}
	converted := make([]*storage.StreamEvent, 0, len(events))
	for _, e := range events {
		converted = append(converted, &storage.StreamEvent{
			ID:        e.ID,
			Timestamp: e.Timestamp,
			DataType:  e.Schema,
			Data:      e.Data,
			Metadata:  e.Metadata,
		})
	}
	return w.adapter.InsertBatch(tableName, converted)
}

func (w *clickHouseAdapterWrapper) InsertData(tableName string, data []map[string]interface{}) error {
	if len(data) == 0 {
		return nil
	}
	// Convert map data to StreamEvent format for the adapter
	events := make([]*storage.StreamEvent, 0, len(data))
	for _, row := range data {
		// Extract metadata from the data if present
		metadata := make(map[string]interface{})
		if metadataValue, exists := row["metadata"]; exists {
			if metadataMap, ok := metadataValue.(map[string]interface{}); ok {
				metadata = metadataMap
			}
		}

		event := &storage.StreamEvent{
			ID:        getStringValue(row, "id"),
			Timestamp: getTimeValue(row, "timestamp"),
			DataType:  getStringValue(row, "data_type"),
			Data:      row,
			Metadata:  metadata,
		}
		events = append(events, event)
	}
	return w.adapter.InsertBatch(tableName, events)
}

func (w *clickHouseAdapterWrapper) Query(query string, args ...interface{}) (interface{}, error) {
	return w.adapter.Query(query, args...)
}

func (w *clickHouseAdapterWrapper) Close() error {
	return w.adapter.Close()
}

func (w *clickHouseAdapterWrapper) Health() error {
	return w.adapter.Health()
}

// Helper functions for data conversion
func getStringValue(data map[string]interface{}, key string) string {
	if val, exists := data[key]; exists {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return ""
}

func getTimeValue(data map[string]interface{}, key string) time.Time {
	if val, exists := data[key]; exists {
		if t, ok := val.(time.Time); ok {
			return t
		}
	}
	return time.Now()
}

// Ensure unused imports are avoided
var _ time.Duration
