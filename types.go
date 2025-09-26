package streamhouse

import (
	"context"
	"time"
)

// FieldType represents the supported data types for schema fields
type FieldType string

const (
	FieldTypeString   FieldType = "string"
	FieldTypeInt      FieldType = "int"
	FieldTypeFloat    FieldType = "float"
	FieldTypeDatetime FieldType = "datetime"
	FieldTypeJSON     FieldType = "json"
	FieldTypeBool     FieldType = "bool"
)

// FieldConfig defines the configuration for a schema field
type FieldConfig struct {
	Type        FieldType              `json:"type" yaml:"type"`
	Required    bool                   `json:"required" yaml:"required"`
	Index       bool                   `json:"index" yaml:"index"`
	Description string                 `json:"description" yaml:"description"`
	Default     interface{}            `json:"default" yaml:"default"`
	Validator   func(interface{}) error `json:"-" yaml:"-"`
}

// DataSchema defines the structure for data schemas
type DataSchema struct {
	Name        string                 `json:"name" yaml:"name"`
	Fields      map[string]FieldConfig `json:"fields" yaml:"fields"`
	TTL         *time.Duration         `json:"ttl" yaml:"ttl"`
	Partitions  []string               `json:"partitions" yaml:"partitions"`
	IndexFields []string               `json:"index_fields" yaml:"index_fields"`
}

// Client interface defines the main client operations
type Client interface {
	RegisterSchema(*DataSchema) error
	Stream(dataType string, data map[string]interface{}) error
	StreamAsync(dataType string, data map[string]interface{})
	Data(dataType string) DataBuilderInterface
	StartConsumer(context.Context) error
	Close() error
	Health() error
}

// DataBuilder interface for fluent API pattern
type DataBuilderInterface interface {
	WithUser(string) *DataBuilder
	WithSession(string) *DataBuilder
	WithOrganization(string) *DataBuilder
	WithIP(string) *DataBuilder
	WithField(key string, value interface{}) *DataBuilder
	WithFields(map[string]interface{}) *DataBuilder
	Stream() error
	StreamAsync()
}

// StreamEvent represents a data event in the stream
type StreamEvent struct {
	ID           string                 `json:"id"`
	Schema       string                 `json:"schema"`
	Data         map[string]interface{} `json:"data"`
	Timestamp    time.Time              `json:"timestamp"`
	Metadata     map[string]interface{} `json:"metadata"`
	MetadataJSON string                 `json:"metadata_json,omitempty"` // Pre-marshaled metadata for performance
}

// ConsumerGroup represents a consumer group configuration
type ConsumerGroup struct {
	Name      string `json:"name" yaml:"name"`
	StreamKey string `json:"stream_key" yaml:"stream_key"`
	Consumer  string `json:"consumer" yaml:"consumer"`
}

// BatchProcessor handles batch processing configuration
type BatchProcessor struct {
	Size     int           `json:"size" yaml:"size"`
	Interval time.Duration `json:"interval" yaml:"interval"`
	MaxWait  time.Duration `json:"max_wait" yaml:"max_wait"`
}

// ConnectionStatus represents the status of connections
type ConnectionStatus struct {
	Redis      bool      `json:"redis"`
	ClickHouse bool      `json:"clickhouse"`
	LastCheck  time.Time `json:"last_check"`
}

// Metrics holds performance and operational metrics
type Metrics struct {
	EventsProcessed   int64     `json:"events_processed"`
	EventsFailed      int64     `json:"events_failed"`
	BatchesProcessed  int64     `json:"batches_processed"`
	LastProcessedTime time.Time `json:"last_processed_time"`
	AverageLatency    float64   `json:"average_latency_ms"`
}