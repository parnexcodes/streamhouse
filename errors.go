package streamhouse

import (
	"errors"
	"fmt"
)

// Common error variables
var (
	ErrSchemaNotFound      = errors.New("schema not found")
	ErrSchemaAlreadyExists = errors.New("schema already exists")
	ErrInvalidFieldType    = errors.New("invalid field type")
	ErrRequiredField       = errors.New("required field missing")
	ErrValidationFailed    = errors.New("validation failed")
	ErrConnectionFailed    = errors.New("connection failed")
	ErrStreamNotFound      = errors.New("stream not found")
	ErrConsumerNotFound    = errors.New("consumer not found")
	ErrInvalidConfig       = errors.New("invalid configuration")
	ErrClientClosed        = errors.New("client is closed")
)

// SchemaError represents schema-related errors
type SchemaError struct {
	Schema string
	Field  string
	Err    error
}

func (e *SchemaError) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("schema '%s' field '%s': %v", e.Schema, e.Field, e.Err)
	}
	return fmt.Sprintf("schema '%s': %v", e.Schema, e.Err)
}

func (e *SchemaError) Unwrap() error {
	return e.Err
}

// ValidationError represents data validation errors
type ValidationError struct {
	Field   string
	Value   interface{}
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation error for field '%s' with value '%v': %s", e.Field, e.Value, e.Message)
}

// ConnectionError represents connection-related errors
type ConnectionError struct {
	Service string // "redis" or "clickhouse"
	Err     error
}

func (e *ConnectionError) Error() string {
	return fmt.Sprintf("%s connection error: %v", e.Service, e.Err)
}

func (e *ConnectionError) Unwrap() error {
	return e.Err
}

// StreamError represents streaming-related errors
type StreamError struct {
	StreamKey string
	EventID   string
	Err       error
}

func (e *StreamError) Error() string {
	if e.EventID != "" {
		return fmt.Sprintf("stream '%s' event '%s': %v", e.StreamKey, e.EventID, e.Err)
	}
	return fmt.Sprintf("stream '%s': %v", e.StreamKey, e.Err)
}

func (e *StreamError) Unwrap() error {
	return e.Err
}

// ConsumerError represents consumer-related errors
type ConsumerError struct {
	Group    string
	Consumer string
	Err      error
}

func (e *ConsumerError) Error() string {
	return fmt.Sprintf("consumer group '%s' consumer '%s': %v", e.Group, e.Consumer, e.Err)
}

func (e *ConsumerError) Unwrap() error {
	return e.Err
}

// BatchError represents batch processing errors
type BatchError struct {
	BatchID string
	Size    int
	Err     error
}

func (e *BatchError) Error() string {
	return fmt.Sprintf("batch '%s' (size: %d): %v", e.BatchID, e.Size, e.Err)
}

func (e *BatchError) Unwrap() error {
	return e.Err
}

// Helper functions for creating specific errors
func NewSchemaError(schema, field string, err error) *SchemaError {
	return &SchemaError{Schema: schema, Field: field, Err: err}
}

func NewValidationError(field string, value interface{}, message string) *ValidationError {
	return &ValidationError{Field: field, Value: value, Message: message}
}

func NewConnectionError(service string, err error) *ConnectionError {
	return &ConnectionError{Service: service, Err: err}
}

func NewStreamError(streamKey, eventID string, err error) *StreamError {
	return &StreamError{StreamKey: streamKey, EventID: eventID, Err: err}
}

func NewConsumerError(group, consumer string, err error) *ConsumerError {
	return &ConsumerError{Group: group, Consumer: consumer, Err: err}
}

func NewBatchError(batchID string, size int, err error) *BatchError {
	return &BatchError{BatchID: batchID, Size: size, Err: err}
}