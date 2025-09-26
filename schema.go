package streamhouse

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

// CompiledValidator represents a pre-compiled validator for a schema
type CompiledValidator struct {
	schema    *DataSchema
	fieldKeys []string // Pre-computed field keys for iteration
}

// ValidateAndConvert validates and converts data in a single pass
func (cv *CompiledValidator) ValidateAndConvert(data map[string]interface{}) (map[string]interface{}, error) {
	result := make(map[string]interface{}, len(cv.schema.Fields))
	
	// Check required fields and convert values
	for _, fieldName := range cv.fieldKeys {
		fieldConfig := cv.schema.Fields[fieldName]
		value, exists := data[fieldName]

		if !exists {
			if fieldConfig.Required {
				return nil, NewValidationError(fieldName, nil, "required field missing")
			}
			// Set default value if provided
			if fieldConfig.Default != nil {
				convertedValue, err := convertValueDirect(fieldConfig.Default, fieldConfig.Type)
				if err != nil {
					return nil, fmt.Errorf("field %s default conversion failed: %w", fieldName, err)
				}
				result[fieldName] = convertedValue
			}
			continue
		}

		// Validate and convert field value
		convertedValue, err := convertValueDirect(value, fieldConfig.Type)
		if err != nil {
			return nil, NewValidationError(fieldName, value, err.Error())
		}

		// Run custom validator if provided
		if fieldConfig.Validator != nil {
			if err := fieldConfig.Validator(convertedValue); err != nil {
				return nil, NewValidationError(fieldName, value, fmt.Sprintf("custom validation failed: %v", err))
			}
		}

		result[fieldName] = convertedValue
	}

	return result, nil
}

// convertValueDirect performs direct conversion without interface overhead
func convertValueDirect(value interface{}, fieldType FieldType) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	switch fieldType {
	case FieldTypeString:
		switch v := value.(type) {
		case string:
			return v, nil
		case []byte:
			return string(v), nil
		default:
			return fmt.Sprintf("%v", v), nil
		}

	case FieldTypeInt:
		switch v := value.(type) {
		case int:
			return int64(v), nil
		case int8:
			return int64(v), nil
		case int16:
			return int64(v), nil
		case int32:
			return int64(v), nil
		case int64:
			return v, nil
		case uint:
			return int64(v), nil
		case uint8:
			return int64(v), nil
		case uint16:
			return int64(v), nil
		case uint32:
			return int64(v), nil
		case uint64:
			return int64(v), nil
		case float32:
			return int64(v), nil
		case float64:
			return int64(v), nil
		case string:
			return strconv.ParseInt(v, 10, 64)
		default:
			return nil, fmt.Errorf("cannot convert %T to int", value)
		}

	case FieldTypeFloat:
		switch v := value.(type) {
		case float32:
			return float64(v), nil
		case float64:
			return v, nil
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			return float64(reflect.ValueOf(v).Convert(reflect.TypeOf(float64(0))).Float()), nil
		case string:
			return strconv.ParseFloat(v, 64)
		default:
			return nil, fmt.Errorf("cannot convert %T to float", value)
		}

	case FieldTypeBool:
		switch v := value.(type) {
		case bool:
			return v, nil
		case string:
			return strconv.ParseBool(v)
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			return reflect.ValueOf(v).Convert(reflect.TypeOf(int64(0))).Int() != 0, nil
		default:
			return nil, fmt.Errorf("cannot convert %T to bool", value)
		}

	case FieldTypeDatetime:
		switch v := value.(type) {
		case time.Time:
			return v, nil
		case string:
			// Try multiple time formats
			formats := []string{
				time.RFC3339,
				time.RFC3339Nano,
				"2006-01-02T15:04:05",
				"2006-01-02 15:04:05",
				"2006-01-02",
			}
			
			for _, format := range formats {
				if t, err := time.Parse(format, v); err == nil {
					return t, nil
				}
			}
			return nil, fmt.Errorf("cannot parse datetime: %s", v)
		case int64:
			// Unix timestamp
			return time.Unix(v, 0), nil
		default:
			return nil, fmt.Errorf("cannot convert %T to datetime", value)
		}

	case FieldTypeJSON:
		// Fast-path: if it's already a string, assume it's valid JSON
		if str, ok := value.(string); ok {
			return str, nil
		}
		
		// Convert to JSON string for non-string values
		jsonBytes, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal JSON: %w", err)
		}
		
		return string(jsonBytes), nil

	default:
		return nil, fmt.Errorf("unknown field type: %s", fieldType)
	}
}

// SchemaRegistry manages registered schemas
type SchemaRegistry struct {
	schemas           map[string]*DataSchema
	compiledValidators map[string]*CompiledValidator
	mu                sync.RWMutex
}

// NewSchemaRegistry creates a new schema registry
func NewSchemaRegistry() *SchemaRegistry {
	return &SchemaRegistry{
		schemas:           make(map[string]*DataSchema),
		compiledValidators: make(map[string]*CompiledValidator),
	}
}

// Register registers a new schema
func (sr *SchemaRegistry) Register(schema *DataSchema) error {
	if schema == nil {
		return NewSchemaError("", "", fmt.Errorf("schema cannot be nil"))
	}

	if schema.Name == "" {
		return NewSchemaError("", "", fmt.Errorf("schema name cannot be empty"))
	}

	if err := sr.validateSchema(schema); err != nil {
		return NewSchemaError(schema.Name, "", err)
	}

	sr.mu.Lock()
	defer sr.mu.Unlock()

	if _, exists := sr.schemas[schema.Name]; exists {
		return NewSchemaError(schema.Name, "", ErrSchemaAlreadyExists)
	}

	// Add default fields if not present
	sr.addDefaultFields(schema)

	sr.schemas[schema.Name] = schema
	
	// Create compiled validator
	fieldKeys := make([]string, 0, len(schema.Fields))
	for fieldName := range schema.Fields {
		fieldKeys = append(fieldKeys, fieldName)
	}
	
	sr.compiledValidators[schema.Name] = &CompiledValidator{
		schema:    schema,
		fieldKeys: fieldKeys,
	}
	
	return nil
}

// Get retrieves a schema by name
func (sr *SchemaRegistry) Get(name string) (*DataSchema, error) {
	sr.mu.RLock()
	defer sr.mu.RUnlock()
	
	schema, exists := sr.schemas[name]
	if !exists {
		return nil, NewSchemaError(name, "", ErrSchemaNotFound)
	}
	return schema, nil
}

// GetCompiledValidator retrieves a compiled validator by schema name
func (sr *SchemaRegistry) GetCompiledValidator(name string) (*CompiledValidator, error) {
	sr.mu.RLock()
	defer sr.mu.RUnlock()
	
	validator, exists := sr.compiledValidators[name]
	if !exists {
		return nil, NewSchemaError(name, "", ErrSchemaNotFound)
	}
	return validator, nil
}

// List returns all registered schema names
func (sr *SchemaRegistry) List() []string {
	sr.mu.RLock()
	defer sr.mu.RUnlock()
	
	names := make([]string, 0, len(sr.schemas))
	for name := range sr.schemas {
		names = append(names, name)
	}
	return names
}

// Unregister removes a schema
func (sr *SchemaRegistry) Unregister(name string) error {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	
	if _, exists := sr.schemas[name]; !exists {
		return NewSchemaError(name, "", ErrSchemaNotFound)
	}
	delete(sr.schemas, name)
	delete(sr.compiledValidators, name)
	return nil
}

// ValidateData validates data against a registered schema
func (sr *SchemaRegistry) ValidateData(schemaName string, data map[string]interface{}) error {
	schema, err := sr.Get(schemaName)
	if err != nil {
		return err
	}

	return sr.validateDataAgainstSchema(schema, data)
}

// validateSchema validates the schema definition itself
func (sr *SchemaRegistry) validateSchema(schema *DataSchema) error {
	if len(schema.Fields) == 0 {
		return fmt.Errorf("schema must have at least one field")
	}

	for fieldName, fieldConfig := range schema.Fields {
		if fieldName == "" {
			return fmt.Errorf("field name cannot be empty")
		}

		if !sr.isValidFieldType(fieldConfig.Type) {
			return NewValidationError(fieldName, fieldConfig.Type, "invalid field type")
		}

		// Validate default value type if provided
		if fieldConfig.Default != nil {
			if err := sr.validateFieldValue(fieldName, fieldConfig.Default, fieldConfig.Type); err != nil {
				return fmt.Errorf("invalid default value for field %s: %w", fieldName, err)
			}
		}
	}

	// Validate index fields exist
	for _, indexField := range schema.IndexFields {
		if _, exists := schema.Fields[indexField]; !exists {
			return fmt.Errorf("index field '%s' does not exist in schema", indexField)
		}
	}

	// Validate partition fields exist
	for _, partitionField := range schema.Partitions {
		if _, exists := schema.Fields[partitionField]; !exists {
			return fmt.Errorf("partition field '%s' does not exist in schema", partitionField)
		}
	}

	return nil
}

// validateDataAgainstSchema validates data against a specific schema
func (sr *SchemaRegistry) validateDataAgainstSchema(schema *DataSchema, data map[string]interface{}) error {
	// Check required fields
	for fieldName, fieldConfig := range schema.Fields {
		value, exists := data[fieldName]

		if !exists {
			if fieldConfig.Required {
				return NewValidationError(fieldName, nil, "required field missing")
			}
			// Set default value if provided
			if fieldConfig.Default != nil {
				data[fieldName] = fieldConfig.Default
			}
			continue
		}

		// Validate field value
		if err := sr.validateFieldValue(fieldName, value, fieldConfig.Type); err != nil {
			return err
		}

		// Run custom validator if provided
		if fieldConfig.Validator != nil {
			if err := fieldConfig.Validator(value); err != nil {
				return NewValidationError(fieldName, value, fmt.Sprintf("custom validation failed: %v", err))
			}
		}
	}

	return nil
}

// validateFieldValue validates a field value against its type
func (sr *SchemaRegistry) validateFieldValue(fieldName string, value interface{}, fieldType FieldType) error {
	if value == nil {
		return nil // nil values are handled by required field check
	}

	switch fieldType {
	case FieldTypeString:
		if _, ok := value.(string); !ok {
			return NewValidationError(fieldName, value, "expected string")
		}

	case FieldTypeInt:
		switch v := value.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			// Valid integer types
		case float64:
			// JSON numbers are float64, check if it's actually an integer
			if v != float64(int64(v)) {
				return NewValidationError(fieldName, value, "expected integer")
			}
		case string:
			// Try to parse string as integer
			if _, err := strconv.ParseInt(v, 10, 64); err != nil {
				return NewValidationError(fieldName, value, "expected integer")
			}
		default:
			return NewValidationError(fieldName, value, "expected integer")
		}

	case FieldTypeFloat:
		switch v := value.(type) {
		case float32, float64:
			// Valid float types
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			// Integers can be converted to floats
		case string:
			// Try to parse string as float
			if _, err := strconv.ParseFloat(v, 64); err != nil {
				return NewValidationError(fieldName, value, "expected float")
			}
		default:
			return NewValidationError(fieldName, value, "expected float")
		}

	case FieldTypeBool:
		switch v := value.(type) {
		case bool:
			// Valid boolean
		case string:
			// Try to parse string as boolean
			if _, err := strconv.ParseBool(v); err != nil {
				return NewValidationError(fieldName, value, "expected boolean")
			}
		default:
			return NewValidationError(fieldName, value, "expected boolean")
		}

	case FieldTypeDatetime:
		switch v := value.(type) {
		case time.Time:
			// Valid time
		case string:
			// Try to parse string as time
			if _, err := time.Parse(time.RFC3339, v); err != nil {
				return NewValidationError(fieldName, value, "expected datetime in RFC3339 format")
			}
		default:
			return NewValidationError(fieldName, value, "expected datetime")
		}

	case FieldTypeJSON:
		// JSON can be any type, but let's ensure it's serializable
		if _, err := json.Marshal(value); err != nil {
			return NewValidationError(fieldName, value, "value is not JSON serializable")
		}

	default:
		return NewValidationError(fieldName, value, "unknown field type")
	}

	return nil
}

// isValidFieldType checks if a field type is valid
func (sr *SchemaRegistry) isValidFieldType(fieldType FieldType) bool {
	switch fieldType {
	case FieldTypeString, FieldTypeInt, FieldTypeFloat, FieldTypeBool, FieldTypeDatetime, FieldTypeJSON:
		return true
	default:
		return false
	}
}

// addDefaultFields adds standard fields to the schema if not present
func (sr *SchemaRegistry) addDefaultFields(schema *DataSchema) {
	if schema.Fields == nil {
		schema.Fields = make(map[string]FieldConfig)
	}

	// Add ID field if not present
	if _, exists := schema.Fields["id"]; !exists {
		schema.Fields["id"] = FieldConfig{
			Type:        FieldTypeString,
			Required:    true,
			Index:       true,
			Description: "Unique identifier for the event",
		}
	}

	// Add timestamp field if not present
	if _, exists := schema.Fields["timestamp"]; !exists {
		schema.Fields["timestamp"] = FieldConfig{
			Type:        FieldTypeDatetime,
			Required:    true,
			Index:       true,
			Description: "Event timestamp",
		}
	}
}

// ConvertValue converts a value to the appropriate type based on field configuration
func (sr *SchemaRegistry) ConvertValue(value interface{}, fieldType FieldType) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	switch fieldType {
	case FieldTypeString:
		return sr.convertToString(value)

	case FieldTypeInt:
		return sr.convertToInt(value)

	case FieldTypeFloat:
		return sr.convertToFloat(value)

	case FieldTypeBool:
		return sr.convertToBool(value)

	case FieldTypeDatetime:
		return sr.convertToDatetime(value)

	case FieldTypeJSON:
		return sr.convertToJSON(value)

	default:
		return nil, fmt.Errorf("unknown field type: %s", fieldType)
	}
}

// convertToJSON handles JSON field conversion with fast-path for strings
func (sr *SchemaRegistry) convertToJSON(value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	
	// Fast-path: if it's already a string, assume it's valid JSON
	if str, ok := value.(string); ok {
		return str, nil
	}
	
	// Convert to JSON string for non-string values
	jsonBytes, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal JSON: %w", err)
	}
	
	return string(jsonBytes), nil
}

// Helper conversion functions
func (sr *SchemaRegistry) convertToString(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	default:
		return fmt.Sprintf("%v", v), nil
	}
}

func (sr *SchemaRegistry) convertToInt(value interface{}) (int64, error) {
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		return int64(v), nil
	case float32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to int", value)
	}
}

func (sr *SchemaRegistry) convertToFloat(value interface{}) (float64, error) {
	switch v := value.(type) {
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return float64(reflect.ValueOf(v).Convert(reflect.TypeOf(float64(0))).Float()), nil
	case string:
		return strconv.ParseFloat(v, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to float", value)
	}
}

func (sr *SchemaRegistry) convertToBool(value interface{}) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case string:
		return strconv.ParseBool(v)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return reflect.ValueOf(v).Convert(reflect.TypeOf(int64(0))).Int() != 0, nil
	default:
		return false, fmt.Errorf("cannot convert %T to bool", value)
	}
}

func (sr *SchemaRegistry) convertToDatetime(value interface{}) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	case string:
		// Try multiple time formats
		formats := []string{
			time.RFC3339,
			time.RFC3339Nano,
			"2006-01-02T15:04:05",
			"2006-01-02 15:04:05",
			"2006-01-02",
		}
		
		for _, format := range formats {
			if t, err := time.Parse(format, v); err == nil {
				return t, nil
			}
		}
		return time.Time{}, fmt.Errorf("cannot parse datetime: %s", v)
	case int64:
		// Unix timestamp
		return time.Unix(v, 0), nil
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to datetime", value)
	}
}

// GenerateClickHouseSchema generates ClickHouse CREATE TABLE statement from schema
func (sr *SchemaRegistry) GenerateClickHouseSchema(schema *DataSchema, tableName string) string {
	var fields []string
	
	for fieldName, fieldConfig := range schema.Fields {
		clickHouseType := sr.getClickHouseType(fieldConfig.Type)
		if !fieldConfig.Required {
			clickHouseType = fmt.Sprintf("Nullable(%s)", clickHouseType)
		}
		fields = append(fields, fmt.Sprintf("    `%s` %s", fieldName, clickHouseType))
	}

	sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n%s\n)", tableName, strings.Join(fields, ",\n"))

	// Add engine and partitioning
	engine := "ENGINE = MergeTree()"
	if len(schema.Partitions) > 0 {
		partitionBy := strings.Join(schema.Partitions, ", ")
		engine = fmt.Sprintf("ENGINE = MergeTree() PARTITION BY (%s)", partitionBy)
	}

	// Add order by clause (use index fields or timestamp)
	orderBy := "timestamp"
	if len(schema.IndexFields) > 0 {
		orderBy = strings.Join(schema.IndexFields, ", ")
	}

	sql += fmt.Sprintf("\n%s\nORDER BY (%s)", engine, orderBy)

	// Add TTL if specified
	if schema.TTL != nil {
		sql += fmt.Sprintf("\nTTL timestamp + INTERVAL %d SECOND", int(schema.TTL.Seconds()))
	}

	return sql
}

// getClickHouseType converts field type to ClickHouse type
func (sr *SchemaRegistry) getClickHouseType(fieldType FieldType) string {
	switch fieldType {
	case FieldTypeString:
		return "String"
	case FieldTypeInt:
		return "Int64"
	case FieldTypeFloat:
		return "Float64"
	case FieldTypeBool:
		return "UInt8"
	case FieldTypeDatetime:
		return "DateTime"
	case FieldTypeJSON:
		return "String" // Store JSON as string in ClickHouse
	default:
		return "String"
	}
}