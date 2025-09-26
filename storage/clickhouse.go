package storage

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// ClickHouseClient handles ClickHouse operations
type ClickHouseClient struct {
	conn   driver.Conn
	ctx    context.Context
	config ClickHouseConfig
}

// ClickHouseConfig holds ClickHouse connection configuration
type ClickHouseConfig struct {
	Host     string
	Port     int
	Database string
	Username string
	Password string
	Settings map[string]interface{}
}

// NewClickHouseClient creates a new ClickHouse client
func NewClickHouseClient(config ClickHouseConfig) (*ClickHouseClient, error) {
	options := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", config.Host, config.Port)},
		Auth: clickhouse.Auth{
			Database: config.Database,
			Username: config.Username,
			Password: config.Password,
		},
		Settings: config.Settings,
	}

	conn, err := clickhouse.Open(options)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	ctx := context.Background()
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	return &ClickHouseClient{
		conn:   conn,
		ctx:    ctx,
		config: config,
	}, nil
}

// TableSchema represents a ClickHouse table schema
type TableSchema struct {
	Name        string
	Fields      map[string]FieldDefinition
	Engine      string
	OrderBy     []string
	PartitionBy string
	TTL         *time.Duration
	Settings    map[string]interface{}
}

// FieldDefinition represents a field in a ClickHouse table
type FieldDefinition struct {
	Type     string
	Nullable bool
	Default  interface{}
	Codec    string
	Comment  string
}

// CreateTable creates a table based on the schema
func (c *ClickHouseClient) CreateTable(schema TableSchema) error {
	query := c.buildCreateTableQuery(schema)

	if err := c.conn.Exec(c.ctx, query); err != nil {
		return fmt.Errorf("failed to create table %s: %w", schema.Name, err)
	}

	return nil
}

// buildCreateTableQuery builds the CREATE TABLE query
func (c *ClickHouseClient) buildCreateTableQuery(schema TableSchema) string {
	var parts []string

	// Start with CREATE TABLE
	parts = append(parts, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (", schema.Name))

	// Add fields
	var fieldDefs []string
	for name, field := range schema.Fields {
		// For now, use base types without Nullable to avoid syntax issues
		fieldDef := fmt.Sprintf("    `%s` %s", name, field.Type)

		if field.Default != nil {
			fieldDef += fmt.Sprintf(" DEFAULT %v", field.Default)
		}

		if field.Codec != "" {
			fieldDef += fmt.Sprintf(" CODEC(%s)", field.Codec)
		}

		// Temporarily remove COMMENT to avoid syntax issues
		// if field.Comment != "" {
		//	fieldDef += fmt.Sprintf(" COMMENT '%s'", field.Comment)
		// }

		fieldDefs = append(fieldDefs, fieldDef)
	}

	parts = append(parts, strings.Join(fieldDefs, ",\n"))
	parts = append(parts, ")")

	// Add engine
	engine := schema.Engine
	if engine == "" {
		engine = "MergeTree()"
	}
	parts = append(parts, fmt.Sprintf("ENGINE = %s", engine))

	// Add ORDER BY
	if len(schema.OrderBy) > 0 {
		parts = append(parts, fmt.Sprintf("ORDER BY (%s)", strings.Join(schema.OrderBy, ", ")))
	}

	// Add PARTITION BY
	if schema.PartitionBy != "" {
		parts = append(parts, fmt.Sprintf("PARTITION BY %s", schema.PartitionBy))
	}

	// Add TTL
	if schema.TTL != nil {
		parts = append(parts, fmt.Sprintf("TTL timestamp + INTERVAL %d SECOND", int(schema.TTL.Seconds())))
	}

	// Add settings
	if len(schema.Settings) > 0 {
		var settings []string
		for key, value := range schema.Settings {
			settings = append(settings, fmt.Sprintf("%s = %v", key, value))
		}
		parts = append(parts, fmt.Sprintf("SETTINGS %s", strings.Join(settings, ", ")))
	}

	return strings.Join(parts, "\n")
}

// InsertData inserts data into a table
func (c *ClickHouseClient) InsertData(tableName string, data []map[string]interface{}) error {
	if len(data) == 0 {
		return nil
	}

	// Get column names from the first row
	var columns []string
	for col := range data[0] {
		columns = append(columns, col)
	}

	// Build INSERT query
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES", tableName, strings.Join(columns, ", "))

	batch, err := c.conn.PrepareBatch(c.ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	// Add rows to batch
	for _, row := range data {
		var values []interface{}
		for _, col := range columns {
			values = append(values, row[col])
		}

		if err := batch.Append(values...); err != nil {
			return fmt.Errorf("failed to append row to batch: %w", err)
		}
	}

	// Execute batch
	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}

	return nil
}

// InsertDataWithColumns inserts data with specific columns
func (c *ClickHouseClient) InsertDataWithColumns(tableName string, columns []string, data [][]interface{}) error {
	if len(data) == 0 {
		return nil
	}

	// Build INSERT query
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES", tableName, strings.Join(columns, ", "))

	batch, err := c.conn.PrepareBatch(c.ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	// Add rows to batch
	for _, row := range data {
		if err := batch.Append(row...); err != nil {
			return fmt.Errorf("failed to append row to batch: %w", err)
		}
	}

	// Execute batch
	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}

	return nil
}

// Query executes a SELECT query and returns results
func (c *ClickHouseClient) Query(query string, args ...interface{}) ([]map[string]interface{}, error) {
	rows, err := c.conn.Query(c.ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Get column names
	columns := rows.Columns()

	var results []map[string]interface{}
	for rows.Next() {
		// Create slice to hold column values
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// Scan row
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Create result map
		result := make(map[string]interface{})
		for i, col := range columns {
			result[col] = values[i]
		}

		results = append(results, result)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return results, nil
}

// QueryRow executes a query that returns a single row
func (c *ClickHouseClient) QueryRow(query string, args ...interface{}) (map[string]interface{}, error) {
	results, err := c.Query(query, args...)
	if err != nil {
		return nil, err
	}

	if len(results) == 0 {
		return nil, sql.ErrNoRows
	}

	return results[0], nil
}

// Exec executes a query without returning results
func (c *ClickHouseClient) Exec(query string, args ...interface{}) error {
	return c.conn.Exec(c.ctx, query, args...)
}

// TableExists checks if a table exists
func (c *ClickHouseClient) TableExists(tableName string) (bool, error) {
	query := `
		SELECT COUNT(*) as count 
		FROM system.tables 
		WHERE database = ? AND name = ?
	`

	result, err := c.QueryRow(query, c.config.Database, tableName)
	if err != nil {
		return false, fmt.Errorf("failed to check table existence: %w", err)
	}

	count, ok := result["count"].(uint64)
	if !ok {
		return false, fmt.Errorf("unexpected result type for count")
	}

	return count > 0, nil
}

// GetTableSchema retrieves the schema of an existing table
func (c *ClickHouseClient) GetTableSchema(tableName string) (*TableSchema, error) {
	query := `
		SELECT 
			name,
			type,
			default_kind,
			default_expression,
			comment
		FROM system.columns 
		WHERE database = ? AND table = ?
		ORDER BY position
	`

	results, err := c.Query(query, c.config.Database, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get table schema: %w", err)
	}

	schema := &TableSchema{
		Name:   tableName,
		Fields: make(map[string]FieldDefinition),
	}

	for _, row := range results {
		name := row["name"].(string)
		fieldType := row["type"].(string)
		comment := ""
		if row["comment"] != nil {
			comment = row["comment"].(string)
		}

		schema.Fields[name] = FieldDefinition{
			Type:    fieldType,
			Comment: comment,
		}
	}

	return schema, nil
}

// DropTable drops a table
func (c *ClickHouseClient) DropTable(tableName string) error {
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	return c.conn.Exec(c.ctx, query)
}

// TruncateTable truncates a table
func (c *ClickHouseClient) TruncateTable(tableName string) error {
	query := fmt.Sprintf("TRUNCATE TABLE %s", tableName)
	return c.conn.Exec(c.ctx, query)
}

// OptimizeTable optimizes a table
func (c *ClickHouseClient) OptimizeTable(tableName string) error {
	query := fmt.Sprintf("OPTIMIZE TABLE %s", tableName)
	return c.conn.Exec(c.ctx, query)
}

// GetTableStats returns statistics about a table
func (c *ClickHouseClient) GetTableStats(tableName string) (map[string]interface{}, error) {
	query := `
		SELECT 
			COUNT(*) as row_count,
			uniqExact(*) as unique_rows,
			formatReadableSize(sum(data_compressed_bytes)) as compressed_size,
			formatReadableSize(sum(data_uncompressed_bytes)) as uncompressed_size
		FROM system.parts 
		WHERE database = ? AND table = ? AND active = 1
	`

	return c.QueryRow(query, c.config.Database, tableName)
}

// CreateIndex creates an index on a table
func (c *ClickHouseClient) CreateIndex(tableName, indexName string, columns []string, indexType string) error {
	if indexType == "" {
		indexType = "minmax"
	}

	query := fmt.Sprintf(
		"ALTER TABLE %s ADD INDEX %s (%s) TYPE %s GRANULARITY 1",
		tableName,
		indexName,
		strings.Join(columns, ", "),
		indexType,
	)

	return c.conn.Exec(c.ctx, query)
}

// DropIndex drops an index from a table
func (c *ClickHouseClient) DropIndex(tableName, indexName string) error {
	query := fmt.Sprintf("ALTER TABLE %s DROP INDEX %s", tableName, indexName)
	return c.conn.Exec(c.ctx, query)
}

// GetDatabaseInfo returns information about the database
func (c *ClickHouseClient) GetDatabaseInfo() (map[string]interface{}, error) {
	query := `
		SELECT 
			name,
			engine,
			formatReadableSize(sum(bytes_on_disk)) as total_size,
			COUNT(*) as table_count
		FROM system.tables 
		WHERE database = ?
		GROUP BY name, engine
	`

	return c.QueryRow(query, c.config.Database)
}

// Ping tests the connection to ClickHouse
func (c *ClickHouseClient) Ping() error {
	return c.conn.Ping(c.ctx)
}

// Close closes the ClickHouse connection
func (c *ClickHouseClient) Close() error {
	return c.conn.Close()
}

// ConvertGoTypeToClickHouse converts Go types to ClickHouse types
func ConvertGoTypeToClickHouse(goType reflect.Type, nullable bool) string {
	var clickHouseType string

	switch goType.Kind() {
	case reflect.String:
		clickHouseType = "String"
	case reflect.Int, reflect.Int32:
		clickHouseType = "Int32"
	case reflect.Int64:
		clickHouseType = "Int64"
	case reflect.Int8:
		clickHouseType = "Int8"
	case reflect.Int16:
		clickHouseType = "Int16"
	case reflect.Uint, reflect.Uint32:
		clickHouseType = "UInt32"
	case reflect.Uint64:
		clickHouseType = "UInt64"
	case reflect.Uint8:
		clickHouseType = "UInt8"
	case reflect.Uint16:
		clickHouseType = "UInt16"
	case reflect.Float32:
		clickHouseType = "Float32"
	case reflect.Float64:
		clickHouseType = "Float64"
	case reflect.Bool:
		clickHouseType = "Bool"
	case reflect.Slice:
		if goType.Elem().Kind() == reflect.Uint8 {
			clickHouseType = "String" // byte slice as string
		} else {
			elemType := ConvertGoTypeToClickHouse(goType.Elem(), false)
			clickHouseType = fmt.Sprintf("Array(%s)", elemType)
		}
	case reflect.Map, reflect.Struct:
		clickHouseType = "String" // JSON as string
	default:
		if goType == reflect.TypeOf(time.Time{}) {
			clickHouseType = "DateTime"
		} else {
			clickHouseType = "String" // fallback
		}
	}

	if nullable {
		clickHouseType = fmt.Sprintf("Nullable(%s)", clickHouseType)
	}

	return clickHouseType
}

// BatchInsert provides a more efficient way to insert large amounts of data
type BatchInsert struct {
	client    *ClickHouseClient
	tableName string
	columns   []string
	batch     driver.Batch
	batchSize int
	count     int
}

// NewBatchInsert creates a new batch insert helper
func (c *ClickHouseClient) NewBatchInsert(tableName string, columns []string, batchSize int) (*BatchInsert, error) {
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES", tableName, strings.Join(columns, ", "))

	batch, err := c.conn.PrepareBatch(c.ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare batch: %w", err)
	}

	return &BatchInsert{
		client:    c,
		tableName: tableName,
		columns:   columns,
		batch:     batch,
		batchSize: batchSize,
		count:     0,
	}, nil
}

// Add adds a row to the batch
func (bi *BatchInsert) Add(values ...interface{}) error {
	if err := bi.batch.Append(values...); err != nil {
		return fmt.Errorf("failed to append to batch: %w", err)
	}

	bi.count++

	// Auto-flush if batch size is reached
	if bi.count >= bi.batchSize {
		return bi.Flush()
	}

	return nil
}

// AddMap adds a row from a map to the batch
func (bi *BatchInsert) AddMap(data map[string]interface{}) error {
	var values []interface{}
	for _, col := range bi.columns {
		values = append(values, data[col])
	}
	return bi.Add(values...)
}

// Flush sends the current batch
func (bi *BatchInsert) Flush() error {
	if bi.count == 0 {
		return nil
	}

	if err := bi.batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}

	// Prepare new batch
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES", bi.tableName, strings.Join(bi.columns, ", "))
	batch, err := bi.client.conn.PrepareBatch(bi.client.ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare new batch: %w", err)
	}

	bi.batch = batch
	bi.count = 0

	return nil
}

// Close flushes any remaining data and closes the batch
func (bi *BatchInsert) Close() error {
	return bi.Flush()
}
