package streamhouse

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/google/uuid"
)

// StreamHouseClient implements the Client interface
type StreamHouseClient struct {
	config         *Config
	redisClient    *redis.Client
	clickHouseConn ClickHouseConnection
	schemaRegistry *SchemaRegistry
	consumer       *Consumer
	metrics        *Metrics
	
	// Internal state
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	mu         sync.RWMutex
	closed     bool
	
	// Channels for async processing
	eventChan chan *StreamEvent
}

// ClickHouseConnection interface for ClickHouse operations
type ClickHouseConnection interface {
	Connect(config ClickHouseConfig) error
	CreateTable(schema *DataSchema, tableName string) error
	InsertBatch(tableName string, events []*StreamEvent) error
	Query(query string, args ...interface{}) (interface{}, error)
	Close() error
	Health() error
}

// NewClient creates a new StreamHouse client
func NewClient(config *Config) (*StreamHouseClient, error) {
	if config == nil {
		config = DefaultConfig()
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	client := &StreamHouseClient{
		config:         config,
		schemaRegistry: NewSchemaRegistry(),
		metrics:        &Metrics{},
		ctx:            ctx,
		cancel:         cancel,
		eventChan:      make(chan *StreamEvent, config.BatchSize*2), // Buffer for async events
	}

	// Initialize Redis connection
	if err := client.initRedis(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to initialize Redis: %w", err)
	}

	// Initialize ClickHouse connection
	if err := client.initClickHouse(); err != nil {
		cancel()
		client.redisClient.Close()
		return nil, fmt.Errorf("failed to initialize ClickHouse: %w", err)
	}

	// Initialize consumer
	consumer, err := NewConsumer(client, client.config.Consumer)
	if err != nil {
		cancel()
		client.redisClient.Close()
		return nil, fmt.Errorf("failed to initialize consumer: %w", err)
	}
	client.consumer = consumer

	return client, nil
}

// initRedis initializes the Redis connection
func (c *StreamHouseClient) initRedis() error {
	opts := &redis.Options{
		Addr:         c.config.RedisAddr(),
		Password:     c.config.Redis.Password,
		DB:           c.config.Redis.DB,
		PoolSize:     c.config.Redis.PoolSize,
		MinIdleConns: c.config.Redis.MinIdleConns,
		MaxRetries:   c.config.Redis.MaxRetries,
		DialTimeout:  c.config.Redis.DialTimeout,
		ReadTimeout:  c.config.Redis.ReadTimeout,
		WriteTimeout: c.config.Redis.WriteTimeout,
		ConnMaxIdleTime: c.config.Redis.IdleTimeout,
	}

	// Add TLS configuration if enabled
	if c.config.Redis.TLS != nil && c.config.Redis.TLS.Enabled {
		// TLS configuration would be added here
		// opts.TLSConfig = &tls.Config{...}
	}

	c.redisClient = redis.NewClient(opts)

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := c.redisClient.Ping(ctx).Err(); err != nil {
		return NewConnectionError("redis", err)
	}

	return nil
}

// initClickHouse initializes the ClickHouse connection
func (c *StreamHouseClient) initClickHouse() error {
	// This would be implemented with actual ClickHouse driver
	// For now, we'll use a placeholder
	c.clickHouseConn = &MockClickHouseConnection{}
	return c.clickHouseConn.Connect(c.config.ClickHouse)
}

// RegisterSchema registers a new data schema
func (c *StreamHouseClient) RegisterSchema(schema *DataSchema) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return ErrClientClosed
	}

	if err := c.schemaRegistry.Register(schema); err != nil {
		return err
	}

	// Create ClickHouse table for the schema
	tableName := c.getTableName(schema.Name)
	if err := c.clickHouseConn.CreateTable(schema, tableName); err != nil {
		// Rollback schema registration
		c.schemaRegistry.Unregister(schema.Name)
		return fmt.Errorf("failed to create ClickHouse table: %w", err)
	}

	return nil
}

// Stream sends data synchronously
func (c *StreamHouseClient) Stream(dataType string, data map[string]interface{}) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return ErrClientClosed
	}

	// Validate data against schema
	if err := c.schemaRegistry.ValidateData(dataType, data); err != nil {
		return err
	}

	// Create stream event
	event := &StreamEvent{
		ID:        uuid.New().String(),
		Schema:    dataType,
		Data:      data,
		Timestamp: time.Now(),
		Metadata:  make(map[string]interface{}),
	}

	// Add to Redis stream
	streamKey := c.getStreamKey(dataType)
	fields := c.eventToRedisFields(event)

	ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
	defer cancel()

	if err := c.redisClient.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		Values: fields,
	}).Err(); err != nil {
		c.metrics.EventsFailed++
		return NewStreamError(streamKey, event.ID, err)
	}

	c.metrics.EventsProcessed++
	c.metrics.LastProcessedTime = time.Now()

	return nil
}

// StreamAsync sends data asynchronously
func (c *StreamHouseClient) StreamAsync(dataType string, data map[string]interface{}) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return
	}

	// Validate data against schema
	if err := c.schemaRegistry.ValidateData(dataType, data); err != nil {
		c.metrics.EventsFailed++
		return
	}

	// Create stream event
	event := &StreamEvent{
		ID:        uuid.New().String(),
		Schema:    dataType,
		Data:      data,
		Timestamp: time.Now(),
		Metadata:  make(map[string]interface{}),
	}

	// Send to async channel (non-blocking)
	select {
	case c.eventChan <- event:
		// Event queued successfully
	default:
		// Channel is full, increment failed counter
		c.metrics.EventsFailed++
	}
}

// Data returns a new data builder for the specified data type
func (c *StreamHouseClient) Data(dataType string) *DataBuilder {
	return NewDataBuilder(c, dataType)
}

// StartConsumer starts the background consumer
func (c *StreamHouseClient) StartConsumer(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return ErrClientClosed
	}

	// Start async event processor
	c.wg.Add(1)
	go c.processAsyncEvents()

	// Start consumer
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.consumer.Start()
	}()

	// Start health checker if enabled
	if c.config.Monitoring.HealthCheck {
		c.wg.Add(1)
		go c.healthChecker()
	}

	return nil
}

// Close closes the client and all connections
func (c *StreamHouseClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true
	c.cancel()

	// Close async event channel
	close(c.eventChan)

	// Wait for all goroutines to finish
	c.wg.Wait()

	// Close connections
	var errs []error
	if c.redisClient != nil {
		if err := c.redisClient.Close(); err != nil {
			errs = append(errs, NewConnectionError("redis", err))
		}
	}

	if c.clickHouseConn != nil {
		if err := c.clickHouseConn.Close(); err != nil {
			errs = append(errs, NewConnectionError("clickhouse", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors during close: %v", errs)
	}

	return nil
}

// Health checks the health of all connections
func (c *StreamHouseClient) Health() error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return ErrClientClosed
	}

	// Check Redis connection
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := c.redisClient.Ping(ctx).Err(); err != nil {
		return NewConnectionError("redis", err)
	}

	// Check ClickHouse connection
	if err := c.clickHouseConn.Health(); err != nil {
		return NewConnectionError("clickhouse", err)
	}

	return nil
}

// GetMetrics returns current metrics
func (c *StreamHouseClient) GetMetrics() *Metrics {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Return a copy to avoid race conditions
	return &Metrics{
		EventsProcessed:   c.metrics.EventsProcessed,
		EventsFailed:      c.metrics.EventsFailed,
		BatchesProcessed:  c.metrics.BatchesProcessed,
		LastProcessedTime: c.metrics.LastProcessedTime,
		AverageLatency:    c.metrics.AverageLatency,
	}
}

// processAsyncEvents processes events from the async channel
func (c *StreamHouseClient) processAsyncEvents() {
	defer c.wg.Done()

	for {
		select {
		case event, ok := <-c.eventChan:
			if !ok {
				return // Channel closed
			}

			streamKey := c.getStreamKey(event.Schema)
			fields := c.eventToRedisFields(event)

			ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
			if err := c.redisClient.XAdd(ctx, &redis.XAddArgs{
				Stream: streamKey,
				Values: fields,
			}).Err(); err != nil {
				c.metrics.EventsFailed++
			} else {
				c.metrics.EventsProcessed++
				c.metrics.LastProcessedTime = time.Now()
			}
			cancel()

		case <-c.ctx.Done():
			return
		}
	}
}

// healthChecker periodically checks connection health
func (c *StreamHouseClient) healthChecker() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.config.Monitoring.HealthInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := c.Health(); err != nil {
				// Log health check failure
				// In a real implementation, this would use a proper logger
				fmt.Printf("Health check failed: %v\n", err)
			}

		case <-c.ctx.Done():
			return
		}
	}
}

// Helper methods
func (c *StreamHouseClient) getStreamKey(schemaName string) string {
	return fmt.Sprintf("%s:%s", c.config.StreamName, schemaName)
}

func (c *StreamHouseClient) getTableName(schemaName string) string {
	return fmt.Sprintf("streamhouse_%s", schemaName)
}

func (c *StreamHouseClient) eventToRedisFields(event *StreamEvent) map[string]interface{} {
	fields := make(map[string]interface{})
	fields["id"] = event.ID
	fields["schema"] = event.Schema
	fields["timestamp"] = event.Timestamp.Unix()

	// Add data fields
	for k, v := range event.Data {
		fields[k] = v
	}

	// Add metadata fields
	for k, v := range event.Metadata {
		fields[fmt.Sprintf("meta_%s", k)] = v
	}

	return fields
}

// MockClickHouseConnection is a placeholder implementation
type MockClickHouseConnection struct{}

func (m *MockClickHouseConnection) Connect(config ClickHouseConfig) error {
	return nil
}

func (m *MockClickHouseConnection) CreateTable(schema *DataSchema, tableName string) error {
	return nil
}

func (m *MockClickHouseConnection) InsertBatch(tableName string, events []*StreamEvent) error {
	return nil
}

func (m *MockClickHouseConnection) Query(query string, args ...interface{}) (interface{}, error) {
	return nil, nil
}

func (m *MockClickHouseConnection) Close() error {
	return nil
}

func (m *MockClickHouseConnection) Health() error {
	return nil
}