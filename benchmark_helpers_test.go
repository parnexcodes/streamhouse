package streamhouse

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/parnexcodes/streamhouse/storage"
	"github.com/redis/go-redis/v9"
)

type benchmarkEnvironment struct {
	client     *StreamHouseClient
	redis      *redis.Client
	clickhouse *benchmarkClickHouse
	mini       *miniredis.Miniredis
}

type benchmarkClickHouse struct {
	insertCount int64
}

func (m *benchmarkClickHouse) Connect(config ClickHouseConfig) error                  { return nil }
func (m *benchmarkClickHouse) CreateTable(schema *DataSchema, tableName string) error { return nil }
func (m *benchmarkClickHouse) InsertBatch(tableName string, events []*StreamEvent) error {
	m.insertCount += int64(len(events))
	return nil
}
func (m *benchmarkClickHouse) InsertData(tableName string, data []map[string]interface{}) error {
	m.insertCount += int64(len(data))
	return nil
}
func (m *benchmarkClickHouse) Query(query string, args ...interface{}) (interface{}, error) {
	return nil, nil
}
func (m *benchmarkClickHouse) Close() error  { return nil }
func (m *benchmarkClickHouse) Health() error { return nil }

func setupBenchmarkClient(tb testing.TB) (*benchmarkEnvironment, func()) {
	tb.Helper()

	mini := miniredis.RunT(tb)

	ctx, cancel := context.WithCancel(context.Background())

	config := DefaultConfig()
	host, portStr, err := net.SplitHostPort(mini.Addr())
	if err != nil {
		tb.Fatalf("failed to parse miniredis address: %v", err)
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		tb.Fatalf("failed to parse miniredis port: %v", err)
	}

	config.Redis.Host = host
	config.Redis.Port = port
	config.StreamName = "benchmark-stream"
	config.BatchSize = 256
	config.Workers = 4

	redisClient := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	if err := redisClient.Ping(ctx).Err(); err != nil {
		tb.Fatalf("failed to ping miniredis: %v", err)
	}

	clickhouse := &benchmarkClickHouse{}

	client := &StreamHouseClient{
		config:         config,
		redisClient:    redisClient,
		clickHouseConn: clickhouse,
		schemaRegistry: NewSchemaRegistry(),
		metrics:        &Metrics{},
		ctx:            ctx,
		cancel:         cancel,
		eventChan:      make(chan *StreamEvent, config.BatchSize*2),
	}

	env := &benchmarkEnvironment{
		client:     client,
		redis:      redisClient,
		clickhouse: clickhouse,
		mini:       mini,
	}

	cleanup := func() {
		cancel()
		close(client.eventChan)
		_ = redisClient.Close()
		mini.Close()
	}

	return env, cleanup
}

func registerBenchmarkSchema(tb testing.TB, client *StreamHouseClient) *DataSchema {
	tb.Helper()

	ttl := time.Hour * 24

	schema := &DataSchema{
		Name: "benchmark_event",
		Fields: map[string]FieldConfig{
			"timestamp": {
				Type:     FieldTypeDatetime,
				Required: true,
			},
			"user_id": {
				Type:     FieldTypeString,
				Required: true,
			},
			"session_id": {
				Type:     FieldTypeString,
				Required: true,
			},
			"success": {
				Type: FieldTypeBool,
			},
			"duration_ms": {
				Type: FieldTypeFloat,
			},
			"count": {
				Type: FieldTypeInt,
			},
			"score": {
				Type: FieldTypeFloat,
			},
			"payload": {
				Type: FieldTypeJSON,
			},
		},
		TTL:         &ttl,
		Partitions:  []string{"timestamp"},
		IndexFields: []string{"user_id", "session_id"},
	}

	if err := client.RegisterSchema(schema); err != nil {
		tb.Fatalf("failed to register benchmark schema: %v", err)
	}

	registered, err := client.schemaRegistry.Get(schema.Name)
	if err != nil {
		tb.Fatalf("failed to retrieve registered schema: %v", err)
	}

	return registered
}

func generateBenchmarkEvent(i int) map[string]interface{} {
	return map[string]interface{}{
		"user_id":     fmt.Sprintf("user-%d", i%1024),
		"session_id":  fmt.Sprintf("session-%d", i%256),
		"success":     i%2 == 0,
		"duration_ms": float64(100 + (i % 50)),
		"count":       i,
		"score":       float64(i%100) / 10.0,
		"payload": map[string]interface{}{
			"feature":  "benchmark",
			"attempts": i % 5,
		},
	}
}

func generateBenchmarkMessages(schemaName string, size int) []*storage.StreamMessage {
	messages := make([]*storage.StreamMessage, size)
	now := time.Now()

	// Pre-marshal metadata once for reuse (metadata reuse optimization)
	metadata := map[string]interface{}{
		"retry_count": 0,
	}
	metadataJSON := `{"retry_count":0}`

	for i := 0; i < size; i++ {
		data := generateBenchmarkEvent(i)
		data["id"] = uuid.New().String()
		data["timestamp"] = now.Add(time.Duration(i) * time.Millisecond)

		messages[i] = &storage.StreamMessage{
			ID:           fmt.Sprintf("%d-0", i+1),
			DataType:     schemaName,
			Data:         data,
			Metadata:     metadata,
			MetadataJSON: metadataJSON, // Use pre-marshaled metadata
		}
	}

	return messages
}
