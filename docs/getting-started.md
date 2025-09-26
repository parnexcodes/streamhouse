# Getting Started with StreamHouse

StreamHouse is a powerful Go package for schema-driven data streaming and analytics with Redis Streams and ClickHouse backend. This guide will help you get up and running quickly.

## Prerequisites

- Go 1.19 or higher
- Redis server (6.0+)
- ClickHouse server (21.3+)

## Installation

```bash
go get github.com/parnexcodes/streamhouse
```

## Quick Start

### 1. Basic Setup

```go
package main

import (
    "context"
    "log"
    "time"
    
    "github.com/parnexcodes/streamhouse"
)

func main() {
    // Create configuration
    config := streamhouse.Config{
        Redis: streamhouse.RedisConfig{
            Host: "localhost",
            Port: 6379,
        },
        ClickHouse: streamhouse.ClickHouseConfig{
            Host:     "localhost",
            Port:     9000,
            Database: "analytics",
        },
        StreamName:    "events",
        BatchSize:     100,
        FlushInterval: 5 * time.Second,
        Workers:       4,
    }
    
    // Create client
    client, err := streamhouse.NewClient(config)
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()
    
    // Start background consumer
    ctx := context.Background()
    go client.StartConsumer(ctx)
    
    // Your application code here...
}
```

### 2. Define a Schema

```go
// Define your data schema
userActivitySchema := &streamhouse.DataSchema{
    Name: "user.activity",
    Fields: map[string]streamhouse.FieldConfig{
        "user_id":    {Type: "string", Required: true, Index: true},
        "action":     {Type: "string", Required: true},
        "timestamp":  {Type: "datetime", Required: true},
        "metadata":   {Type: "json", Required: false},
    },
    TTL: &[]time.Duration{30 * 24 * time.Hour}[0], // 30 days
}

// Register the schema
err := client.RegisterSchema(userActivitySchema)
if err != nil {
    log.Fatal(err)
}
```

### 3. Stream Data

#### Direct Streaming
```go
// Stream data directly
err := client.Stream("user.activity", map[string]interface{}{
    "user_id":   "user123",
    "action":    "login",
    "timestamp": time.Now(),
    "metadata":  map[string]interface{}{"ip": "192.168.1.1"},
})
if err != nil {
    log.Printf("Error streaming data: %v", err)
}
```

#### Async Streaming
```go
// Stream data asynchronously (non-blocking)
client.StreamAsync("user.activity", map[string]interface{}{
    "user_id":   "user123",
    "action":    "page_view",
    "timestamp": time.Now(),
    "metadata":  map[string]interface{}{"page": "/dashboard"},
})
```

#### Builder Pattern
```go
// Use the fluent builder API
client.Data("user.activity").
    WithUser("user123").
    WithAction("purchase").
    WithField("amount", 99.99).
    WithField("currency", "USD").
    WithTimestamp(time.Now()).
    StreamAsync()
```

## Configuration Options

### Environment Variables

You can configure StreamHouse using environment variables:

```bash
# Redis Configuration - supports host:port format or separate host/port
export STREAMHOUSE_REDIS_HOST="localhost:6379"  # or just "localhost"
export STREAMHOUSE_REDIS_PORT="6379"            # optional if port in host
export STREAMHOUSE_REDIS_PASSWORD=""

# ClickHouse Configuration - supports host:port format or separate host/port  
export STREAMHOUSE_CLICKHOUSE_HOST="localhost:9000"  # or just "localhost"
export STREAMHOUSE_CLICKHOUSE_PORT="9000"            # optional if port in host
export STREAMHOUSE_CLICKHOUSE_DATABASE="analytics"
export STREAMHOUSE_CLICKHOUSE_USERNAME="default"
export STREAMHOUSE_CLICKHOUSE_PASSWORD=""

# StreamHouse Configuration
export STREAMHOUSE_STREAM_NAME="events"
```

### Configuration File

Create a `config.yaml` file:

```yaml
redis:
  host: "localhost"
  port: 6379
  password: ""
  db: 0
  pool_size: 10

clickhouse:
  host: "localhost"
  port: 9000
  database: "analytics"
  username: "default"
  password: ""

stream_name: "events"
batch_size: 100
flush_interval: "5s"
workers: 4

consumer:
  group_name: "streamhouse-consumers"
  consumer_name: "consumer-1"
  dead_letter_queue: true
  dead_letter_ttl: "24h"
  claim_interval: "30s"

monitoring:
  enabled: true
  metrics_interval: "30s"
  health_check: true
  health_interval: "10s"

logging:
  level: "info"
  format: "json"
  output: "stdout"
```

Load the configuration:

```go
config, err := streamhouse.LoadConfig("config.yaml")
if err != nil {
    log.Fatal(err)
}

client, err := streamhouse.NewClient(*config)
if err != nil {
    log.Fatal(err)
}
```

## Error Handling

StreamHouse provides comprehensive error handling:

```go
// Check for specific error types
err := client.Stream("user.activity", data)
if err != nil {
    switch e := err.(type) {
    case *streamhouse.SchemaError:
        log.Printf("Schema error: %v", e)
    case *streamhouse.ValidationError:
        log.Printf("Validation error: %v", e)
    case *streamhouse.ConnectionError:
        log.Printf("Connection error: %v", e)
    default:
        log.Printf("Unknown error: %v", e)
    }
}
```

## Health Checks

Monitor the health of your StreamHouse client:

```go
// Check overall health
err := client.Health()
if err != nil {
    log.Printf("Health check failed: %v", err)
}

// Get detailed metrics
metrics := client.GetMetrics()
log.Printf("Events processed: %d", metrics.EventsProcessed)
log.Printf("Events failed: %d", metrics.EventsFailed)
log.Printf("Average latency: %.2fms", metrics.AverageLatency)
```

## Next Steps

- Read the [Schema Reference](schema-reference.md) to learn about advanced schema features
- Check out the [Configuration Guide](configuration.md) for detailed configuration options
- Review [Best Practices](best-practices.md) for production deployments
- Explore the examples in the `examples/` directory

## Common Patterns

### Audit Logging
```go
client.Data("audit.user.login").
    WithUser(userID).
    WithAction("login").
    WithIP(clientIP).
    WithUserAgent(userAgent).
    WithTimestamp(time.Now()).
    StreamAsync()
```

### Analytics Events
```go
client.Data("analytics.pageview").
    WithUser(userID).
    WithSession(sessionID).
    WithField("page_url", pageURL).
    WithField("referrer", referrer).
    WithDuration(pageLoadTime).
    StreamAsync()
```

### IoT Data
```go
client.Data("iot.sensor.reading").
    WithField("device_id", deviceID).
    WithField("temperature", temperature).
    WithField("humidity", humidity).
    WithLocation(country, region, city).
    WithTimestamp(time.Now()).
    StreamAsync()
```

## Troubleshooting

### Common Issues

1. **Connection Errors**: Ensure Redis and ClickHouse are running and accessible
2. **Schema Validation Errors**: Check that your data matches the registered schema
3. **Performance Issues**: Adjust batch size and worker count based on your load
4. **Memory Usage**: Monitor and tune the flush interval and batch size

### Debug Mode

Enable debug logging to troubleshoot issues:

```go
config.Logging.Level = "debug"
```

For more detailed troubleshooting, see the [Best Practices](best-practices.md) guide.