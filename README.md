# StreamHouse

[![Go Version](https://img.shields.io/badge/go-%3E%3D1.21-blue.svg)](https://golang.org/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A powerful Go package for schema-driven data streaming and analytics with Redis Streams and ClickHouse backend. StreamHouse provides a flexible schema system for reliable data streaming, real-time analytics, event processing, and high-performance data ingestion.

## 🚀 Features

- **Schema-Driven Architecture**: Flexible schema system with field validation and type safety
- **Dual Storage Backend**: Redis Streams for reliable queuing + ClickHouse for analytics
- **Data Builder API**: Builder pattern for constructing events and batches
- **Background Processing**: Efficient batch processing with configurable workers
- **Auto Table Management**: Automatic ClickHouse table creation from schemas
- **Consumer Groups**: Horizontal scaling with Redis Streams consumer groups
- **Error Handling**: Comprehensive error handling with configurable retry logic
- **Health Monitoring**: Built-in health checks and connection monitoring
- **Production Ready**: Connection pooling, graceful shutdown, and resource management

## 📦 Installation

```bash
go get github.com/parnexcodes/streamhouse
```

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Application   │───▶│   StreamHouse   │───▶│  Redis Streams  │
│                 │    │     Client      │    │   (Queue)       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                        │
                                ▼                        ▼
                       ┌─────────────────┐    ┌─────────────────┐
                       │    Schema       │    │   Background    │
                       │   Registry      │    │   Consumer      │
                       └─────────────────┘    └─────────────────┘
                                                        │
                                                        ▼
                                              ┌─────────────────┐
                                              │   ClickHouse    │
                                              │   (Analytics)   │
                                              └─────────────────┘
```

## 🚀 Quick Start

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
    // Configure StreamHouse
    config := streamhouse.Config{
        Redis: streamhouse.RedisConfig{
            Addr:     "localhost:6379",
            Password: "",
            DB:       0,
        },
        ClickHouse: streamhouse.ClickHouseConfig{
            Host:     "localhost",
            Port:     9000,
            Database: "analytics",
            Username: "default",
            Password: "",
        },
        StreamName:    "events",
        BatchSize:     100,
        FlushInterval: 5 * time.Second,
        Workers:       3,
    }
    
    // Create client
    client, err := streamhouse.NewClient(config)
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()
    
    // Register schema
    schema := &streamhouse.DataSchema{
        Name: "user.signup",
        Fields: map[string]streamhouse.FieldConfig{
            "user_id":    {Type: "string", Required: true, Index: true},
            "email":      {Type: "string", Required: true},
            "name":       {Type: "string", Required: true},
            "plan":       {Type: "string", Required: false},
            "metadata":   {Type: "json", Required: false},
        },
    }
    
    if err := client.RegisterSchema(schema); err != nil {
        log.Fatal(err)
    }
    
    // Start background consumer
    ctx := context.Background()
    go client.StartConsumer(ctx)
    
    // Stream data using builder pattern
    err = client.Data("user.signup").
        WithUser("user123").
        WithField("email", "user@example.com").
        WithField("name", "John Doe").
        WithField("plan", "premium").
        Stream()
    if err != nil {
        log.Printf("Failed to stream data: %v", err)
    }
}
```
```

### 2. Schema Definition

```go
// User activity schema
userSchema := &streamhouse.DataSchema{
    Name: "user.activity",
    Fields: map[string]streamhouse.FieldConfig{
        "user_id":     {Type: "string", Required: true, Index: true},
        "action":      {Type: "string", Required: true},
        "resource":    {Type: "string", Required: false},
        "timestamp":   {Type: "datetime", Required: true},
        "metadata":    {Type: "json", Required: false},
    },
}

// Analytics schema
analyticsSchema := &streamhouse.DataSchema{
    Name: "analytics.pageview",
    Fields: map[string]streamhouse.FieldConfig{
        "user_id":     {Type: "string", Required: true, Index: true},
        "session_id":  {Type: "string", Required: true, Index: true},
        "page_url":    {Type: "string", Required: true},
        "referrer":    {Type: "string", Required: false},
        "duration_ms": {Type: "int", Required: false},
        "user_agent":  {Type: "string", Required: false},
    },
}

// IoT sensor schema
sensorSchema := &streamhouse.DataSchema{
    Name: "iot.sensor.reading",
    Fields: map[string]streamhouse.FieldConfig{
        "device_id":   {Type: "string", Required: true, Index: true},
        "sensor_type": {Type: "string", Required: true, Index: true},
        "value":       {Type: "float", Required: true},
        "unit":        {Type: "string", Required: true},
        "location":    {Type: "json", Required: false},
        "quality":     {Type: "float", Required: false},
    },
}
```
```

## 🎯 Use Cases & Examples

### User Activity Tracking

```go
// Register user activity schema
client.RegisterSchema(userSchema)

// Track user login
err := client.Data("user.activity").
    WithUser("user123").
    WithField("action", "login").
    WithField("resource", "web_app").
    WithTimestamp(time.Now()).
    Stream()

// Track user logout
err = client.Data("user.activity").
    WithUser("user123").
    WithField("action", "logout").
    WithField("resource", "web_app").
    WithTimestamp(time.Now()).
    Stream()
```

### Real-time Analytics

```go
// Page view tracking
err := client.Data("analytics.pageview").
    WithUser("user123").
    WithField("session_id", "sess456").
    WithField("page_url", "/dashboard").
    WithField("referrer", "https://google.com").
    WithField("duration_ms", 2500).
    WithField("user_agent", "Mozilla/5.0...").
    Stream()

// E-commerce events
err = client.Data("analytics.purchase").
    WithUser("user123").
    WithField("order_id", "order789").
    WithField("total_amount", 99.99).
    WithField("currency", "USD").
    WithField("items", []map[string]interface{}{
        {"product_id": "prod1", "quantity": 2, "price": 49.99},
    }).
    Stream()
```
```

### IoT Data Streaming

```go
// Sensor data collection
err := client.Data("iot.sensor.reading").
    WithField("device_id", "sensor001").
    WithField("sensor_type", "temperature").
    WithField("value", 23.5).
    WithField("unit", "celsius").
    WithField("location", map[string]interface{}{
        "building": "A",
        "floor": 2,
        "room": "201",
    }).
    WithField("quality", 0.95).
    Stream()

// Batch sensor readings using EventBuilder
events := client.NewEventBuilder("iot.sensor.reading")
for i := 0; i < 100; i++ {
    events.Add().
        WithField("device_id", fmt.Sprintf("sensor%03d", i)).
        WithField("sensor_type", "humidity").
        WithField("value", 45.0 + float64(i)*0.1).
        WithField("unit", "percent")
}
err = events.StreamAll()
```

## 🔧 Configuration

### Consumer Configuration

```go
// Create consumer with custom configuration
consumer, err := client.NewConsumer(streamhouse.ConsumerConfig{
    GroupName:     "my-consumer-group",
    ConsumerName:  "consumer-1",
    Workers:       5,
    ClaimInterval: 30 * time.Second,
    DeadLetterTTL: 24 * time.Hour,
})
if err != nil {
    log.Fatal(err)
}

// Start consumer
ctx := context.Background()
err = consumer.Start(ctx)
if err != nil {
    log.Fatal(err)
}
```
```

### Schema Validation

```go
schema := &streamhouse.DataSchema{
    Name: "user.profile.updated",
    Fields: map[string]streamhouse.FieldConfig{
        "user_id": {
            Type:     "string",
            Required: true,
            Index:    true,
        },
        "email": {
            Type:     "string",
            Required: true,
        },
        "age": {
            Type:     "int",
            Required: false,
            Default:  0,
        },
    },
}
```

## 📊 Health Monitoring

### Health Checks

```go
// Check client health
health := client.Health()
if health != nil {
    log.Printf("Client health check failed: %v", health)
}

// Check connection status
if !client.IsConnected() {
    log.Printf("Client is not connected")
}
```

## 🔄 Data Flow

1. **Schema Registration**: Define data structure and validation rules
2. **Event Creation**: Use builder pattern to construct events
3. **Validation**: Automatic validation against registered schema
4. **Redis Streaming**: Events queued in Redis Streams for reliability
5. **Background Processing**: Consumer workers process events in batches
6. **ClickHouse Storage**: Validated data inserted into ClickHouse tables
7. **Analytics**: Query ClickHouse for real-time analytics and reporting
## 🛠️ Field Types

| Type | ClickHouse Type | Description | Example |
|------|----------------|-------------|---------|
| `string` | String | Text data | `"user@example.com"` |
| `int` | Int64 | Integer numbers | `42` |
| `float` | Float64 | Decimal numbers | `3.14159` |
| `bool` | Bool | Boolean values | `true` |
| `datetime` | DateTime | Timestamps | `time.Now()` |
| `json` | String | JSON objects | `{"key": "value"}` |

## 🔧 Configuration Options

### Redis Configuration

```go
RedisConfig{
    Addr:         "localhost:6379",  // Redis server address
    Password:     "",                // Redis password
    DB:           0,                 // Redis database number
    PoolSize:     10,                // Connection pool size
    MinIdleConns: 5,                 // Minimum idle connections
    MaxRetries:   3,                 // Maximum retry attempts
    DialTimeout:  5 * time.Second,   // Connection timeout
    ReadTimeout:  3 * time.Second,   // Read timeout
    WriteTimeout: 3 * time.Second,   // Write timeout
}
```

### ClickHouse Configuration

```go
ClickHouseConfig{
    Host:     "localhost",           // ClickHouse host
    Port:     9000,                  // ClickHouse port
    Database: "analytics",           // Database name
    Username: "default",             // Username
    Password: "",                    // Password
}
```
```

## 🧪 Testing

```go
// Create test client
client := streamhouse.NewTestClient()

// Register test schema
schema := &streamhouse.DataSchema{
    Name: "test.event",
    Fields: map[string]streamhouse.FieldConfig{
        "id": {Type: "string", Required: true},
    },
}
err := client.RegisterSchema(schema)
if err != nil {
    t.Fatal(err)
}

// Stream test data
err = client.Data("test.event").
    WithField("id", "test123").
    Stream()
if err != nil {
    t.Fatal(err)
}
```
```

## 🚀 Production Deployment

### Docker Compose Example

```yaml
version: '3.8'
services:
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data

  clickhouse:
    image: clickhouse/clickhouse-server:latest
    ports:
      - "9000:9000"
      - "8123:8123"
    volumes:
      - clickhouse_data:/var/lib/clickhouse
    environment:
      CLICKHOUSE_DB: analytics
      CLICKHOUSE_USER: default
      CLICKHOUSE_PASSWORD: ""

  streamhouse-app:
    build: .
    depends_on:
      - redis
      - clickhouse
    environment:
      REDIS_ADDR: redis:6379
      CLICKHOUSE_HOST: clickhouse
      CLICKHOUSE_PORT: 9000

volumes:
  redis_data:
  clickhouse_data:
```

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: streamhouse-consumer
spec:
  replicas: 3
  selector:
    matchLabels:
      app: streamhouse-consumer
  template:
    metadata:
      labels:
        app: streamhouse-consumer
    spec:
      containers:
      - name: consumer
        image: your-app:latest
        env:
        - name: REDIS_ADDR
          value: "redis-service:6379"
        - name: CLICKHOUSE_HOST
          value: "clickhouse-service"
        - name: CONSUMER_WORKERS
          value: "5"
        resources:
          requests:
            memory: "256Mi"
            cpu: "250m"
          limits:
            memory: "512Mi"
            cpu: "500m"
```

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- [Redis Streams](https://redis.io/topics/streams-intro) for reliable message queuing
- [ClickHouse](https://clickhouse.com/) for high-performance analytics
- [Prisma](https://www.prisma.io/) for schema design inspiration

## 📚 Additional Resources

- [Getting Started Guide](docs/getting-started.md)
- [Schema Reference](docs/schema-reference.md)
- [Configuration Guide](docs/configuration.md)
- [Best Practices](docs/best-practices.md)
- [API Reference](https://pkg.go.dev/github.com/parnexcodes/streamhouse)

---

**StreamHouse** - Stream your data with confidence 🚀