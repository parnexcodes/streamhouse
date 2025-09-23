# Best Practices

This guide covers best practices for using StreamHouse in production environments, including performance optimization, security, monitoring, and operational considerations.

## Schema Design

### 1. Naming Conventions

Use hierarchical, descriptive names for schemas:

```go
// Good
"user.activity.login"
"ecommerce.order.created"
"iot.sensor.temperature"

// Avoid
"event1"
"data"
"stuff"
```

### 2. Field Design

**Use appropriate data types:**
```go
// Good
"user_id":    {Type: "string", Required: true, Index: true}
"timestamp":  {Type: "datetime", Required: true, Index: true}
"amount":     {Type: "float", Required: true}
"is_active":  {Type: "bool", Required: false, Default: true}

// Avoid storing everything as strings
"amount":     {Type: "string"} // Should be float
"timestamp":  {Type: "string"} // Should be datetime
```

**Index strategically:**
```go
// Index fields you'll query frequently
"user_id":    {Index: true}  // Frequently filtered
"timestamp":  {Index: true}  // Used in time-range queries
"metadata":   {Index: false} // Rarely queried directly
```

### 3. Schema Evolution

Design schemas for evolution:

```go
// Version 1
userSchema := &streamhouse.DataSchema{
    Name: "user.profile.v1",
    Fields: map[string]streamhouse.FieldConfig{
        "user_id": {Type: "string", Required: true},
        "name":    {Type: "string", Required: true},
    },
}

// Version 2 - Add optional fields only
userSchemaV2 := &streamhouse.DataSchema{
    Name: "user.profile.v2",
    Fields: map[string]streamhouse.FieldConfig{
        "user_id": {Type: "string", Required: true},
        "name":    {Type: "string", Required: true},
        "email":   {Type: "string", Required: false}, // New optional field
        "phone":   {Type: "string", Required: false}, // New optional field
    },
}
```

## Performance Optimization

### 1. Batch Configuration

**High-throughput scenarios:**
```go
config := streamhouse.Config{
    BatchSize:     5000,
    FlushInterval: 30 * time.Second,
    Workers:       20,
}
```

**Low-latency scenarios:**
```go
config := streamhouse.Config{
    BatchSize:     50,
    FlushInterval: 100 * time.Millisecond,
    Workers:       5,
}
```

### 2. Connection Pooling

Configure appropriate pool sizes:

```go
redis: streamhouse.RedisConfig{
    PoolSize:     50,  // High throughput
    MinIdleConns: 10,  // Keep connections warm
}

// For low-traffic applications
redis: streamhouse.RedisConfig{
    PoolSize:     10,
    MinIdleConns: 2,
}
```

### 3. ClickHouse Optimization

**Table partitioning:**
```go
schema := &streamhouse.DataSchema{
    Name: "high.volume.events",
    Partitions: []string{
        "toYYYYMM(timestamp)",     // Monthly partitions
        "intHash64(user_id) % 10", // Hash partitioning
    },
}
```

**Optimize settings:**
```go
clickhouse: streamhouse.ClickHouseConfig{
    Settings: map[string]interface{}{
        "max_insert_block_size": 1048576,  // 1M rows per block
        "max_threads":           8,        // Parallel processing
        "max_memory_usage":      10000000000, // 10GB limit
    },
}
```

## Error Handling

### 1. Graceful Degradation

Handle errors gracefully without blocking your application:

```go
// Use async streaming for non-critical events
client.Data("analytics.pageview").
    WithUser(userID).
    WithField("page", "/dashboard").
    StreamAsync() // Won't block on errors

// Use sync streaming for critical events
err := client.Data("audit.payment.processed").
    WithUser(userID).
    WithField("amount", 100.00).
    Stream()
if err != nil {
    // Handle critical error - maybe retry or alert
    log.Printf("Critical event failed: %v", err)
}
```

### 2. Error Classification

Handle different error types appropriately:

```go
err := client.Stream("user.activity", data)
if err != nil {
    switch e := err.(type) {
    case *streamhouse.ValidationError:
        // Data validation failed - fix the data
        log.Printf("Invalid data: %v", e)
        
    case *streamhouse.ConnectionError:
        // Connection issue - might be temporary
        log.Printf("Connection error: %v", e)
        // Consider retrying or queuing for later
        
    case *streamhouse.SchemaError:
        // Schema not found or invalid - code issue
        log.Printf("Schema error: %v", e)
        // This needs immediate attention
        
    default:
        log.Printf("Unknown error: %v", e)
    }
}
```

### 3. Circuit Breaker Pattern

Implement circuit breakers for external dependencies:

```go
type CircuitBreaker struct {
    failures    int
    lastFailure time.Time
    threshold   int
    timeout     time.Duration
}

func (cb *CircuitBreaker) Call(fn func() error) error {
    if cb.failures >= cb.threshold && 
       time.Since(cb.lastFailure) < cb.timeout {
        return errors.New("circuit breaker open")
    }
    
    err := fn()
    if err != nil {
        cb.failures++
        cb.lastFailure = time.Now()
    } else {
        cb.failures = 0
    }
    
    return err
}
```

## Security

### 1. Network Security

**Use TLS everywhere:**
```go
config := streamhouse.Config{
    Redis: streamhouse.RedisConfig{
        TLS: streamhouse.TLSConfig{
            Enabled:  true,
            CertFile: "/etc/ssl/client.crt",
            KeyFile:  "/etc/ssl/client.key",
            CAFile:   "/etc/ssl/ca.crt",
        },
    },
    ClickHouse: streamhouse.ClickHouseConfig{
        TLS: streamhouse.TLSConfig{
            Enabled:  true,
            CertFile: "/etc/ssl/client.crt",
            KeyFile:  "/etc/ssl/client.key",
            CAFile:   "/etc/ssl/ca.crt",
        },
    },
}
```

**Network isolation:**
- Use private networks for Redis and ClickHouse
- Implement firewall rules
- Use VPNs or service meshes

### 2. Authentication & Authorization

**Strong passwords:**
```bash
# Generate strong passwords
export REDIS_PASSWORD=$(openssl rand -base64 32)
export CLICKHOUSE_PASSWORD=$(openssl rand -base64 32)
```

**Certificate-based authentication:**
```go
// Use client certificates for authentication
tls: streamhouse.TLSConfig{
    Enabled:  true,
    CertFile: "/etc/ssl/streamhouse-client.crt",
    KeyFile:  "/etc/ssl/streamhouse-client.key",
    CAFile:   "/etc/ssl/ca.crt",
}
```

### 3. Data Protection

**Encrypt sensitive data:**
```go
// Encrypt PII before streaming
encryptedEmail := encrypt(user.Email)
client.Data("user.activity").
    WithUser(user.ID).
    WithField("email_hash", encryptedEmail).
    StreamAsync()
```

**Data retention policies:**
```go
// Set appropriate TTL for different data types
auditSchema := &streamhouse.DataSchema{
    TTL: &[]time.Duration{7 * 365 * 24 * time.Hour}[0], // 7 years for audit
}

analyticsSchema := &streamhouse.DataSchema{
    TTL: &[]time.Duration{90 * 24 * time.Hour}[0], // 90 days for analytics
}
```

## Monitoring & Observability

### 1. Metrics Collection

Monitor key metrics:

```go
// Collect metrics regularly
go func() {
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()
    
    for range ticker.C {
        metrics := client.GetMetrics()
        
        // Send to your monitoring system
        prometheus.EventsProcessed.Set(float64(metrics.EventsProcessed))
        prometheus.EventsFailed.Set(float64(metrics.EventsFailed))
        prometheus.AverageLatency.Set(metrics.AverageLatency)
    }
}()
```

### 2. Health Checks

Implement comprehensive health checks:

```go
func healthCheck(client streamhouse.Client) error {
    // Check overall health
    if err := client.Health(); err != nil {
        return fmt.Errorf("client health check failed: %w", err)
    }
    
    // Check specific components
    metrics := client.GetMetrics()
    
    // Check error rate
    if metrics.EventsFailed > 0 {
        errorRate := float64(metrics.EventsFailed) / float64(metrics.EventsProcessed)
        if errorRate > 0.05 { // 5% error rate threshold
            return fmt.Errorf("high error rate: %.2f%%", errorRate*100)
        }
    }
    
    // Check latency
    if metrics.AverageLatency > 1000 { // 1 second threshold
        return fmt.Errorf("high latency: %.2fms", metrics.AverageLatency)
    }
    
    return nil
}
```

### 3. Alerting

Set up alerts for critical issues:

```go
// Example alert conditions
type AlertCondition struct {
    Name      string
    Condition func(*streamhouse.Metrics) bool
    Message   string
}

alerts := []AlertCondition{
    {
        Name: "High Error Rate",
        Condition: func(m *streamhouse.Metrics) bool {
            if m.EventsProcessed == 0 {
                return false
            }
            errorRate := float64(m.EventsFailed) / float64(m.EventsProcessed)
            return errorRate > 0.05
        },
        Message: "Error rate exceeded 5%",
    },
    {
        Name: "High Latency",
        Condition: func(m *streamhouse.Metrics) bool {
            return m.AverageLatency > 1000
        },
        Message: "Average latency exceeded 1 second",
    },
}
```

## Deployment

### 1. Container Configuration

**Dockerfile example:**
```dockerfile
FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o streamhouse-app

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /app/streamhouse-app .
COPY config.yaml .
CMD ["./streamhouse-app"]
```

**Docker Compose:**
```yaml
version: '3.8'
services:
  streamhouse-app:
    build: .
    environment:
      - REDIS_ADDR=redis:6379
      - CLICKHOUSE_HOST=clickhouse
    depends_on:
      - redis
      - clickhouse
    restart: unless-stopped
    
  redis:
    image: redis:7-alpine
    volumes:
      - redis_data:/data
    restart: unless-stopped
    
  clickhouse:
    image: clickhouse/clickhouse-server:latest
    volumes:
      - clickhouse_data:/var/lib/clickhouse
    restart: unless-stopped

volumes:
  redis_data:
  clickhouse_data:
```

### 2. Kubernetes Deployment

**Deployment manifest:**
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
        image: streamhouse-app:latest
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
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 5
```

### 3. Scaling Considerations

**Horizontal scaling:**
- Use multiple consumer instances
- Configure unique consumer names
- Monitor Redis Streams consumer group lag

**Vertical scaling:**
- Increase worker count for CPU-bound workloads
- Increase batch size for I/O-bound workloads
- Monitor memory usage and adjust limits

## Testing

### 1. Unit Testing

```go
func TestStreamHouseClient(t *testing.T) {
    // Use test client with mocks
    client := streamhouse.NewTestClient()
    
    schema := &streamhouse.DataSchema{
        Name: "test.event",
        Fields: map[string]streamhouse.FieldConfig{
            "id": {Type: "string", Required: true},
        },
    }
    
    err := client.RegisterSchema(schema)
    assert.NoError(t, err)
    
    err = client.Stream("test.event", map[string]interface{}{
        "id": "test123",
    })
    assert.NoError(t, err)
    
    // Verify data was processed
    events := client.GetProcessedEvents("test.event")
    assert.Len(t, events, 1)
    assert.Equal(t, "test123", events[0]["id"])
}
```

### 2. Integration Testing

```go
func TestIntegration(t *testing.T) {
    // Use testcontainers for real Redis/ClickHouse
    redisContainer := testcontainers.RunRedis(t)
    clickhouseContainer := testcontainers.RunClickHouse(t)
    
    config := streamhouse.Config{
        Redis: streamhouse.RedisConfig{
            Addr: redisContainer.Addr(),
        },
        ClickHouse: streamhouse.ClickHouseConfig{
            Host: clickhouseContainer.Host(),
            Port: clickhouseContainer.Port(),
        },
    }
    
    client, err := streamhouse.NewClient(config)
    require.NoError(t, err)
    defer client.Close()
    
    // Test real data flow
    // ...
}
```

### 3. Load Testing

```go
func BenchmarkStreaming(b *testing.B) {
    client := setupTestClient(b)
    
    b.ResetTimer()
    b.RunParallel(func(pb *testing.PB) {
        for pb.Next() {
            client.StreamAsync("test.event", map[string]interface{}{
                "id":        uuid.New().String(),
                "timestamp": time.Now(),
            })
        }
    })
}
```

## Troubleshooting

### 1. Common Issues

**High memory usage:**
- Reduce batch size
- Increase flush interval
- Check for memory leaks in custom validators

**Slow performance:**
- Increase worker count
- Optimize ClickHouse settings
- Check network latency

**Connection errors:**
- Verify network connectivity
- Check authentication credentials
- Review firewall rules

### 2. Debug Tools

**Enable debug logging:**
```go
config.Logging.Level = "debug"
```

**Monitor Redis Streams:**
```bash
redis-cli XINFO GROUPS events
redis-cli XPENDING events streamhouse-consumers
```

**Monitor ClickHouse:**
```sql
SELECT * FROM system.processes;
SELECT * FROM system.query_log ORDER BY event_time DESC LIMIT 10;
```

### 3. Performance Profiling

```go
import _ "net/http/pprof"

go func() {
    log.Println(http.ListenAndServe("localhost:6060", nil))
}()
```

Access profiling at `http://localhost:6060/debug/pprof/`

## Maintenance

### 1. Regular Tasks

- Monitor disk usage for Redis and ClickHouse
- Review and optimize slow queries
- Update dependencies regularly
- Backup configuration and schemas

### 2. Capacity Planning

- Monitor growth trends
- Plan for peak loads
- Scale infrastructure proactively
- Review retention policies

### 3. Disaster Recovery

- Implement backup strategies
- Test recovery procedures
- Document runbooks
- Plan for failover scenarios