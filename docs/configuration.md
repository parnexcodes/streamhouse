# Configuration Guide

StreamHouse provides flexible configuration options through code, configuration files, and environment variables. This guide covers all available configuration options and best practices.

## Configuration Methods

### 1. Programmatic Configuration

```go
config := streamhouse.Config{
    Redis: streamhouse.RedisConfig{
        Host:         "localhost",
        Port:         6379,
        Password:     "",
        DB:           0,
        PoolSize:     10,
        MinIdleConns: 5,
        MaxRetries:   3,
        DialTimeout:  5 * time.Second,
        ReadTimeout:  3 * time.Second,
        WriteTimeout: 3 * time.Second,
    },
    ClickHouse: streamhouse.ClickHouseConfig{
        Host:     "localhost",
        Port:     9000,
        Database: "analytics",
        Username: "default",
        Password: "",
        Settings: map[string]interface{}{
            "max_execution_time": 60,
            "max_memory_usage":   10000000000,
        },
    },
    StreamName:    "events",
    BatchSize:     100,
    FlushInterval: 5 * time.Second,
    Workers:       4,
}
```

### 2. Configuration File

Create a `config.yaml` file:

```yaml
redis:
  host: "localhost"
  port: 6379
  password: ""
  db: 0
  pool_size: 10
  min_idle_conns: 5
  max_retries: 3
  dial_timeout: "5s"
  read_timeout: "3s"
  write_timeout: "3s"
  tls:
    enabled: false
    cert_file: ""
    key_file: ""
    ca_file: ""
    insecure_skip_verify: false

clickhouse:
  host: "localhost"
  port: 9000
  database: "analytics"
  username: "default"
  password: ""
  settings:
    max_execution_time: 60
    max_memory_usage: 10000000000
  tls:
    enabled: false
    cert_file: ""
    key_file: ""
    ca_file: ""
    insecure_skip_verify: false

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
```

### 3. Environment Variables

Set environment variables to override configuration:

```bash
# Redis Configuration - supports host:port format or separate host/port
export STREAMHOUSE_REDIS_HOST="localhost:6379"     # Can include port
export STREAMHOUSE_REDIS_PORT="6379"               # Optional if port in host
export STREAMHOUSE_REDIS_PASSWORD=""

# ClickHouse Configuration - supports host:port format or separate host/port
export STREAMHOUSE_CLICKHOUSE_HOST="localhost:9000"  # Can include port
export STREAMHOUSE_CLICKHOUSE_PORT="9000"            # Optional if port in host
export STREAMHOUSE_CLICKHOUSE_DATABASE="analytics"
export STREAMHOUSE_CLICKHOUSE_USERNAME="default"
export STREAMHOUSE_CLICKHOUSE_PASSWORD=""

# StreamHouse Configuration
export STREAMHOUSE_STREAM_NAME="events"
```


**Note**: The `STREAMHOUSE_REDIS_HOST` and `STREAMHOUSE_CLICKHOUSE_HOST` environment variables support both formats:
- `host:port` format (e.g., `localhost:6379`) - port will be automatically parsed
- `host` only format (e.g., `localhost`) - uses default or separately configured port

If both host:port format and separate port variables are provided, the separate port variable takes precedence.

## Configuration Reference

### Redis Configuration

```go
type RedisConfig struct {
    Host         string        // Redis server host
    Port         int           // Redis server port
    Password     string        // Redis password
    DB           int           // Redis database number
    PoolSize     int           // Connection pool size
    MinIdleConns int           // Minimum idle connections
    MaxRetries   int           // Maximum retry attempts
    DialTimeout  time.Duration // Connection timeout
    ReadTimeout  time.Duration // Read timeout
    WriteTimeout time.Duration // Write timeout
    TLS          TLSConfig     // TLS configuration
}
```

#### Redis Options

| Option | Default | Description |
|--------|---------|-------------|
| `Host` | `localhost` | Redis server host |
| `Port` | `6379` | Redis server port |
| `Password` | `""` | Redis password (empty for no auth) |
| `DB` | `0` | Redis database number |
| `PoolSize` | `10` | Maximum number of connections |
| `MinIdleConns` | `5` | Minimum idle connections to maintain |
| `MaxRetries` | `3` | Maximum retry attempts for failed operations |
| `DialTimeout` | `5s` | Timeout for establishing connections |
| `ReadTimeout` | `3s` | Timeout for read operations |
| `WriteTimeout` | `3s` | Timeout for write operations |

### ClickHouse Configuration

```go
type ClickHouseConfig struct {
    Host     string                 // ClickHouse host
    Port     int                    // ClickHouse port
    Database string                 // Database name
    Username string                 // Username
    Password string                 // Password
    Settings map[string]interface{} // ClickHouse settings
    TLS      TLSConfig             // TLS configuration
}
```

#### ClickHouse Options

| Option | Default | Description |
|--------|---------|-------------|
| `Host` | `localhost` | ClickHouse server hostname |
| `Port` | `9000` | ClickHouse native protocol port |
| `Database` | `default` | Database name |
| `Username` | `default` | Username for authentication |
| `Password` | `""` | Password for authentication |
| `Settings` | `{}` | ClickHouse-specific settings |

#### Common ClickHouse Settings

```go
Settings: map[string]interface{}{
    "max_execution_time":     60,           // Query timeout in seconds
    "max_memory_usage":       10000000000,  // Max memory per query (10GB)
    "max_threads":            4,            // Max threads per query
    "max_insert_block_size":  1048576,      // Max rows per insert block
    "insert_quorum":          1,            // Insert quorum for replication
    "select_sequential_consistency": 1,     // Sequential consistency for reads
}
```

### Consumer Configuration

```go
type ConsumerConfig struct {
    GroupName        string        // Consumer group name
    ConsumerName     string        // Individual consumer name
    DeadLetterQueue  bool          // Enable dead letter queue
    DeadLetterTTL    time.Duration // TTL for dead letter messages
    ClaimInterval    time.Duration // Interval for claiming stale messages
}
```

#### Consumer Options

| Option | Default | Description |
|--------|---------|-------------|
| `GroupName` | `streamhouse-consumers` | Redis Streams consumer group name |
| `ConsumerName` | `consumer-1` | Individual consumer identifier |
| `DeadLetterQueue` | `true` | Enable dead letter queue for failed messages |
| `DeadLetterTTL` | `24h` | TTL for messages in dead letter queue |
| `ClaimInterval` | `30s` | Interval for claiming stale messages |

### Monitoring Configuration

```go
type MonitoringConfig struct {
    Enabled         bool          // Enable monitoring
    MetricsInterval time.Duration // Metrics collection interval
    HealthCheck     bool          // Enable health checks
    HealthInterval  time.Duration // Health check interval
}
```

#### Monitoring Options

| Option | Default | Description |
|--------|---------|-------------|
| `Enabled` | `true` | Enable monitoring and metrics collection |
| `MetricsInterval` | `30s` | How often to collect metrics |
| `HealthCheck` | `true` | Enable periodic health checks |
| `HealthInterval` | `10s` | How often to perform health checks |

### Logging Configuration

```go
type LoggingConfig struct {
    Level  string // Log level (debug, info, warn, error)
    Format string // Log format (json, text)
    Output string // Log output (stdout, stderr, file path)
}
```

#### Logging Options

| Option | Default | Description |
|--------|---------|-------------|
| `Level` | `info` | Minimum log level to output |
| `Format` | `json` | Log format (json or text) |
| `Output` | `stdout` | Where to write logs |

### TLS Configuration

```go
type TLSConfig struct {
    Enabled            bool   // Enable TLS
    CertFile           string // Certificate file path
    KeyFile            string // Private key file path
    CAFile             string // CA certificate file path
    InsecureSkipVerify bool   // Skip certificate verification
}
```

## Environment-Specific Configurations

### Development

```yaml
# config.dev.yaml
redis:
  addr: "localhost:6379"
  db: 0

clickhouse:
  host: "localhost"
  port: 9000
  database: "analytics_dev"

batch_size: 10
flush_interval: "1s"
workers: 2

logging:
  level: "debug"
  format: "text"
```

### Production

```yaml
# config.prod.yaml
redis:
  host: "redis-cluster"
  port: 6379
  password: "${REDIS_PASSWORD}"
  pool_size: 50
  min_idle_conns: 10
  tls:
    enabled: true
    cert_file: "/etc/ssl/redis-client.crt"
    key_file: "/etc/ssl/redis-client.key"
    ca_file: "/etc/ssl/ca.crt"

clickhouse:
  host: "clickhouse-cluster"
  port: 9440
  database: "analytics_prod"
  username: "${CLICKHOUSE_USER}"
  password: "${CLICKHOUSE_PASSWORD}"
  tls:
    enabled: true
    cert_file: "/etc/ssl/clickhouse-client.crt"
    key_file: "/etc/ssl/clickhouse-client.key"
    ca_file: "/etc/ssl/ca.crt"
  settings:
    max_execution_time: 300
    max_memory_usage: 50000000000

batch_size: 1000
flush_interval: "10s"
workers: 10

consumer:
  dead_letter_ttl: "7d"
  claim_interval: "60s"

monitoring:
  enabled: true
  metrics_interval: "15s"

logging:
  level: "info"
  format: "json"
  output: "/var/log/streamhouse/app.log"
```

## Configuration Validation

StreamHouse validates configuration on startup:

```go
config := streamhouse.Config{
    BatchSize: -1, // Invalid: must be positive
    Workers:   0,  // Invalid: must be positive
}

client, err := streamhouse.NewClient(config)
if err != nil {
    // Handle configuration validation error
    if configErr, ok := err.(*streamhouse.ConfigError); ok {
        log.Printf("Configuration error: %v", configErr)
    }
}
```

## Performance Tuning

### High Throughput

For high-throughput scenarios:

```yaml
batch_size: 5000
flush_interval: "30s"
workers: 20

redis:
  pool_size: 100
  min_idle_conns: 20

clickhouse:
  settings:
    max_insert_block_size: 1048576
    max_threads: 8
```

### Low Latency

For low-latency scenarios:

```yaml
batch_size: 50
flush_interval: "100ms"
workers: 5

redis:
  pool_size: 20
  read_timeout: "1s"
  write_timeout: "1s"

clickhouse:
  settings:
    max_execution_time: 10
    max_threads: 2
```

### Memory Optimization

For memory-constrained environments:

```yaml
batch_size: 100
flush_interval: "5s"
workers: 2

redis:
  pool_size: 5
  min_idle_conns: 2

clickhouse:
  settings:
    max_memory_usage: 1000000000  # 1GB
    max_threads: 2
```

## Security Best Practices

### 1. Use TLS

Always enable TLS in production:

```yaml
redis:
  tls:
    enabled: true
    cert_file: "/path/to/client.crt"
    key_file: "/path/to/client.key"
    ca_file: "/path/to/ca.crt"

clickhouse:
  tls:
    enabled: true
    cert_file: "/path/to/client.crt"
    key_file: "/path/to/client.key"
    ca_file: "/path/to/ca.crt"
```

### 2. Use Environment Variables for Secrets

Never hardcode passwords in configuration files:

```yaml
redis:
  password: "${REDIS_PASSWORD}"

clickhouse:
  password: "${CLICKHOUSE_PASSWORD}"
```

### 3. Restrict Network Access

Configure firewalls and network policies to restrict access to Redis and ClickHouse.

### 4. Use Strong Authentication

Configure strong passwords and consider using certificate-based authentication.

## Troubleshooting

### Common Configuration Issues

1. **Connection Timeouts**: Increase timeout values for slow networks
2. **Memory Issues**: Reduce batch size or increase flush interval
3. **High CPU Usage**: Reduce worker count or batch size
4. **Slow Queries**: Tune ClickHouse settings or optimize schemas

### Debug Configuration

Enable debug logging to troubleshoot configuration issues:

```yaml
logging:
  level: "debug"
  format: "text"
```

### Health Checks

Use health checks to monitor configuration effectiveness:

```go
err := client.Health()
if err != nil {
    log.Printf("Health check failed: %v", err)
}

metrics := client.GetMetrics()
log.Printf("Average latency: %.2fms", metrics.AverageLatency)
```