# StreamHouse Audit Logging Example

This example demonstrates how to use StreamHouse for comprehensive audit logging in your applications. It shows how to track user actions, data access, and system configuration changes with real-time streaming to Redis and persistent storage in ClickHouse.

## Features Demonstrated

- **User Action Auditing**: Login/logout events, profile updates, user management
- **Data Access Auditing**: Database queries, API calls, data exports
- **System Configuration Auditing**: Configuration changes, admin actions
- **Real-time Streaming**: Events are streamed to Redis Streams for immediate processing
- **Persistent Storage**: All audit data is stored in ClickHouse for long-term analysis
- **Flexible Schema System**: Easy to extend with custom audit event types

## Prerequisites

Before running this example, ensure you have the following installed and running:

### 1. Redis Server
```bash
# Using Docker
docker run -d --name redis -p 6379:6379 redis:latest

# Or install locally (Ubuntu/Debian)
sudo apt-get install redis-server
sudo systemctl start redis-server

# Or install locally (macOS)
brew install redis
brew services start redis
```

### 2. ClickHouse Server
```bash
# Using Docker
docker run -d --name clickhouse -p 9000:9000 -p 8123:8123 clickhouse/clickhouse-server:latest

# Or install locally (Ubuntu/Debian)
sudo apt-get install clickhouse-server clickhouse-client
sudo systemctl start clickhouse-server

# Or install locally (macOS)
brew install clickhouse
brew services start clickhouse
```

### 3. Go Environment
Make sure you have Go 1.19 or later installed:
```bash
go version
```

## Setup Instructions

### 1. Clone and Navigate to Example
```bash
cd examples/audit-logging
```

### 2. Initialize Go Module (if needed)
```bash
go mod init audit-logging-example
go mod tidy
```

### 3. Configure Services

#### Redis Configuration
The example uses default Redis settings (localhost:6379). If your Redis instance uses different settings, update `config.yaml`:

```yaml
redis:
  host: "your-redis-host"
  port: 6379
  password: "your-password"  # if authentication is enabled
  db: 0
```

#### ClickHouse Configuration
Create the audit database in ClickHouse:

```sql
-- Connect to ClickHouse
clickhouse-client

-- Create database
CREATE DATABASE IF NOT EXISTS audit_logs;

-- Verify database creation
SHOW DATABASES;
```

Update `config.yaml` if using different ClickHouse settings:

```yaml
clickhouse:
  host: "your-clickhouse-host"
  port: 9000
  database: "audit_logs"
  username: "your-username"
  password: "your-password"
```

### 4. Run the Example
```bash
go run main.go
```

## What the Example Does

When you run the example, it will:

1. **Register Audit Schemas**: Three different audit schemas are registered:
   - `audit.user.action`: For user management and authentication events
   - `audit.data.access`: For data access and query tracking
   - `audit.system.config`: For system configuration changes

2. **Start Background Consumer**: A consumer process starts to read from Redis Streams and batch insert data into ClickHouse

3. **Simulate Audit Events**: The example generates various audit events:
   - User login (successful)
   - Data access (database query)
   - Failed login attempt
   - System configuration change
   - Bulk data export
   - User profile update

4. **Process Events**: Events are streamed to Redis and then processed in batches to ClickHouse

## Verifying the Results

### Check Redis Streams
```bash
# Connect to Redis CLI
redis-cli

# List all streams
XINFO GROUPS audit_events

# Read recent events from the stream
XREAD COUNT 10 STREAMS audit_events $
```

### Check ClickHouse Tables
```sql
-- Connect to ClickHouse
clickhouse-client

-- Use the audit database
USE audit_logs;

-- Show created tables
SHOW TABLES;

-- Query user action audits
SELECT * FROM audit_user_action ORDER BY timestamp DESC LIMIT 10;

-- Query data access audits
SELECT * FROM audit_data_access ORDER BY timestamp DESC LIMIT 10;

-- Query system configuration audits
SELECT * FROM audit_system_config ORDER BY timestamp DESC LIMIT 10;

-- Example analytics queries
-- Count login attempts by success status
SELECT success, COUNT(*) as attempts 
FROM audit_user_action 
WHERE action = 'login' 
GROUP BY success;

-- Top users by data access volume
SELECT user_id, SUM(record_count) as total_records_accessed
FROM audit_data_access 
WHERE record_count > 0
GROUP BY user_id 
ORDER BY total_records_accessed DESC;

-- Recent configuration changes
SELECT admin_user_id, config_section, config_key, old_value, new_value, timestamp
FROM audit_system_config 
ORDER BY timestamp DESC 
LIMIT 5;
```

## Customizing for Your Application

### Adding New Audit Event Types

1. **Define a New Schema**:
```go
customSchema := &streamhouse.DataSchema{
    Name: "audit.custom.event",
    Fields: map[string]streamhouse.FieldConfig{
        "custom_field": {
            Type:        streamhouse.FieldTypeString,
            Required:    true,
            Index:       true,
            Description: "Your custom field",
        },
        // Add more fields as needed
    },
    IndexFields: []string{"custom_field"},
}
```

2. **Register the Schema**:
```go
if err := client.RegisterSchema(customSchema); err != nil {
    log.Fatalf("Failed to register custom schema: %v", err)
}
```

3. **Stream Events**:
```go
client.Data("audit.custom.event").
    WithField("custom_field", "custom_value").
    StreamAsync()
```

### Integration with Your Application

To integrate this audit logging into your application:

1. **Import StreamHouse**:
```go
import "github.com/streamhouse/streamhouse"
```

2. **Initialize Client** (typically in your main function or service initialization):
```go
config, _ := streamhouse.LoadConfig("config.yaml")
auditClient, _ := streamhouse.NewClient(config)
defer auditClient.Close()

// Register your audit schemas
registerAuditSchemas(auditClient)

// Start consumer
go auditClient.StartConsumer(context.Background())
```

3. **Add Audit Calls** throughout your application:
```go
// In your authentication handler
func loginHandler(w http.ResponseWriter, r *http.Request) {
    // ... authentication logic ...
    
    // Audit the login attempt
    auditClient.Data("audit.user.action").
        WithUser(userID).
        WithIP(getClientIP(r)).
        WithField("action", "login").
        WithField("resource", "user").
        WithField("success", loginSuccessful).
        WithField("user_agent", r.UserAgent()).
        StreamAsync()
}

// In your data access layer
func queryDatabase(userID, query string) {
    // ... database query logic ...
    
    // Audit the data access
    auditClient.Data("audit.data.access").
        WithUser(userID).
        WithField("resource_type", "database").
        WithField("operation", "read").
        WithField("query", query).
        WithField("success", querySuccessful).
        WithField("record_count", recordCount).
        StreamAsync()
}
```

## Configuration Options

The `config.yaml` file supports extensive configuration options:

- **Batch Processing**: Adjust `batch_size` and `flush_interval` for performance tuning
- **Worker Scaling**: Increase `workers` for higher throughput
- **Retry Logic**: Configure retry attempts and delays for reliability
- **Monitoring**: Enable metrics and health checks
- **Security**: Configure TLS for production environments

## Performance Considerations

- **Async Streaming**: Use `StreamAsync()` for non-blocking audit logging
- **Batch Processing**: Events are processed in batches for optimal ClickHouse performance
- **Indexing**: Configure appropriate indexes in your schemas for query performance
- **TTL**: Set TTL on schemas for automatic data cleanup if needed

## Troubleshooting

### Common Issues

1. **Connection Errors**:
   - Verify Redis and ClickHouse are running
   - Check network connectivity and firewall settings
   - Validate configuration settings

2. **Schema Registration Failures**:
   - Ensure schema names are unique
   - Verify field types are valid
   - Check for required field violations

3. **Performance Issues**:
   - Increase batch size for higher throughput
   - Add more workers for parallel processing
   - Optimize ClickHouse table schemas and indexes

### Logs and Monitoring

The example includes comprehensive logging. Check the console output for:
- Schema registration confirmations
- Event processing status
- Error messages and warnings
- Performance metrics

## Next Steps

- Explore other StreamHouse examples for different use cases
- Read the [StreamHouse documentation](../../docs/) for advanced features
- Consider implementing custom aggregations and real-time analytics
- Set up monitoring and alerting for production deployments

## Support

For questions and support:
- Check the [StreamHouse documentation](../../docs/)
- Review the [configuration guide](../../docs/configuration.md)
- See [best practices](../../docs/best-practices.md) for production deployments