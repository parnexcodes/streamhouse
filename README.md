# streamhouse

[![Go Version](https://img.shields.io/badge/go-%3E%3D1.21-blue.svg)](https://golang.org/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/parnexcodes/streamhouse)

A powerful Go package for schema-driven data streaming and analytics with Redis Streams and ClickHouse backend. StreamHouse provides a flexible schema system for reliable data streaming, real-time analytics, event processing, and high-performance data ingestion.

## Features

- **Schema-Driven Architecture**: Flexible schema system with field validation and type safety
- **Dual Storage Backend**: Redis Streams for reliable queuing + ClickHouse for analytics
- **Data Builder API**: Builder pattern for constructing events and batches
- **Background Processing**: Efficient batch processing with configurable workers
- **Auto Table Management**: Automatic ClickHouse table creation from schemas
- **Consumer Groups**: Horizontal scaling with Redis Streams consumer groups
- **Error Handling**: Comprehensive error handling with configurable retry logic
- **Health Monitoring**: Built-in health checks and connection monitoring
- **Production Ready**: Connection pooling, graceful shutdown, and resource management

## Installation

```bash
go get github.com/parnexcodes/streamhouse
```

## Architecture

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

## Quick Start

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
            Host:     "localhost",
            Port:     6379,
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
            "user_id": {Type: "string", Required: true, Index: true},
            "email":   {Type: "string", Required: true},
            "name":    {Type: "string", Required: true},
        },
    }
    
    if err := client.RegisterSchema(schema); err != nil {
        log.Fatal(err)
    }
    
    // Start background consumer
    ctx := context.Background()
    go client.StartConsumer(ctx)
    
    // Stream data with error handling
    err = client.Stream("user.signup", map[string]interface{}{
        "user_id": "user123",
        "email":   "user@example.com",
        "name":    "John Doe",
    })
    if err != nil {
        log.Printf("Failed to stream data: %v", err)
    }
    
    // Or use async streaming (fire-and-forget)
    client.StreamAsync("user.signup", map[string]interface{}{
        "user_id": "user456",
        "email":   "jane@example.com", 
        "name":    "Jane Smith",
    })
}
```

## Use Cases

- **Audit Logging**: Track user activities and system events
- **Real-time Analytics**: Page views, user interactions, business metrics
- **IoT Data Streaming**: Sensor readings, device telemetry
- **E-commerce Events**: Orders, payments, inventory changes
- **Application Monitoring**: Performance metrics, error tracking

## Documentation

- **[Getting Started](docs/getting-started.md)** - Installation, setup, and basic usage
- **[Schema Reference](docs/schema-reference.md)** - Complete schema system documentation
- **[Configuration Guide](docs/configuration.md)** - All configuration options and examples
- **[Best Practices](docs/best-practices.md)** - Production deployment and optimization tips

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
## Acknowledgments

- [Redis Streams](https://redis.io/topics/streams-intro) for reliable message queuing
- [ClickHouse](https://clickhouse.com/) for high-performance analytics
- [Prisma](https://www.prisma.io/) for schema design inspiration
---
