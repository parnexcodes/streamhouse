# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial StreamHouse implementation
- Redis Streams integration for event streaming
- ClickHouse integration for data storage
- Schema validation system
- Consumer group management
- Configurable batching and workers
- Health check endpoints
- Comprehensive error handling
- TLS support for secure connections
- Metrics collection and monitoring
- Data builder pattern for easy event construction
- Event and batch builder utilities

### Features
- **Schema Management**: Dynamic schema registration and validation
- **Data Streaming**: High-performance event streaming with Redis Streams
- **Storage**: Efficient data storage in ClickHouse with automatic table creation
- **Consumer Groups**: Scalable consumer group processing with configurable workers
- **Monitoring**: Built-in health checks and metrics collection
- **Security**: TLS encryption and authentication support
- **Configuration**: Flexible configuration via code, files, or environment variables

### Configuration Options
- Configurable batch sizes and flush intervals
- Multiple worker support for parallel processing
- Dead letter TTL and claim interval settings
- Redis connection pooling
- ClickHouse connection settings
- Logging configuration
- TLS/SSL configuration

### Documentation
- Getting started guide
- Schema reference documentation
- Configuration guide
- Best practices guide
- API documentation

## [0.1.0] - 2024-01-XX

### Added
- Initial project structure
- Core client implementation
- Basic Redis and ClickHouse integration
- Schema validation framework
- Consumer implementation
- Configuration management
- Error handling system

### Technical Details
- Go 1.21+ support
- Redis 6.0+ compatibility
- ClickHouse 21.0+ compatibility
- Structured logging with configurable levels
- Graceful shutdown handling
- Connection pooling and retry logic

### Known Limitations
- Single consumer group per client instance
- Limited schema evolution support
- Basic monitoring metrics only

## Future Releases

### Planned Features
- [ ] Schema evolution and migration tools
- [ ] Advanced monitoring and alerting
- [ ] Distributed tracing support
- [ ] Performance optimizations
- [ ] Additional storage backends
- [ ] Enhanced security features
- [ ] Kubernetes operator
- [ ] Web UI for monitoring and management

### Performance Improvements
- [ ] Optimized batch processing
- [ ] Connection pooling enhancements
- [ ] Memory usage optimizations
- [ ] Compression support
- [ ] Async processing improvements

### Developer Experience
- [ ] CLI tools for management
- [ ] Docker images
- [ ] Helm charts
- [ ] Integration examples
- [ ] Performance benchmarks