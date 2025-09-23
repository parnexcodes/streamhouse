package streamhouse

import (
	"net"
	"time"
)

// DataBuilder implements the fluent API for building and streaming data
type DataBuilder struct {
	client   *StreamHouseClient
	dataType string
	data     map[string]interface{}
	metadata map[string]interface{}
}

// NewDataBuilder creates a new data builder
func NewDataBuilder(client *StreamHouseClient, dataType string) *DataBuilder {
	return &DataBuilder{
		client:   client,
		dataType: dataType,
		data:     make(map[string]interface{}),
		metadata: make(map[string]interface{}),
	}
}

// WithUser adds user information to the event
func (db *DataBuilder) WithUser(userID string) *DataBuilder {
	db.data["user_id"] = userID
	return db
}

// WithSession adds session information to the event
func (db *DataBuilder) WithSession(sessionID string) *DataBuilder {
	db.data["session_id"] = sessionID
	return db
}

// WithOrganization adds organization information to the event
func (db *DataBuilder) WithOrganization(orgID string) *DataBuilder {
	db.data["organization_id"] = orgID
	return db
}

// WithIP adds IP address information to the event
func (db *DataBuilder) WithIP(ip string) *DataBuilder {
	// Validate IP address
	if parsedIP := net.ParseIP(ip); parsedIP != nil {
		db.data["ip_address"] = ip
		
		// Add IP type metadata
		if parsedIP.To4() != nil {
			db.metadata["ip_type"] = "ipv4"
		} else {
			db.metadata["ip_type"] = "ipv6"
		}
	}
	return db
}

// WithField adds a custom field to the event
func (db *DataBuilder) WithField(key string, value interface{}) *DataBuilder {
	db.data[key] = value
	return db
}

// WithFields adds multiple custom fields to the event
func (db *DataBuilder) WithFields(fields map[string]interface{}) *DataBuilder {
	for k, v := range fields {
		db.data[k] = v
	}
	return db
}

// WithMetadata adds metadata to the event
func (db *DataBuilder) WithMetadata(key string, value interface{}) *DataBuilder {
	db.metadata[key] = value
	return db
}

// WithTimestamp sets a custom timestamp for the event
func (db *DataBuilder) WithTimestamp(timestamp time.Time) *DataBuilder {
	db.data["timestamp"] = timestamp
	return db
}

// WithAction adds action information (useful for audit logging)
func (db *DataBuilder) WithAction(action string) *DataBuilder {
	db.data["action"] = action
	return db
}

// WithResource adds resource information (useful for audit logging)
func (db *DataBuilder) WithResource(resourceType, resourceID string) *DataBuilder {
	db.data["resource_type"] = resourceType
	db.data["resource_id"] = resourceID
	return db
}

// WithLocation adds geographical location information
func (db *DataBuilder) WithLocation(country, region, city string) *DataBuilder {
	location := make(map[string]interface{})
	if country != "" {
		location["country"] = country
	}
	if region != "" {
		location["region"] = region
	}
	if city != "" {
		location["city"] = city
	}
	if len(location) > 0 {
		db.data["location"] = location
	}
	return db
}

// WithUserAgent adds user agent information
func (db *DataBuilder) WithUserAgent(userAgent string) *DataBuilder {
	db.data["user_agent"] = userAgent
	return db
}

// WithReferrer adds referrer information
func (db *DataBuilder) WithReferrer(referrer string) *DataBuilder {
	db.data["referrer"] = referrer
	return db
}

// WithDuration adds duration information (useful for performance tracking)
func (db *DataBuilder) WithDuration(duration time.Duration) *DataBuilder {
	db.data["duration_ms"] = duration.Milliseconds()
	return db
}

// WithError adds error information
func (db *DataBuilder) WithError(err error) *DataBuilder {
	if err != nil {
		db.data["error"] = err.Error()
		db.data["has_error"] = true
	} else {
		db.data["has_error"] = false
	}
	return db
}

// WithStatus adds status information
func (db *DataBuilder) WithStatus(status string) *DataBuilder {
	db.data["status"] = status
	return db
}

// WithTags adds tags to the event
func (db *DataBuilder) WithTags(tags []string) *DataBuilder {
	db.data["tags"] = tags
	return db
}

// WithEnvironment adds environment information
func (db *DataBuilder) WithEnvironment(env string) *DataBuilder {
	db.metadata["environment"] = env
	return db
}

// WithVersion adds version information
func (db *DataBuilder) WithVersion(version string) *DataBuilder {
	db.metadata["version"] = version
	return db
}

// WithCorrelationID adds correlation ID for request tracing
func (db *DataBuilder) WithCorrelationID(correlationID string) *DataBuilder {
	db.metadata["correlation_id"] = correlationID
	return db
}

// WithRequestID adds request ID for request tracing
func (db *DataBuilder) WithRequestID(requestID string) *DataBuilder {
	db.metadata["request_id"] = requestID
	return db
}

// WithSource adds source information
func (db *DataBuilder) WithSource(source string) *DataBuilder {
	db.metadata["source"] = source
	return db
}

// WithPriority adds priority information
func (db *DataBuilder) WithPriority(priority string) *DataBuilder {
	db.metadata["priority"] = priority
	return db
}

// Stream sends the event synchronously
func (db *DataBuilder) Stream() error {
	// Add metadata to the event if not already present
	if len(db.metadata) > 0 {
		db.data["_metadata"] = db.metadata
	}
	
	return db.client.Stream(db.dataType, db.data)
}

// StreamAsync sends the event asynchronously
func (db *DataBuilder) StreamAsync() {
	// Add metadata to the event if not already present
	if len(db.metadata) > 0 {
		db.data["_metadata"] = db.metadata
	}
	
	db.client.StreamAsync(db.dataType, db.data)
}

// Build returns the built data without streaming
func (db *DataBuilder) Build() map[string]interface{} {
	result := make(map[string]interface{})
	
	// Copy data
	for k, v := range db.data {
		result[k] = v
	}
	
	// Add metadata if present
	if len(db.metadata) > 0 {
		result["_metadata"] = db.metadata
	}
	
	return result
}

// Clone creates a copy of the data builder
func (db *DataBuilder) Clone() *DataBuilder {
	newBuilder := &DataBuilder{
		client:   db.client,
		dataType: db.dataType,
		data:     make(map[string]interface{}),
		metadata: make(map[string]interface{}),
	}
	
	// Copy data
	for k, v := range db.data {
		newBuilder.data[k] = v
	}
	
	// Copy metadata
	for k, v := range db.metadata {
		newBuilder.metadata[k] = v
	}
	
	return newBuilder
}

// Reset clears all data and metadata
func (db *DataBuilder) Reset() *DataBuilder {
	db.data = make(map[string]interface{})
	db.metadata = make(map[string]interface{})
	return db
}

// EventBuilder provides additional methods for building complex events
type EventBuilder struct {
	*DataBuilder
}

// NewEventBuilder creates a new event builder with additional functionality
func NewEventBuilder(client *StreamHouseClient, dataType string) *EventBuilder {
	return &EventBuilder{
		DataBuilder: NewDataBuilder(client, dataType),
	}
}

// WithAuditTrail adds audit trail information
func (eb *EventBuilder) WithAuditTrail(actor, action, resource string) *EventBuilder {
	eb.WithField("actor", actor)
	eb.WithField("action", action)
	eb.WithField("resource", resource)
	eb.WithTimestamp(time.Now())
	return eb
}

// WithPerformanceMetrics adds performance metrics
func (eb *EventBuilder) WithPerformanceMetrics(responseTime time.Duration, memoryUsage int64, cpuUsage float64) *EventBuilder {
	metrics := map[string]interface{}{
		"response_time_ms": responseTime.Milliseconds(),
		"memory_usage_bytes": memoryUsage,
		"cpu_usage_percent": cpuUsage,
	}
	eb.WithField("performance_metrics", metrics)
	return eb
}

// WithBusinessMetrics adds business-specific metrics
func (eb *EventBuilder) WithBusinessMetrics(metrics map[string]interface{}) *EventBuilder {
	eb.WithField("business_metrics", metrics)
	return eb
}

// WithSecurityContext adds security context information
func (eb *EventBuilder) WithSecurityContext(authMethod, permissions []string, riskScore float64) *EventBuilder {
	securityContext := map[string]interface{}{
		"auth_method": authMethod,
		"permissions": permissions,
		"risk_score": riskScore,
	}
	eb.WithField("security_context", securityContext)
	return eb
}

// WithDeviceInfo adds device information
func (eb *EventBuilder) WithDeviceInfo(deviceType, deviceID, platform, version string) *EventBuilder {
	deviceInfo := map[string]interface{}{
		"device_type": deviceType,
		"device_id": deviceID,
		"platform": platform,
		"version": version,
	}
	eb.WithField("device_info", deviceInfo)
	return eb
}

// WithCustomDimensions adds custom dimensions for analytics
func (eb *EventBuilder) WithCustomDimensions(dimensions map[string]string) *EventBuilder {
	eb.WithField("custom_dimensions", dimensions)
	return eb
}

// WithCustomMetrics adds custom metrics for analytics
func (eb *EventBuilder) WithCustomMetrics(metrics map[string]float64) *EventBuilder {
	eb.WithField("custom_metrics", metrics)
	return eb
}

// Batch allows building multiple events efficiently
type BatchBuilder struct {
	client   *StreamHouseClient
	events   []*DataBuilder
	dataType string
}

// NewBatchBuilder creates a new batch builder
func NewBatchBuilder(client *StreamHouseClient, dataType string) *BatchBuilder {
	return &BatchBuilder{
		client:   client,
		dataType: dataType,
		events:   make([]*DataBuilder, 0),
	}
}

// Add adds an event to the batch
func (bb *BatchBuilder) Add() *DataBuilder {
	builder := NewDataBuilder(bb.client, bb.dataType)
	bb.events = append(bb.events, builder)
	return builder
}

// AddEvent adds a pre-built event to the batch
func (bb *BatchBuilder) AddEvent(builder *DataBuilder) *BatchBuilder {
	bb.events = append(bb.events, builder)
	return bb
}

// StreamAll streams all events in the batch synchronously
func (bb *BatchBuilder) StreamAll() []error {
	var errors []error
	for _, event := range bb.events {
		if err := event.Stream(); err != nil {
			errors = append(errors, err)
		}
	}
	return errors
}

// StreamAllAsync streams all events in the batch asynchronously
func (bb *BatchBuilder) StreamAllAsync() {
	for _, event := range bb.events {
		event.StreamAsync()
	}
}

// Count returns the number of events in the batch
func (bb *BatchBuilder) Count() int {
	return len(bb.events)
}

// Clear clears all events from the batch
func (bb *BatchBuilder) Clear() *BatchBuilder {
	bb.events = bb.events[:0]
	return bb
}