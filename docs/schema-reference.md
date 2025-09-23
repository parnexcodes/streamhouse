# Schema Reference

StreamHouse uses a Prisma-like schema system to define data structures, validation rules, and storage configurations. This guide covers all aspects of schema definition and management.

## Schema Structure

A `DataSchema` defines the structure and constraints for a specific data type:

```go
type DataSchema struct {
    Name        string                    // Unique schema name
    Fields      map[string]FieldConfig    // Field definitions
    TTL         *time.Duration           // Time-to-live for data
    Partitions  []string                 // ClickHouse partitioning fields
    IndexFields []string                 // Fields to index for performance
}
```

## Field Types

StreamHouse supports the following field types:

### String
```go
"username": {
    Type:        "string",
    Required:    true,
    Index:       true,
    Description: "User's login name",
    Default:     nil,
}
```

### Integer
```go
"age": {
    Type:        "int",
    Required:    false,
    Index:       false,
    Description: "User's age in years",
    Default:     0,
}
```

### Float
```go
"price": {
    Type:        "float",
    Required:    true,
    Index:       false,
    Description: "Product price in USD",
    Default:     0.0,
}
```

### Boolean
```go
"is_active": {
    Type:        "bool",
    Required:    false,
    Index:       true,
    Description: "Whether the user is active",
    Default:     true,
}
```

### DateTime
```go
"created_at": {
    Type:        "datetime",
    Required:    true,
    Index:       true,
    Description: "Record creation timestamp",
    Default:     nil, // Will use current time if not provided
}
```

### JSON
```go
"metadata": {
    Type:        "json",
    Required:    false,
    Index:       false,
    Description: "Additional structured data",
    Default:     nil,
}
```

## Field Configuration

Each field can be configured with the following properties:

```go
type FieldConfig struct {
    Type        string                    // Field data type
    Required    bool                      // Whether field is mandatory
    Index       bool                      // Whether to create database index
    Description string                    // Human-readable description
    Default     interface{}               // Default value if not provided
    Validator   func(interface{}) error   // Custom validation function
}
```

### Custom Validation

You can add custom validation logic to fields:

```go
"email": {
    Type:     "string",
    Required: true,
    Validator: func(value interface{}) error {
        email, ok := value.(string)
        if !ok {
            return errors.New("email must be a string")
        }
        if !strings.Contains(email, "@") {
            return errors.New("invalid email format")
        }
        return nil
    },
}
```

## Schema Examples

### User Activity Schema
```go
userActivitySchema := &streamhouse.DataSchema{
    Name: "user.activity",
    Fields: map[string]streamhouse.FieldConfig{
        "user_id": {
            Type:        "string",
            Required:    true,
            Index:       true,
            Description: "Unique user identifier",
        },
        "action": {
            Type:        "string",
            Required:    true,
            Index:       true,
            Description: "Action performed by user",
        },
        "timestamp": {
            Type:        "datetime",
            Required:    true,
            Index:       true,
            Description: "When the action occurred",
        },
        "session_id": {
            Type:        "string",
            Required:    false,
            Index:       true,
            Description: "User session identifier",
        },
        "ip_address": {
            Type:        "string",
            Required:    false,
            Index:       false,
            Description: "Client IP address",
        },
        "user_agent": {
            Type:        "string",
            Required:    false,
            Index:       false,
            Description: "Client user agent string",
        },
        "metadata": {
            Type:        "json",
            Required:    false,
            Index:       false,
            Description: "Additional event data",
        },
    },
    TTL:         &[]time.Duration{90 * 24 * time.Hour}[0], // 90 days
    IndexFields: []string{"user_id", "action", "timestamp"},
    Partitions:  []string{"toYYYYMM(timestamp)"}, // Monthly partitions
}
```

### E-commerce Order Schema
```go
orderSchema := &streamhouse.DataSchema{
    Name: "ecommerce.order",
    Fields: map[string]streamhouse.FieldConfig{
        "order_id": {
            Type:        "string",
            Required:    true,
            Index:       true,
            Description: "Unique order identifier",
        },
        "customer_id": {
            Type:        "string",
            Required:    true,
            Index:       true,
            Description: "Customer identifier",
        },
        "total_amount": {
            Type:        "float",
            Required:    true,
            Index:       false,
            Description: "Total order amount",
        },
        "currency": {
            Type:        "string",
            Required:    true,
            Index:       true,
            Description: "Currency code (USD, EUR, etc.)",
            Default:     "USD",
        },
        "status": {
            Type:        "string",
            Required:    true,
            Index:       true,
            Description: "Order status",
            Default:     "pending",
        },
        "items": {
            Type:        "json",
            Required:    true,
            Index:       false,
            Description: "Array of order items",
        },
        "shipping_address": {
            Type:        "json",
            Required:    false,
            Index:       false,
            Description: "Shipping address details",
        },
        "created_at": {
            Type:        "datetime",
            Required:    true,
            Index:       true,
            Description: "Order creation time",
        },
        "updated_at": {
            Type:        "datetime",
            Required:    false,
            Index:       false,
            Description: "Last update time",
        },
    },
    TTL:         &[]time.Duration{7 * 365 * 24 * time.Hour}[0], // 7 years
    IndexFields: []string{"order_id", "customer_id", "status", "created_at"},
    Partitions:  []string{"toYYYYMM(created_at)"},
}
```

### IoT Sensor Schema
```go
sensorSchema := &streamhouse.DataSchema{
    Name: "iot.sensor.reading",
    Fields: map[string]streamhouse.FieldConfig{
        "device_id": {
            Type:        "string",
            Required:    true,
            Index:       true,
            Description: "Unique device identifier",
        },
        "sensor_type": {
            Type:        "string",
            Required:    true,
            Index:       true,
            Description: "Type of sensor (temperature, humidity, etc.)",
        },
        "value": {
            Type:        "float",
            Required:    true,
            Index:       false,
            Description: "Sensor reading value",
        },
        "unit": {
            Type:        "string",
            Required:    true,
            Index:       false,
            Description: "Unit of measurement",
        },
        "location": {
            Type:        "json",
            Required:    false,
            Index:       false,
            Description: "GPS coordinates and location info",
        },
        "quality": {
            Type:        "float",
            Required:    false,
            Index:       false,
            Description: "Reading quality score (0-1)",
            Default:     1.0,
        },
        "timestamp": {
            Type:        "datetime",
            Required:    true,
            Index:       true,
            Description: "Reading timestamp",
        },
    },
    TTL:         &[]time.Duration{365 * 24 * time.Hour}[0], // 1 year
    IndexFields: []string{"device_id", "sensor_type", "timestamp"},
    Partitions:  []string{"toYYYYMMDD(timestamp)"}, // Daily partitions for high volume
}
```

## Schema Registration

Register schemas with your StreamHouse client:

```go
// Register a single schema
err := client.RegisterSchema(userActivitySchema)
if err != nil {
    log.Fatal(err)
}

// Register multiple schemas
schemas := []*streamhouse.DataSchema{
    userActivitySchema,
    orderSchema,
    sensorSchema,
}

for _, schema := range schemas {
    if err := client.RegisterSchema(schema); err != nil {
        log.Printf("Failed to register schema %s: %v", schema.Name, err)
    }
}
```

## Schema Validation

StreamHouse automatically validates data against registered schemas:

```go
// This will succeed
err := client.Stream("user.activity", map[string]interface{}{
    "user_id":   "user123",
    "action":    "login",
    "timestamp": time.Now(),
})

// This will fail validation (missing required field)
err := client.Stream("user.activity", map[string]interface{}{
    "action": "login", // missing required user_id
})
if err != nil {
    // Handle validation error
    if validationErr, ok := err.(*streamhouse.ValidationError); ok {
        log.Printf("Validation failed for field %s: %s", 
            validationErr.Field, validationErr.Message)
    }
}
```

## Advanced Features

### Time-to-Live (TTL)

Set automatic data expiration:

```go
schema := &streamhouse.DataSchema{
    Name: "temporary.events",
    TTL:  &[]time.Duration{24 * time.Hour}[0], // Data expires after 24 hours
    // ... fields
}
```

### Partitioning

Configure ClickHouse table partitioning for better performance:

```go
schema := &streamhouse.DataSchema{
    Name: "high.volume.events",
    Partitions: []string{
        "toYYYYMM(timestamp)",           // Monthly partitions
        "intHash64(user_id) % 10",       // Hash partitioning by user
    },
    // ... fields
}
```

### Indexing

Specify which fields should be indexed:

```go
schema := &streamhouse.DataSchema{
    Name: "searchable.events",
    IndexFields: []string{
        "user_id",
        "action",
        "timestamp",
        "status",
    },
    // ... fields
}
```

## Best Practices

### Schema Design

1. **Use descriptive names**: Choose clear, hierarchical names like `user.activity.login`
2. **Index strategically**: Only index fields you'll query frequently
3. **Set appropriate TTL**: Balance storage costs with data retention needs
4. **Partition large datasets**: Use time-based or hash partitioning for high-volume data

### Field Configuration

1. **Mark required fields**: Use `Required: true` for essential data
2. **Provide defaults**: Set sensible defaults for optional fields
3. **Add descriptions**: Document field purposes for team clarity
4. **Use appropriate types**: Choose the most specific type for your data

### Performance Optimization

1. **Batch related events**: Group similar events in the same schema
2. **Optimize partitioning**: Choose partition keys that align with query patterns
3. **Limit JSON usage**: Use structured fields when possible for better performance
4. **Monitor index usage**: Remove unused indexes to improve write performance

## Schema Evolution

StreamHouse supports schema evolution with backward compatibility:

```go
// Version 1
v1Schema := &streamhouse.DataSchema{
    Name: "user.profile",
    Fields: map[string]streamhouse.FieldConfig{
        "user_id": {Type: "string", Required: true},
        "name":    {Type: "string", Required: true},
    },
}

// Version 2 - Add optional field
v2Schema := &streamhouse.DataSchema{
    Name: "user.profile",
    Fields: map[string]streamhouse.FieldConfig{
        "user_id": {Type: "string", Required: true},
        "name":    {Type: "string", Required: true},
        "email":   {Type: "string", Required: false}, // New optional field
    },
}
```

### Migration Guidelines

1. **Add fields as optional**: New fields should have `Required: false`
2. **Provide defaults**: Set appropriate default values for new fields
3. **Avoid breaking changes**: Don't remove or change existing required fields
4. **Version your schemas**: Use semantic versioning in schema names if needed