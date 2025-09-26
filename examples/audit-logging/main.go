package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/parnexcodes/streamhouse"
)

func main() {
	// Load configuration
	config, err := streamhouse.LoadConfig("config.yaml")
	if err != nil {
		log.Printf("Failed to load config, using defaults: %v", err)
		config = streamhouse.DefaultConfig()
	}

	// Create StreamHouse client
	client, err := streamhouse.NewClient(config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Register audit schemas
	if err := registerAuditSchemas(client); err != nil {
		log.Fatalf("Failed to register schemas: %v", err)
	}

	// Start consumer in background
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := client.StartConsumer(ctx); err != nil {
			log.Printf("Consumer error: %v", err)
		} else {
			log.Printf("Consumer started successfully")
		}
	}()

	// Wait a moment for consumer to start
	time.Sleep(2 * time.Second)

	// Simulate audit events
	fmt.Println("Starting audit logging simulation...")
	simulateAuditEvents(client)

	// Wait a bit more for processing
	fmt.Println("Waiting for events to be processed...")
	time.Sleep(10 * time.Second)

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("Audit logging example is running. Press Ctrl+C to stop...")
	<-sigChan

	fmt.Println("Shutting down...")
	cancel()
	time.Sleep(2 * time.Second)
}

func registerAuditSchemas(client *streamhouse.StreamHouseClient) error {
	// User Management Audit Schema
	userAuditSchema := &streamhouse.DataSchema{
		Name: "audit.user.action",
		Fields: map[string]streamhouse.FieldConfig{
			"user_id": {
				Type:        streamhouse.FieldTypeString,
				Required:    true,
				Index:       true,
				Description: "ID of the user performing the action",
			},
			"target_user_id": {
				Type:        streamhouse.FieldTypeString,
				Required:    false,
				Index:       true,
				Description: "ID of the user being acted upon (for user management actions)",
			},
			"action": {
				Type:        streamhouse.FieldTypeString,
				Required:    true,
				Index:       true,
				Description: "Type of action performed (login, logout, create, update, delete)",
			},
			"resource": {
				Type:        streamhouse.FieldTypeString,
				Required:    true,
				Index:       true,
				Description: "Resource being accessed (user, profile, settings)",
			},
			"ip_address": {
				Type:        streamhouse.FieldTypeString,
				Required:    false,
				Index:       true,
				Description: "IP address of the user",
			},
			"user_agent": {
				Type:        streamhouse.FieldTypeString,
				Required:    false,
				Description: "User agent string from the request",
			},
			"session_id": {
				Type:        streamhouse.FieldTypeString,
				Required:    false,
				Index:       true,
				Description: "Session ID for the user",
			},
			"success": {
				Type:        streamhouse.FieldTypeBool,
				Required:    true,
				Index:       true,
				Description: "Whether the action was successful",
			},
			"error_message": {
				Type:        streamhouse.FieldTypeString,
				Required:    false,
				Description: "Error message if action failed",
			},
			"metadata": {
				Type:        streamhouse.FieldTypeJSON,
				Required:    false,
				Description: "Additional metadata about the action",
			},
		},
		IndexFields: []string{"user_id", "action", "resource", "success", "ip_address"},
	}

	// Data Access Audit Schema
	dataAccessSchema := &streamhouse.DataSchema{
		Name: "audit.data.access",
		Fields: map[string]streamhouse.FieldConfig{
			"user_id": {
				Type:        streamhouse.FieldTypeString,
				Required:    true,
				Index:       true,
				Description: "ID of the user accessing data",
			},
			"resource_type": {
				Type:        streamhouse.FieldTypeString,
				Required:    true,
				Index:       true,
				Description: "Type of resource accessed (database, file, api)",
			},
			"resource_id": {
				Type:        streamhouse.FieldTypeString,
				Required:    true,
				Index:       true,
				Description: "Specific resource identifier",
			},
			"operation": {
				Type:        streamhouse.FieldTypeString,
				Required:    true,
				Index:       true,
				Description: "Operation performed (read, write, delete, export)",
			},
			"table_name": {
				Type:        streamhouse.FieldTypeString,
				Required:    false,
				Index:       true,
				Description: "Database table name if applicable",
			},
			"record_count": {
				Type:        streamhouse.FieldTypeInt,
				Required:    false,
				Description: "Number of records accessed",
			},
			"query": {
				Type:        streamhouse.FieldTypeString,
				Required:    false,
				Description: "SQL query or API endpoint accessed",
			},
			"ip_address": {
				Type:        streamhouse.FieldTypeString,
				Required:    false,
				Index:       true,
				Description: "IP address of the accessor",
			},
			"success": {
				Type:        streamhouse.FieldTypeBool,
				Required:    true,
				Index:       true,
				Description: "Whether the access was successful",
			},
			"duration_ms": {
				Type:        streamhouse.FieldTypeInt,
				Required:    false,
				Description: "Duration of the operation in milliseconds",
			},
		},
		IndexFields: []string{"user_id", "resource_type", "operation", "success"},
	}

	// System Configuration Audit Schema
	configAuditSchema := &streamhouse.DataSchema{
		Name: "audit.system.config",
		Fields: map[string]streamhouse.FieldConfig{
			"admin_user_id": {
				Type:        streamhouse.FieldTypeString,
				Required:    true,
				Index:       true,
				Description: "ID of the admin user making changes",
			},
			"config_section": {
				Type:        streamhouse.FieldTypeString,
				Required:    true,
				Index:       true,
				Description: "Configuration section modified",
			},
			"config_key": {
				Type:        streamhouse.FieldTypeString,
				Required:    true,
				Index:       true,
				Description: "Specific configuration key changed",
			},
			"old_value": {
				Type:        streamhouse.FieldTypeString,
				Required:    false,
				Description: "Previous value of the configuration",
			},
			"new_value": {
				Type:        streamhouse.FieldTypeString,
				Required:    false,
				Description: "New value of the configuration",
			},
			"change_reason": {
				Type:        streamhouse.FieldTypeString,
				Required:    false,
				Description: "Reason for the configuration change",
			},
			"ip_address": {
				Type:        streamhouse.FieldTypeString,
				Required:    false,
				Index:       true,
				Description: "IP address of the admin",
			},
			"approval_required": {
				Type:        streamhouse.FieldTypeBool,
				Required:    false,
				Description: "Whether the change requires approval",
			},
			"approved_by": {
				Type:        streamhouse.FieldTypeString,
				Required:    false,
				Description: "User ID who approved the change",
			},
		},
		IndexFields: []string{"admin_user_id", "config_section", "config_key"},
	}

	// Register all schemas
	schemas := []*streamhouse.DataSchema{
		userAuditSchema,
		dataAccessSchema,
		configAuditSchema,
	}

	for _, schema := range schemas {
		if err := client.RegisterSchema(schema); err != nil {
			return fmt.Errorf("failed to register schema %s: %w", schema.Name, err)
		}
		fmt.Printf("✓ Registered schema: %s\n", schema.Name)
	}

	return nil
}

func simulateAuditEvents(client *streamhouse.StreamHouseClient) {
	fmt.Println("Simulating audit events...")

	// Simulate user login
	fmt.Println("1. Simulating user login...")
	err := client.Data("audit.user.action").
		WithUser("user123").
		WithSession("sess_abc123").
		WithIP("192.168.1.100").
		WithField("action", "login").
		WithField("resource", "user").
		WithField("success", true).
		WithField("user_agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36").
		WithField("metadata", map[string]interface{}{
			"login_method": "password",
			"mfa_enabled":  true,
		}).
		Stream()
	if err != nil {
		fmt.Printf("Error streaming user login: %v\n", err)
	} else {
		fmt.Println("✓ User login event streamed successfully")
	}

	time.Sleep(1 * time.Second)

	// Simulate data access
	fmt.Println("2. Simulating data access...")
	client.Data("audit.data.access").
		WithUser("user123").
		WithIP("192.168.1.100").
		WithField("resource_type", "database").
		WithField("resource_id", "customer_db").
		WithField("operation", "read").
		WithField("table_name", "customers").
		WithField("record_count", 150).
		WithField("query", "SELECT * FROM customers WHERE created_at > '2024-01-01'").
		WithField("success", true).
		WithField("duration_ms", 245).
		StreamAsync()

	time.Sleep(1 * time.Second)

	// Simulate failed login attempt
	fmt.Println("3. Simulating failed login attempt...")
	client.Data("audit.user.action").
		WithUser("unknown_user").
		WithIP("10.0.0.50").
		WithField("action", "login").
		WithField("resource", "user").
		WithField("success", false).
		WithField("error_message", "Invalid credentials").
		WithField("user_agent", "curl/7.68.0").
		WithField("metadata", map[string]interface{}{
			"login_method":   "password",
			"attempt_number": 3,
		}).
		StreamAsync()

	time.Sleep(1 * time.Second)

	// Simulate system configuration change
	fmt.Println("4. Simulating system configuration change...")
	client.Data("audit.system.config").
		WithField("admin_user_id", "admin456").
		WithIP("192.168.1.10").
		WithField("config_section", "security").
		WithField("config_key", "password_policy.min_length").
		WithField("old_value", "8").
		WithField("new_value", "12").
		WithField("change_reason", "Compliance requirement update").
		WithField("approval_required", true).
		WithField("approved_by", "admin789").
		StreamAsync()

	time.Sleep(1 * time.Second)

	// Simulate bulk data export
	fmt.Println("5. Simulating bulk data export...")
	client.Data("audit.data.access").
		WithUser("analyst001").
		WithIP("192.168.1.200").
		WithField("resource_type", "api").
		WithField("resource_id", "export_api").
		WithField("operation", "export").
		WithField("table_name", "user_analytics").
		WithField("record_count", 50000).
		WithField("query", "/api/v1/analytics/export?format=csv&date_range=30d").
		WithField("success", true).
		WithField("duration_ms", 15000).
		StreamAsync()

	time.Sleep(1 * time.Second)

	// Simulate user profile update
	fmt.Println("6. Simulating user profile update...")
	client.Data("audit.user.action").
		WithUser("user123").
		WithSession("sess_abc123").
		WithIP("192.168.1.100").
		WithField("action", "update").
		WithField("resource", "profile").
		WithField("target_user_id", "user123").
		WithField("success", true).
		WithField("metadata", map[string]interface{}{
			"fields_updated": []string{"email", "phone"},
			"previous_email": "old@example.com",
			"new_email":      "new@example.com",
		}).
		StreamAsync()

	fmt.Println("✓ All audit events have been queued for processing")
	fmt.Println("Check your ClickHouse database for the audit data!")
}
