package streamhouse

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/goccy/go-yaml"
)

// Config represents the main configuration structure
type Config struct {
	Redis         RedisConfig      `json:"redis" yaml:"redis"`
	ClickHouse    ClickHouseConfig `json:"clickhouse" yaml:"clickhouse"`
	StreamName    string           `json:"stream_name" yaml:"stream_name"`
	BatchSize     int              `json:"batch_size" yaml:"batch_size"`
	FlushInterval time.Duration    `json:"flush_interval" yaml:"flush_interval"`
	Workers       int              `json:"workers" yaml:"workers"`
	Consumer      ConsumerConfig   `json:"consumer" yaml:"consumer"`
	Monitoring    MonitoringConfig `json:"monitoring" yaml:"monitoring"`
	Logging       LoggingConfig    `json:"logging" yaml:"logging"`
}

// RedisConfig holds Redis connection configuration
type RedisConfig struct {
	Host         string        `json:"host" yaml:"host"`
	Port         int           `json:"port" yaml:"port"`
	Password     string        `json:"password" yaml:"password"`
	DB           int           `json:"db" yaml:"db"`
	PoolSize     int           `json:"pool_size" yaml:"pool_size"`
	MinIdleConns int           `json:"min_idle_conns" yaml:"min_idle_conns"`
	MaxRetries   int           `json:"max_retries" yaml:"max_retries"`
	DialTimeout  time.Duration `json:"dial_timeout" yaml:"dial_timeout"`
	ReadTimeout  time.Duration `json:"read_timeout" yaml:"read_timeout"`
	WriteTimeout time.Duration `json:"write_timeout" yaml:"write_timeout"`
	IdleTimeout  time.Duration `json:"idle_timeout" yaml:"idle_timeout"`
	TLS          *TLSConfig    `json:"tls,omitempty" yaml:"tls,omitempty"`
}

// ClickHouseConfig holds ClickHouse connection configuration
type ClickHouseConfig struct {
	Host            string        `json:"host" yaml:"host"`
	Port            int           `json:"port" yaml:"port"`
	Database        string        `json:"database" yaml:"database"`
	Username        string        `json:"username" yaml:"username"`
	Password        string        `json:"password" yaml:"password"`
	MaxOpenConns    int           `json:"max_open_conns" yaml:"max_open_conns"`
	MaxIdleConns    int           `json:"max_idle_conns" yaml:"max_idle_conns"`
	ConnMaxLifetime time.Duration `json:"conn_max_lifetime" yaml:"conn_max_lifetime"`
	DialTimeout     time.Duration `json:"dial_timeout" yaml:"dial_timeout"`
	ReadTimeout     time.Duration `json:"read_timeout" yaml:"read_timeout"`
	WriteTimeout    time.Duration `json:"write_timeout" yaml:"write_timeout"`
	Compression     bool          `json:"compression" yaml:"compression"`
	TLS             *TLSConfig    `json:"tls,omitempty" yaml:"tls,omitempty"`
}

// ConsumerConfig holds consumer-specific configuration
type ConsumerConfig struct {
	GroupName       string        `json:"group_name" yaml:"group_name"`
	ConsumerName    string        `json:"consumer_name" yaml:"consumer_name"`
	BlockTime       time.Duration `json:"block_time" yaml:"block_time"`
	Count           int64         `json:"count" yaml:"count"`
	NoAck           bool          `json:"no_ack" yaml:"no_ack"`
	RetryAttempts   int           `json:"retry_attempts" yaml:"retry_attempts"`
	RetryDelay      time.Duration `json:"retry_delay" yaml:"retry_delay"`
	DeadLetterQueue bool          `json:"dead_letter_queue" yaml:"dead_letter_queue"`
	DeadLetterTTL   time.Duration `json:"dead_letter_ttl" yaml:"dead_letter_ttl"`
	ClaimInterval   time.Duration `json:"claim_interval" yaml:"claim_interval"`
}

// MonitoringConfig holds monitoring and metrics configuration
type MonitoringConfig struct {
	Enabled         bool          `json:"enabled" yaml:"enabled"`
	MetricsInterval time.Duration `json:"metrics_interval" yaml:"metrics_interval"`
	HealthCheck     bool          `json:"health_check" yaml:"health_check"`
	HealthInterval  time.Duration `json:"health_interval" yaml:"health_interval"`
}

// LoggingConfig holds logging configuration
type LoggingConfig struct {
	Level  string `json:"level" yaml:"level"`
	Format string `json:"format" yaml:"format"` // "json" or "text"
	Output string `json:"output" yaml:"output"` // "stdout", "stderr", or file path
}

// TLSConfig holds TLS configuration
type TLSConfig struct {
	Enabled            bool   `json:"enabled" yaml:"enabled"`
	CertFile           string `json:"cert_file" yaml:"cert_file"`
	KeyFile            string `json:"key_file" yaml:"key_file"`
	CAFile             string `json:"ca_file" yaml:"ca_file"`
	InsecureSkipVerify bool   `json:"insecure_skip_verify" yaml:"insecure_skip_verify"`
}

// DefaultConfig returns a configuration with sensible defaults
func DefaultConfig() *Config {
	return &Config{
		Redis: RedisConfig{
			Host:         "localhost",
			Port:         6379,
			DB:           0,
			PoolSize:     10,
			MinIdleConns: 5,
			MaxRetries:   3,
			DialTimeout:  5 * time.Second,
			ReadTimeout:  3 * time.Second,
			WriteTimeout: 3 * time.Second,
			IdleTimeout:  5 * time.Minute,
		},
		ClickHouse: ClickHouseConfig{
			Host:            "localhost",
			Port:            9000,
			Database:        "default",
			Username:        "default",
			MaxOpenConns:    10,
			MaxIdleConns:    5,
			ConnMaxLifetime: time.Hour,
			DialTimeout:     10 * time.Second,
			ReadTimeout:     30 * time.Second,
			WriteTimeout:    30 * time.Second,
			Compression:     true,
		},
		StreamName:    "streamhouse",
		BatchSize:     100,
		FlushInterval: 5 * time.Second,
		Workers:       4,
		Consumer: ConsumerConfig{
			GroupName:       "streamhouse-group",
			ConsumerName:    "streamhouse-consumer",
			BlockTime:       1 * time.Second,
			Count:           10,
			NoAck:           false,
			RetryAttempts:   3,
			RetryDelay:      1 * time.Second,
			DeadLetterQueue: true,
			DeadLetterTTL:   24 * time.Hour,
			ClaimInterval:   30 * time.Second,
		},
		Monitoring: MonitoringConfig{
			Enabled:         true,
			MetricsInterval: 30 * time.Second,
			HealthCheck:     true,
			HealthInterval:  10 * time.Second,
		},
		Logging: LoggingConfig{
			Level:  "info",
			Format: "json",
			Output: "stdout",
		},
	}
}

// LoadConfig loads configuration from a file
func LoadConfig(path string) (*Config, error) {
	if path == "" {
		return DefaultConfig(), nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	config := DefaultConfig()
	ext := filepath.Ext(path)

	switch ext {
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(data, config); err != nil {
			return nil, fmt.Errorf("failed to parse YAML config: %w", err)
		}
	case ".json":
		if err := json.Unmarshal(data, config); err != nil {
			return nil, fmt.Errorf("failed to parse JSON config: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported config file format: %s", ext)
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return config, nil
}

// LoadConfigFromEnv loads configuration from environment variables
func LoadConfigFromEnv() *Config {
	config := DefaultConfig()

	// Redis configuration
	if host := os.Getenv("STREAMHOUSE_REDIS_HOST"); host != "" {
		// Check if host contains port
		if h, p, err := net.SplitHostPort(host); err == nil {
			config.Redis.Host = h
			if port, err := strconv.Atoi(p); err == nil {
				config.Redis.Port = port
			}
		} else {
			// No port specified, use host as-is
			config.Redis.Host = host
		}
	}
	// Allow separate port configuration to override
	if port := os.Getenv("STREAMHOUSE_REDIS_PORT"); port != "" {
		if p, err := strconv.Atoi(port); err == nil {
			config.Redis.Port = p
		}
	}
	if password := os.Getenv("STREAMHOUSE_REDIS_PASSWORD"); password != "" {
		config.Redis.Password = password
	}

	// ClickHouse configuration
	if host := os.Getenv("STREAMHOUSE_CLICKHOUSE_HOST"); host != "" {
		// Check if host contains port
		if h, p, err := net.SplitHostPort(host); err == nil {
			config.ClickHouse.Host = h
			if port, err := strconv.Atoi(p); err == nil {
				config.ClickHouse.Port = port
			}
		} else {
			// No port specified, use host as-is
			config.ClickHouse.Host = host
		}
	}
	// Allow separate port configuration to override
	if port := os.Getenv("STREAMHOUSE_CLICKHOUSE_PORT"); port != "" {
		if p, err := strconv.Atoi(port); err == nil {
			config.ClickHouse.Port = p
		}
	}
	if database := os.Getenv("STREAMHOUSE_CLICKHOUSE_DATABASE"); database != "" {
		config.ClickHouse.Database = database
	}
	if username := os.Getenv("STREAMHOUSE_CLICKHOUSE_USERNAME"); username != "" {
		config.ClickHouse.Username = username
	}
	if password := os.Getenv("STREAMHOUSE_CLICKHOUSE_PASSWORD"); password != "" {
		config.ClickHouse.Password = password
	}

	// Stream configuration
	if streamName := os.Getenv("STREAMHOUSE_STREAM_NAME"); streamName != "" {
		config.StreamName = streamName
	}

	return config
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.Redis.Host == "" {
		return NewValidationError("redis.host", c.Redis.Host, "host cannot be empty")
	}
	if c.Redis.Port <= 0 || c.Redis.Port > 65535 {
		return NewValidationError("redis.port", c.Redis.Port, "port must be between 1 and 65535")
	}

	if c.ClickHouse.Host == "" {
		return NewValidationError("clickhouse.host", c.ClickHouse.Host, "host cannot be empty")
	}
	if c.ClickHouse.Port <= 0 || c.ClickHouse.Port > 65535 {
		return NewValidationError("clickhouse.port", c.ClickHouse.Port, "port must be between 1 and 65535")
	}
	if c.ClickHouse.Database == "" {
		return NewValidationError("clickhouse.database", c.ClickHouse.Database, "database cannot be empty")
	}

	if c.StreamName == "" {
		return NewValidationError("stream_name", c.StreamName, "stream name cannot be empty")
	}
	if c.BatchSize <= 0 {
		return NewValidationError("batch_size", c.BatchSize, "batch size must be greater than 0")
	}
	if c.Workers <= 0 {
		return NewValidationError("workers", c.Workers, "workers must be greater than 0")
	}

	if c.Consumer.GroupName == "" {
		return NewValidationError("consumer.group_name", c.Consumer.GroupName, "consumer group name cannot be empty")
	}
	if c.Consumer.ConsumerName == "" {
		return NewValidationError("consumer.consumer_name", c.Consumer.ConsumerName, "consumer name cannot be empty")
	}

	return nil
}

// RedisAddr returns the Redis address in host:port format
func (c *Config) RedisAddr() string {
	return fmt.Sprintf("%s:%d", c.Redis.Host, c.Redis.Port)
}

// ClickHouseAddr returns the ClickHouse address in host:port format
func (c *Config) ClickHouseAddr() string {
	return fmt.Sprintf("%s:%d", c.ClickHouse.Host, c.ClickHouse.Port)
}

// SaveConfig saves the configuration to a file
func (c *Config) SaveConfig(path string) error {
	ext := filepath.Ext(path)
	var data []byte
	var err error

	switch ext {
	case ".yaml", ".yml":
		data, err = yaml.Marshal(c)
		if err != nil {
			return fmt.Errorf("failed to marshal YAML config: %w", err)
		}
	case ".json":
		data, err = json.MarshalIndent(c, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal JSON config: %w", err)
		}
	default:
		return fmt.Errorf("unsupported config file format: %s", ext)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}