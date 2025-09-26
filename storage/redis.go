package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisStreamsClient handles Redis Streams operations
type RedisStreamsClient struct {
	client     *redis.Client
	streamName string
	ctx        context.Context
}

// InitWithClient initializes the RedisStreamsClient with an existing Redis client
func (r *RedisStreamsClient) InitWithClient(client *redis.Client, streamName string, ctx context.Context) error {
	if client == nil {
		return fmt.Errorf("Redis client cannot be nil")
	}

	r.client = client
	r.streamName = streamName
	r.ctx = ctx

	return nil
}

// NewRedisStreamsClient creates a new Redis Streams client
func NewRedisStreamsClient(addr, password string, db int, streamName string) (*RedisStreamsClient, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	ctx := context.Background()

	// Test connection
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &RedisStreamsClient{
		client:     rdb,
		streamName: streamName,
		ctx:        ctx,
	}, nil
}

// StreamMessage represents a message in Redis Streams
type StreamMessage struct {
	ID           string                 `json:"id"`
	DataType     string                 `json:"data_type"`
	Data         map[string]interface{} `json:"data"`
	Metadata     map[string]interface{} `json:"metadata,omitempty"`
	MetadataJSON string                 `json:"metadata_json,omitempty"` // Pre-marshaled metadata for performance
}

// AddMessage adds a message to the Redis Stream
func (r *RedisStreamsClient) AddMessage(dataType string, data map[string]interface{}) (string, error) {
	// Prepare the message
	message := StreamMessage{
		DataType: dataType,
		Data:     data,
	}

	// Extract metadata if present
	if metadata, ok := data["_metadata"]; ok {
		if metadataMap, ok := metadata.(map[string]interface{}); ok {
			message.Metadata = metadataMap
			// Remove metadata from main data to avoid duplication
			dataCopy := make(map[string]interface{})
			for k, v := range data {
				if k != "_metadata" {
					dataCopy[k] = v
				}
			}
			message.Data = dataCopy
		}
	}

	// Convert message to Redis fields
	fields := map[string]interface{}{
		"data_type": dataType,
		"timestamp": time.Now().Unix(),
	}

	// Add data fields to the top level
	for k, v := range message.Data {
		// To avoid issues with type assertions later, we should marshal complex types
		if _, ok := v.(string); !ok {
			jsonVal, err := json.Marshal(v)
			if err != nil {
				return "", fmt.Errorf("failed to marshal field %s: %w", k, err)
			}
			fields[k] = string(jsonVal)
		} else {
			fields[k] = v
		}
	}

	// Serialize metadata as JSON if present
	if len(message.Metadata) > 0 {
		metadataJSON, err := json.Marshal(message.Metadata)
		if err != nil {
			return "", fmt.Errorf("failed to marshal metadata: %w", err)
		}
		fields["metadata"] = string(metadataJSON)
	}

	// Add to stream
	result := r.client.XAdd(r.ctx, &redis.XAddArgs{
		Stream: r.streamName,
		Values: fields,
	})

	if result.Err() != nil {
		return "", fmt.Errorf("failed to add message to stream: %w", result.Err())
	}

	return result.Val(), nil
}

// ReadMessages reads messages from the Redis Stream
func (r *RedisStreamsClient) ReadMessages(count int64, block time.Duration) ([]StreamMessage, error) {
	args := &redis.XReadArgs{
		Streams: []string{r.streamName, "$"},
		Count:   count,
		Block:   block,
	}

	result := r.client.XRead(r.ctx, args)
	if result.Err() != nil {
		if result.Err() == redis.Nil {
			return []StreamMessage{}, nil // No messages available
		}
		return nil, fmt.Errorf("failed to read messages: %w", result.Err())
	}

	var messages []StreamMessage
	for _, stream := range result.Val() {
		for _, msg := range stream.Messages {
			streamMsg, err := r.parseMessage(msg)
			if err != nil {
				// Log error but continue processing other messages
				continue
			}
			messages = append(messages, streamMsg)
		}
	}

	return messages, nil
}

// ReadMessagesFromGroup reads messages from a consumer group
func (r *RedisStreamsClient) ReadMessagesFromGroup(groupName, consumerName string, count int64, block time.Duration) ([]StreamMessage, error) {
	args := &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumerName,
		Streams:  []string{r.streamName, ">"},
		Count:    count,
		Block:    block,
	}

	result := r.client.XReadGroup(r.ctx, args)
	if result.Err() != nil {
		if result.Err() == redis.Nil {
			return []StreamMessage{}, nil // No messages available
		}
		return nil, fmt.Errorf("failed to read messages from group: %w", result.Err())
	}

	var messages []StreamMessage
	for _, stream := range result.Val() {
		for _, msg := range stream.Messages {
			streamMsg, err := r.parseMessage(msg)
			if err != nil {
				// Log error but continue processing other messages
				continue
			}
			messages = append(messages, streamMsg)
		}
	}

	return messages, nil
}

// CreateConsumerGroup creates a consumer group for the stream
func (r *RedisStreamsClient) CreateConsumerGroup(groupName string) error {
	result := r.client.XGroupCreateMkStream(r.ctx, r.streamName, groupName, "0")
	if result.Err() != nil {
		// Check if group already exists
		errStr := result.Err().Error()
		if strings.Contains(errStr, "BUSYGROUP") {
			return nil // Group already exists, which is fine
		}
		return fmt.Errorf("failed to create consumer group: %w", result.Err())
	}
	return nil
}

// AckMessage acknowledges a message as processed
func (r *RedisStreamsClient) AckMessage(groupName, messageID string) error {
	result := r.client.XAck(r.ctx, r.streamName, groupName, messageID)
	if result.Err() != nil {
		return fmt.Errorf("failed to acknowledge message: %w", result.Err())
	}
	return nil
}

// GetPendingMessages gets pending messages for a consumer group
func (r *RedisStreamsClient) GetPendingMessages(groupName string) (*redis.XPending, error) {
	result := r.client.XPending(r.ctx, r.streamName, groupName)
	if result.Err() != nil {
		return nil, fmt.Errorf("failed to get pending messages: %w", result.Err())
	}
	return result.Val(), nil
}

// GetPendingMessagesDetailed gets detailed pending messages with IDs for a consumer group
func (r *RedisStreamsClient) GetPendingMessagesDetailed(groupName string, count int64) ([]redis.XPendingExt, error) {
	result := r.client.XPendingExt(r.ctx, &redis.XPendingExtArgs{
		Stream: r.streamName,
		Group:  groupName,
		Start:  "-",
		End:    "+",
		Count:  count,
	})
	if result.Err() != nil {
		return nil, fmt.Errorf("failed to get detailed pending messages: %w", result.Err())
	}
	return result.Val(), nil
}

// ClaimMessages claims messages that have been pending for too long
func (r *RedisStreamsClient) ClaimMessages(groupName, consumerName string, minIdleTime time.Duration, messageIDs []string) ([]redis.XMessage, error) {
	result := r.client.XClaim(r.ctx, &redis.XClaimArgs{
		Stream:   r.streamName,
		Group:    groupName,
		Consumer: consumerName,
		MinIdle:  minIdleTime,
		Messages: messageIDs,
	})

	if result.Err() != nil {
		return nil, fmt.Errorf("failed to claim messages: %w", result.Err())
	}

	return result.Val(), nil
}

// ParseMessages converts a slice of redis.XMessage to a slice of StreamMessage
func (r *RedisStreamsClient) ParseMessages(messages []redis.XMessage, dataType string) ([]StreamMessage, error) {
	var streamMessages []StreamMessage
	for _, msg := range messages {
		streamMsg, err := r.parseMessage(msg)
		if err != nil {
			return nil, err
		}
		if streamMsg.DataType == "" {
			streamMsg.DataType = dataType
		}
		streamMessages = append(streamMessages, streamMsg)
	}
	return streamMessages, nil
}

// TrimStream trims the stream to keep only recent messages
func (r *RedisStreamsClient) TrimStream(maxLen int64) error {
	result := r.client.XTrimMaxLen(r.ctx, r.streamName, maxLen)
	if result.Err() != nil {
		return fmt.Errorf("failed to trim stream: %w", result.Err())
	}
	return nil
}

// GetStreamInfo gets information about the stream
func (r *RedisStreamsClient) GetStreamInfo() (*redis.XInfoStream, error) {
	result := r.client.XInfoStream(r.ctx, r.streamName)
	if result.Err() != nil {
		return nil, fmt.Errorf("failed to get stream info: %w", result.Err())
	}
	return result.Val(), nil
}

// GetStreamLength gets the length of the stream
func (r *RedisStreamsClient) GetStreamLength() (int64, error) {
	result := r.client.XLen(r.ctx, r.streamName)
	if result.Err() != nil {
		return 0, fmt.Errorf("failed to get stream length: %w", result.Err())
	}
	return result.Val(), nil
}

// DeleteMessage deletes a message from the stream
func (r *RedisStreamsClient) DeleteMessage(messageID string) error {
	result := r.client.XDel(r.ctx, r.streamName, messageID)
	if result.Err() != nil {
		return fmt.Errorf("failed to delete message: %w", result.Err())
	}
	return nil
}

// parseMessage parses a Redis message into a StreamMessage
func (r *RedisStreamsClient) parseMessage(msg redis.XMessage) (StreamMessage, error) {
	streamMsg := StreamMessage{
		ID:   msg.ID,
		Data: make(map[string]interface{}),
	}

	for key, value := range msg.Values {
		switch key {
		case "data_type":
			if dataType, ok := value.(string); ok {
				streamMsg.DataType = dataType
			}

		case "metadata":
			metadataStr, ok := toStringValue(value)
			if !ok || metadataStr == "" {
				continue
			}

			// Store both the raw JSON string and parsed metadata
			streamMsg.MetadataJSON = metadataStr

			var metadata map[string]interface{}
			if err := json.Unmarshal([]byte(metadataStr), &metadata); err != nil {
				return streamMsg, fmt.Errorf("failed to unmarshal metadata: %w", err)
			}
			streamMsg.Metadata = metadata

		case "timestamp":
			streamMsg.Data["timestamp"] = value

		case "data":
			if err := mergeDataField(streamMsg.Data, value); err != nil {
				return streamMsg, err
			}

		default:
			setParsedField(streamMsg.Data, key, value)
		}
	}

	if ts, ok := streamMsg.Data["timestamp"]; ok {
		streamMsg.Data["timestamp"] = normalizeTimestamp(ts)
	}

	return streamMsg, nil
}

func mergeDataField(target map[string]interface{}, raw interface{}) error {
	switch v := raw.(type) {
	case string:
		if v == "" {
			return nil
		}

		var parsed interface{}
		if err := json.Unmarshal([]byte(v), &parsed); err != nil {
			target["data"] = v
			return nil
		}
		mergeParsedData(target, parsed)

	case []byte:
		return mergeDataField(target, string(v))

	case map[string]interface{}:
		for key, val := range v {
			setParsedField(target, key, val)
		}

	default:
		target["data"] = v
	}

	return nil
}

func mergeParsedData(target map[string]interface{}, parsed interface{}) {
	switch data := parsed.(type) {
	case map[string]interface{}:
		for key, val := range data {
			setParsedField(target, key, val)
		}
	default:
		target["data"] = data
	}
}

func setParsedField(target map[string]interface{}, key string, value interface{}) {
	switch v := value.(type) {
	case string:
		if len(v) > 0 && (v[0] == '{' || v[0] == '[') {
			var parsed interface{}
			if err := json.Unmarshal([]byte(v), &parsed); err == nil {
				if key == "timestamp" {
					target[key] = normalizeTimestamp(parsed)
				} else {
					target[key] = parsed
				}
				return
			}
		}
		if key == "timestamp" {
			target[key] = normalizeTimestamp(v)
		} else {
			target[key] = v
		}

	case []byte:
		setParsedField(target, key, string(v))

	default:
		if key == "timestamp" {
			target[key] = normalizeTimestamp(v)
		} else {
			target[key] = v
		}
	}
}

func normalizeTimestamp(value interface{}) interface{} {
	switch v := value.(type) {
	case time.Time:
		return v

	case string:
		trimmed := strings.TrimSpace(v)
		if idx := strings.Index(trimmed, " m="); idx != -1 {
			trimmed = strings.TrimSpace(trimmed[:idx])
		}

		layouts := []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02 15:04:05.999999999 -0700 MST",
			"2006-01-02 15:04:05.999999999 Z07:00",
			"2006-01-02 15:04:05.999999999",
		}

		for _, layout := range layouts {
			if parsed, err := time.Parse(layout, trimmed); err == nil {
				return parsed
			}
		}

		if unix, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
			return time.Unix(unix, 0)
		}
		return trimmed

	case int64:
		return time.Unix(v, 0)

	case int:
		return time.Unix(int64(v), 0)

	case float64:
		return time.Unix(int64(v), 0)

	case json.Number:
		if unix, err := v.Int64(); err == nil {
			return time.Unix(unix, 0)
		}
		if f, err := v.Float64(); err == nil {
			return time.Unix(int64(f), 0)
		}
		return v

	default:
		return v
	}
}

func toStringValue(value interface{}) (string, bool) {
	switch v := value.(type) {
	case string:
		return v, true
	case []byte:
		return string(v), true
	default:
		return "", false
	}
}

// Close closes the Redis connection
func (r *RedisStreamsClient) Close() error {
	return r.client.Close()
}

// Ping tests the connection to Redis
func (r *RedisStreamsClient) Ping() error {
	return r.client.Ping(r.ctx).Err()
}

// GetClient returns the underlying Redis client
func (r *RedisStreamsClient) GetClient() *redis.Client {
	return r.client
}

// RedisStreamStats provides statistics about the Redis Stream
type RedisStreamStats struct {
	StreamName      string          `json:"stream_name"`
	Length          int64           `json:"length"`
	RadixTreeKeys   int64           `json:"radix_tree_keys"`
	RadixTreeNodes  int64           `json:"radix_tree_nodes"`
	Groups          int64           `json:"groups"`
	LastGeneratedID string          `json:"last_generated_id"`
	FirstEntry      *redis.XMessage `json:"first_entry,omitempty"`
	LastEntry       *redis.XMessage `json:"last_entry,omitempty"`
}

// GetStats returns statistics about the Redis Stream
func (r *RedisStreamsClient) GetStats() (*RedisStreamStats, error) {
	info, err := r.GetStreamInfo()
	if err != nil {
		return nil, err
	}

	stats := &RedisStreamStats{
		StreamName:      r.streamName,
		Length:          info.Length,
		RadixTreeKeys:   info.RadixTreeKeys,
		RadixTreeNodes:  info.RadixTreeNodes,
		Groups:          info.Groups,
		LastGeneratedID: info.LastGeneratedID,
	}

	if len(info.FirstEntry.Values) > 0 {
		stats.FirstEntry = &info.FirstEntry
	}
	if len(info.LastEntry.Values) > 0 {
		stats.LastEntry = &info.LastEntry
	}

	return stats, nil
}

// BatchAddMessages adds multiple messages to the stream in a pipeline
func (r *RedisStreamsClient) BatchAddMessages(messages []StreamMessage) ([]string, error) {
	pipe := r.client.Pipeline()

	var cmds []*redis.StringCmd
	for _, msg := range messages {
		fields := map[string]interface{}{
			"data_type": msg.DataType,
			"timestamp": time.Now().Unix(),
		}

		// Serialize data as JSON
		dataJSON, err := json.Marshal(msg.Data)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal data: %w", err)
		}
		fields["data"] = string(dataJSON)

		// Serialize metadata as JSON if present
		if len(msg.Metadata) > 0 {
			metadataJSON, err := json.Marshal(msg.Metadata)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal metadata: %w", err)
			}
			fields["metadata"] = string(metadataJSON)
		}

		cmd := pipe.XAdd(r.ctx, &redis.XAddArgs{
			Stream: r.streamName,
			Values: fields,
		})
		cmds = append(cmds, cmd)
	}

	_, err := pipe.Exec(r.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute batch add: %w", err)
	}

	var messageIDs []string
	for _, cmd := range cmds {
		if cmd.Err() != nil {
			return nil, fmt.Errorf("failed to add message in batch: %w", cmd.Err())
		}
		messageIDs = append(messageIDs, cmd.Val())
	}

	return messageIDs, nil
}

// SetStreamMaxLength sets the maximum length for the stream with automatic trimming
func (r *RedisStreamsClient) SetStreamMaxLength(maxLen int64) error {
	// This is typically done at the application level by calling XTrimMaxLen periodically
	// or by using the MAXLEN option in XADD commands
	return r.TrimStream(maxLen)
}

// GetConsumerGroupInfo gets information about consumer groups
func (r *RedisStreamsClient) GetConsumerGroupInfo() ([]redis.XInfoGroup, error) {
	result := r.client.XInfoGroups(r.ctx, r.streamName)
	if result.Err() != nil {
		return nil, fmt.Errorf("failed to get consumer group info: %w", result.Err())
	}
	return result.Val(), nil
}

// GetConsumerInfo gets information about consumers in a group
func (r *RedisStreamsClient) GetConsumerInfo(groupName string) ([]redis.XInfoConsumer, error) {
	result := r.client.XInfoConsumers(r.ctx, r.streamName, groupName)
	if result.Err() != nil {
		return nil, fmt.Errorf("failed to get consumer info: %w", result.Err())
	}
	return result.Val(), nil
}
