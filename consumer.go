package streamhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/parnexcodes/streamhouse/storage"
)

// Consumer handles background processing of stream messages
type Consumer struct {
	client         *StreamHouseClient
	redisClient    *storage.RedisStreamsClient
	clickHouse     ClickHouseConnection
	config         ConsumerConfig
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	running        bool
	mu             sync.RWMutex
	metrics        *ConsumerMetrics
	errorHandler   func(error)
	messageHandler func(*storage.StreamMessage) error
}

// DefaultConsumerConfig returns default consumer configuration
func DefaultConsumerConfig() ConsumerConfig {
	return ConsumerConfig{
		GroupName:       "streamhouse-consumer",
		ConsumerName:    "consumer-1",
		BlockTime:       5 * time.Second,
		Count:           100,
		NoAck:           false,
		RetryAttempts:   3,
		RetryDelay:      time.Second,
		DeadLetterQueue: true,
	}
}

// ConsumerMetrics tracks consumer performance metrics
type ConsumerMetrics struct {
	MessagesProcessed     int64         `json:"messages_processed"`
	MessagesSucceeded     int64         `json:"messages_succeeded"`
	MessagesFailed        int64         `json:"messages_failed"`
	BatchesProcessed      int64         `json:"batches_processed"`
	BatchesFailed         int64         `json:"batches_failed"`
	LastProcessedTime     time.Time     `json:"last_processed_time"`
	ProcessingDuration    time.Duration `json:"processing_duration"`
	AverageProcessingTime time.Duration `json:"average_processing_time"`
	ErrorRate             float64       `json:"error_rate"`
	ThroughputPerSecond   float64       `json:"throughput_per_second"`
	mu                    sync.RWMutex
}

// NewConsumer creates a new consumer
func NewConsumer(client *StreamHouseClient, config ConsumerConfig) (*Consumer, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Create RedisStreamsClient with the Redis client from StreamHouseClient
	redisClient := &storage.RedisStreamsClient{}
	if err := redisClient.InitWithClient(client.redisClient, client.config.StreamName, ctx); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to initialize Redis streams client: %w", err)
	}

	consumer := &Consumer{
		client:      client,
		redisClient: redisClient,
		clickHouse:  client.clickHouseConn, // Use the same ClickHouse connection as the main client
		config:      config,
		ctx:         ctx,
		cancel:      cancel,
		metrics:     &ConsumerMetrics{},
		running:     false,
	}

	// Set default error handler
	consumer.errorHandler = func(err error) {
		log.Printf("Consumer error: %v", err)
	}

	return consumer, nil
}

// SetErrorHandler sets a custom error handler
func (c *Consumer) SetErrorHandler(handler func(error)) {
	c.errorHandler = handler
}

// SetMessageHandler sets a custom message handler
func (c *Consumer) SetMessageHandler(handler func(*storage.StreamMessage) error) {
	c.messageHandler = handler
}

// Start starts the consumer
func (c *Consumer) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.running {
		return fmt.Errorf("consumer is already running")
	}

	// Create consumer groups for all streams FIRST
	if err := c.createConsumerGroupsForAllStreams(); err != nil {
		return fmt.Errorf("failed to create consumer groups: %w", err)
	}

	c.running = true

	// Start worker goroutines
	for i := 0; i < c.client.config.Workers; i++ {
		c.wg.Add(1)
		go c.worker(fmt.Sprintf("%s-%d", c.config.ConsumerName, i))
	}

	// Start claim worker for handling stale messages
	c.wg.Add(1)
	go c.claimWorker()

	// Start metrics updater
	c.wg.Add(1)
	go c.metricsUpdater()

	return nil
}

// Stop stops the consumer
func (c *Consumer) Stop() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.running {
		return nil
	}

	c.running = false
	c.cancel()
	c.wg.Wait()

	return nil
}

// IsRunning returns whether the consumer is running
func (c *Consumer) IsRunning() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.running
}

// worker processes messages from Redis Streams
func (c *Consumer) worker(consumerName string) {
	defer c.wg.Done()

	batch := make([]*storage.StreamMessage, 0, int(c.config.Count))
	lastFlush := time.Now()
	flushInterval := 5 * time.Second // Default flush interval since it's not in config

	for {
		select {
		case <-c.ctx.Done():
			// Process remaining batch before exiting
			if len(batch) > 0 {
				c.processBatch(batch)
			}
			return
		default:
			// Read messages from all streams
			messages, err := c.readMessagesFromAllStreams(consumerName, c.config.Count-int64(len(batch)))

			if err != nil {
				c.errorHandler(fmt.Errorf("failed to read messages: %w", err))
				time.Sleep(c.config.RetryDelay)
				continue
			}

			// Add messages to batch
			for i := range messages {
				batch = append(batch, &messages[i])
			}

			// Process batch if it's full or flush interval has passed
			shouldFlush := len(batch) >= int(c.config.Count) ||
				time.Since(lastFlush) >= flushInterval

			if shouldFlush && len(batch) > 0 {
				c.processBatch(batch)
				batch = batch[:0] // Clear batch
				lastFlush = time.Now()
			}
		}
	}
}

// processBatch processes a batch of messages
func (c *Consumer) processBatch(messages []*storage.StreamMessage) {
	startTime := time.Now()

	// Group messages by data type
	messagesByType := make(map[string][]*storage.StreamMessage)
	for _, msg := range messages {
		messagesByType[msg.DataType] = append(messagesByType[msg.DataType], msg)
	}

	var successCount, failCount int64
	messageIDsByStream := make(map[string][]string)

	// Process each data type separately
	for dataType, msgs := range messagesByType {
		if err := c.processMessagesByType(dataType, msgs); err != nil {
			c.errorHandler(fmt.Errorf("failed to process messages for type %s: %w", dataType, err))
			failCount += int64(len(msgs))

			// Handle failed messages
			for _, msg := range msgs {
				c.handleFailedMessage(msg, err)
			}
		} else {
			successCount += int64(len(msgs))

			// Collect message IDs per stream for acknowledgment
			streamName := c.client.getStreamKey(dataType)
			messageIDsByStream[streamName] = append(messageIDsByStream[streamName], getMessageIDs(msgs)...)
		}
	}

	// Acknowledge successfully processed messages for each stream
	for streamName, ids := range messageIDsByStream {
		tempClient := &storage.RedisStreamsClient{}
		if err := tempClient.InitWithClient(c.redisClient.GetClient(), streamName, c.ctx); err != nil {
			c.errorHandler(fmt.Errorf("failed to init redis client for ack on stream %s: %w", streamName, err))
			continue
		}

		for _, msgID := range ids {
			if err := tempClient.AckMessage(c.config.GroupName, msgID); err != nil {
				c.errorHandler(fmt.Errorf("failed to acknowledge message %s on stream %s: %w", msgID, streamName, err))
			}
		}
	}

	// Update metrics
	c.updateMetrics(successCount, failCount, time.Since(startTime))
}

func getMessageIDs(messages []*storage.StreamMessage) []string {
	ids := make([]string, 0, len(messages))
	for _, msg := range messages {
		ids = append(ids, msg.ID)
	}
	return ids
}

// processMessagesByType processes messages of a specific data type
func (c *Consumer) processMessagesByType(dataType string, messages []*storage.StreamMessage) error {
	// Get schema for validation and conversion
	schema, err := c.client.schemaRegistry.Get(dataType)
	if err != nil {
		return fmt.Errorf("schema not found for data type: %s", dataType)
	}

	// Prepare data for ClickHouse insertion
	var tableData []map[string]interface{}

	for _, msg := range messages {
		// Use custom message handler if set
		if c.messageHandler != nil {
			if err := c.messageHandler(msg); err != nil {
				return fmt.Errorf("custom message handler failed: %w", err)
			}
			continue
		}

		// Validate message data
		if err := c.client.schemaRegistry.ValidateData(dataType, msg.Data); err != nil {
			return fmt.Errorf("data validation failed: %w", err)
		}

		// Normalize data for ClickHouse insertion
		row, err := c.normalizeClickHouseRow(schema, dataType, msg)
		if err != nil {
			return fmt.Errorf("failed to normalize data for ClickHouse: %w", err)
		}

		tableData = append(tableData, row)
	}

	// Skip ClickHouse insertion if using custom handler
	if c.messageHandler != nil {
		return nil
	}

	// Insert data into ClickHouse
	tableName := c.getTableName(dataType)
	if err := c.clickHouse.InsertData(tableName, tableData); err != nil {
		return fmt.Errorf("failed to insert data into ClickHouse: %w", err)
	}

	return nil
}

func (c *Consumer) normalizeClickHouseRow(schema *DataSchema, dataType string, msg *storage.StreamMessage) (map[string]interface{}, error) {
	row := make(map[string]interface{}, len(schema.Fields)+1)

	for fieldName, fieldConfig := range schema.Fields {
		rawValue, exists := msg.Data[fieldName]
		if !exists {
			if fieldConfig.Default == nil {
				continue
			}
			rawValue = fieldConfig.Default
		}

		convertedValue, err := c.client.schemaRegistry.ConvertValue(rawValue, fieldConfig.Type)
		if err != nil {
			return nil, fmt.Errorf("field %s conversion failed: %w", fieldName, err)
		}

		switch fieldConfig.Type {
		case FieldTypeJSON:
			if convertedValue == nil {
				continue
			}
			if str, ok := convertedValue.(string); ok {
				row[fieldName] = str
				break
			}
			jsonBytes, err := json.Marshal(convertedValue)
			if err != nil {
				return nil, fmt.Errorf("field %s json marshal failed: %w", fieldName, err)
			}
			row[fieldName] = string(jsonBytes)

		case FieldTypeDatetime:
			switch v := convertedValue.(type) {
			case time.Time:
				row[fieldName] = v
			default:
				row[fieldName] = convertedValue
			}

		default:
			row[fieldName] = convertedValue
		}
	}

	if _, ok := row["id"]; !ok {
		row["id"] = msg.ID
	}

	if ts, ok := row["timestamp"]; ok {
		if t, ok := ts.(time.Time); ok {
			row["timestamp"] = t
		} else {
			row["timestamp"] = time.Now()
		}
	} else {
		row["timestamp"] = time.Now()
	}

	row["data_type"] = dataType

	return row, nil
}

// handleFailedMessage handles a message that failed processing
func (c *Consumer) handleFailedMessage(msg *storage.StreamMessage, err error) {
	// Implement retry logic
	retryCount := c.getRetryCount(msg)

	if retryCount < c.config.RetryAttempts {
		// Increment retry count and requeue
		c.setRetryCount(msg, retryCount+1)
		// Message will be automatically retried by Redis Streams
		return
	}

	// Move to dead letter queue if enabled
	if c.config.DeadLetterQueue {
		c.moveToDeadLetter(msg, err)
	}
}

// getRetryCount gets the retry count from message metadata
func (c *Consumer) getRetryCount(msg *storage.StreamMessage) int {
	if msg.Metadata == nil {
		return 0
	}

	if count, ok := msg.Metadata["retry_count"].(int); ok {
		return count
	}

	return 0
}

// setRetryCount sets the retry count in message metadata
func (c *Consumer) setRetryCount(msg *storage.StreamMessage, count int) {
	if msg.Metadata == nil {
		msg.Metadata = make(map[string]interface{})
	}
	msg.Metadata["retry_count"] = count
}

// moveToDeadLetter moves a message to the dead letter queue
func (c *Consumer) moveToDeadLetter(msg *storage.StreamMessage, err error) {
	deadLetterData := map[string]interface{}{
		"original_message_id": msg.ID,
		"data_type":           msg.DataType,
		"data":                msg.Data,
		"metadata":            msg.Metadata,
		"error":               err.Error(),
		"failed_at":           time.Now(),
		"ttl":                 time.Now().Add(c.config.DeadLetterTTL),
	}

	// Add to dead letter stream (you might want to use a separate Redis stream)
	deadLetterStream := fmt.Sprintf("%s:dead_letter", c.client.config.StreamName)
	// Implementation would depend on your dead letter strategy
	_ = deadLetterData
	_ = deadLetterStream
}

// claimWorker handles claiming of stale messages
func (c *Consumer) claimWorker() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.config.ClaimInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.claimStaleMessages()
		}
	}
}

// claimStaleMessages claims messages that have been pending for too long
func (c *Consumer) claimStaleMessages() {
	// Get all streams to check pending messages for each one
	streams, err := c.getStreamNames()
	if err != nil {
		c.errorHandler(fmt.Errorf("failed to get stream names: %w", err))
		return
	}

	for _, streamName := range streams {
		// Create a temporary client for this stream
		tempClient := &storage.RedisStreamsClient{}
		if err := tempClient.InitWithClient(c.redisClient.GetClient(), streamName, c.ctx); err != nil {
			continue // Skip this stream if we can't create a client
		}

		pending, err := tempClient.GetPendingMessages(c.config.GroupName)
		if err != nil {
			// Check if it's a NOGROUP error - this is expected if the stream/group doesn't exist yet
			if strings.Contains(err.Error(), "NOGROUP") {
				// This is expected during startup, don't log as error
				continue
			}
			c.errorHandler(fmt.Errorf("failed to get pending messages for stream %s: %w", streamName, err))
			continue
		}

		if pending.Count == 0 {
			continue
		}

		// Get detailed pending info and claim stale messages
		// Claim messages that have been pending for more than the claim interval
		minIdleTime := c.config.ClaimInterval

		// Get detailed pending messages with IDs
		pendingDetails, err := tempClient.GetPendingMessagesDetailed(c.config.GroupName, 100) // Limit to 100 messages
		if err != nil {
			if strings.Contains(err.Error(), "NOGROUP") {
				continue
			}
			c.errorHandler(fmt.Errorf("failed to get pending details for stream %s: %w", streamName, err))
			continue
		}

		// If we have pending messages, try to claim stale ones
		if len(pendingDetails) > 0 {
			// Create a consumer name for claiming (use the first worker's name)
			claimConsumerName := fmt.Sprintf("%s-0", c.config.ConsumerName)

			// Filter messages that have been idle for too long
			var staleMessageIDs []string
			for _, pending := range pendingDetails {
				// Check if the message has been idle for longer than the claim interval
				// The Idle field represents time since last delivery
				if pending.Idle > minIdleTime {
					staleMessageIDs = append(staleMessageIDs, pending.ID)
				}
			}

			// Claim stale messages if any
			if len(staleMessageIDs) > 0 {
				claimedMessages, err := tempClient.ClaimMessages(
					c.config.GroupName,
					claimConsumerName,
					minIdleTime,
					staleMessageIDs,
				)

				if err != nil {
					if strings.Contains(err.Error(), "NOGROUP") {
						continue
					}
					c.errorHandler(fmt.Errorf("failed to claim messages for stream %s: %w", streamName, err))
					continue
				}

				// Process claimed messages
				if len(claimedMessages) > 0 {
					log.Printf("Claimed %d stale messages from stream %s", len(claimedMessages), streamName)

					dataType := c.getDataTypeFromStreamName(streamName)
					if dataType == "" {
						c.errorHandler(fmt.Errorf("could not determine data type from stream name: %s", streamName))
						continue
					}

					parsedMessages, err := tempClient.ParseMessages(claimedMessages, dataType)
					if err != nil {
						c.errorHandler(fmt.Errorf("failed to parse claimed messages: %w", err))
						continue
					}

					// Process claimed messages similarly to regular messages
					// This will handle retries and dead-letter queueing
					messagesToProcess := make([]*storage.StreamMessage, len(parsedMessages))
					for i := range parsedMessages {
						messagesToProcess[i] = &parsedMessages[i]
					}
					c.processBatch(messagesToProcess)
				}
			}
		}
	}
}

// getDataTypeFromStreamName extracts the data type from the stream name
func (c *Consumer) getDataTypeFromStreamName(streamName string) string {
	parts := strings.Split(streamName, ":")
	if len(parts) > 1 {
		return strings.Join(parts[1:], ":")
	}
	return ""
}

// metricsUpdater updates consumer metrics periodically
func (c *Consumer) metricsUpdater() {
	defer c.wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.updateDerivedMetrics()
		}
	}
}

// updateMetrics updates processing metrics
func (c *Consumer) updateMetrics(succeeded, failed int64, duration time.Duration) {
	c.metrics.mu.Lock()
	defer c.metrics.mu.Unlock()

	c.metrics.MessagesProcessed += succeeded + failed
	c.metrics.MessagesSucceeded += succeeded
	c.metrics.MessagesFailed += failed
	c.metrics.BatchesProcessed++

	if failed > 0 {
		c.metrics.BatchesFailed++
	}

	c.metrics.LastProcessedTime = time.Now()
	c.metrics.ProcessingDuration += duration
}

// updateDerivedMetrics updates derived metrics like error rate and throughput
func (c *Consumer) updateDerivedMetrics() {
	c.metrics.mu.Lock()
	defer c.metrics.mu.Unlock()

	if c.metrics.MessagesProcessed > 0 {
		c.metrics.ErrorRate = float64(c.metrics.MessagesFailed) / float64(c.metrics.MessagesProcessed)
	}

	if c.metrics.BatchesProcessed > 0 {
		avgDuration := c.metrics.ProcessingDuration / time.Duration(c.metrics.BatchesProcessed)
		c.metrics.AverageProcessingTime = avgDuration
	}

	// Calculate throughput (messages per second over last minute)
	// This is a simplified calculation - you might want to use a sliding window
	if !c.metrics.LastProcessedTime.IsZero() {
		elapsed := time.Since(c.metrics.LastProcessedTime)
		if elapsed > 0 {
			c.metrics.ThroughputPerSecond = float64(c.metrics.MessagesSucceeded) / elapsed.Seconds()
		}
	}
}

// GetMetrics returns current consumer metrics
func (c *Consumer) GetMetrics() ConsumerMetrics {
	c.metrics.mu.RLock()
	defer c.metrics.mu.RUnlock()

	// Return a copy to avoid race conditions
	return ConsumerMetrics{
		MessagesProcessed:     c.metrics.MessagesProcessed,
		MessagesSucceeded:     c.metrics.MessagesSucceeded,
		MessagesFailed:        c.metrics.MessagesFailed,
		BatchesProcessed:      c.metrics.BatchesProcessed,
		BatchesFailed:         c.metrics.BatchesFailed,
		LastProcessedTime:     c.metrics.LastProcessedTime,
		ProcessingDuration:    c.metrics.ProcessingDuration,
		AverageProcessingTime: c.metrics.AverageProcessingTime,
		ErrorRate:             c.metrics.ErrorRate,
		ThroughputPerSecond:   c.metrics.ThroughputPerSecond,
	}
}

// createConsumerGroupsForAllStreams creates consumer groups for all streams
func (c *Consumer) createConsumerGroupsForAllStreams() error {
	// Get all streams that match the pattern
	streams, err := c.getStreamNames()
	if err != nil {
		return fmt.Errorf("failed to get stream names: %w", err)
	}

	// Create consumer group for each stream
	for _, streamName := range streams {
		if err := c.createConsumerGroupForStream(streamName); err != nil {
			return fmt.Errorf("failed to create consumer group for stream %s: %w", streamName, err)
		}
	}

	return nil
}

// getStreamNames gets all stream names that match the pattern
func (c *Consumer) getStreamNames() ([]string, error) {
	// Use the registered schemas to determine stream names
	var streams []string
	for _, schemaName := range c.client.schemaRegistry.List() {
		streamName := c.client.getStreamKey(schemaName)
		streams = append(streams, streamName)
	}

	return streams, nil
}

// createConsumerGroupForStream creates a consumer group for a specific stream
func (c *Consumer) createConsumerGroupForStream(streamName string) error {
	// We need to create a temporary RedisStreamsClient for this specific stream
	tempClient := &storage.RedisStreamsClient{}
	if err := tempClient.InitWithClient(c.redisClient.GetClient(), streamName, c.ctx); err != nil {
		return err
	}

	// Try to create the consumer group
	err := tempClient.CreateConsumerGroup(c.config.GroupName)
	if err != nil {
		// If the stream doesn't exist yet, that's okay - the consumer group will be created
		// when the first message is added to the stream
		if strings.Contains(err.Error(), "NOGROUP") || strings.Contains(err.Error(), "BUSYGROUP") {
			// These are expected errors - stream doesn't exist yet or group already exists
			return nil
		}
		return err
	}

	return nil
}

// readMessagesFromAllStreams reads messages from all streams
func (c *Consumer) readMessagesFromAllStreams(consumerName string, count int64) ([]storage.StreamMessage, error) {
	streams, err := c.getStreamNames()
	if err != nil {
		return nil, err
	}

	var allMessages []storage.StreamMessage
	for _, streamName := range streams {
		// Create a temporary client for this stream
		tempClient := &storage.RedisStreamsClient{}
		if err := tempClient.InitWithClient(c.redisClient.GetClient(), streamName, c.ctx); err != nil {
			continue // Skip this stream if we can't create a client
		}

		// Read messages from this stream
		messages, err := tempClient.ReadMessagesFromGroup(
			c.config.GroupName,
			consumerName,
			count,
			time.Second,
		)
		if err != nil {
			// Check if it's a NOGROUP error - this is expected if the stream/group doesn't exist yet
			if strings.Contains(err.Error(), "NOGROUP") {
				// This is expected during startup, don't log as error
				continue
			}
			// Log error but continue with other streams
			c.errorHandler(fmt.Errorf("failed to read from stream %s: %w", streamName, err))
			continue
		}

		allMessages = append(allMessages, messages...)
	}

	return allMessages, nil
}

// getTableName generates table name from data type
func (c *Consumer) getTableName(dataType string) string {
	// Replace dots with underscores for ClickHouse table names
	return fmt.Sprintf("streamhouse_%s",
		strings.ReplaceAll(strings.ReplaceAll(dataType, ".", "_"), "-", "_"))
}

// Health check methods

// HealthStatus represents the health status of the consumer
type HealthStatus struct {
	Healthy             bool      `json:"healthy"`
	LastProcessedTime   time.Time `json:"last_processed_time"`
	ErrorRate           float64   `json:"error_rate"`
	ThroughputPerSecond float64   `json:"throughput_per_second"`
	Issues              []string  `json:"issues,omitempty"`
}

// GetHealthStatus returns the current health status
func (c *Consumer) GetHealthStatus() HealthStatus {
	metrics := c.GetMetrics()

	status := HealthStatus{
		Healthy:             true,
		LastProcessedTime:   metrics.LastProcessedTime,
		ErrorRate:           metrics.ErrorRate,
		ThroughputPerSecond: metrics.ThroughputPerSecond,
		Issues:              []string{},
	}

	// Check for health issues
	if metrics.ErrorRate > 0.1 { // More than 10% error rate
		status.Healthy = false
		status.Issues = append(status.Issues, fmt.Sprintf("High error rate: %.2f%%", metrics.ErrorRate*100))
	}

	if !metrics.LastProcessedTime.IsZero() && time.Since(metrics.LastProcessedTime) > 5*time.Minute {
		status.Healthy = false
		status.Issues = append(status.Issues, "No messages processed in the last 5 minutes")
	}

	if metrics.ThroughputPerSecond < 1 && metrics.MessagesProcessed > 100 {
		status.Issues = append(status.Issues, "Low throughput detected")
	}

	return status
}
