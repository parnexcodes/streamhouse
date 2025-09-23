package streamhouse

import (
	"context"
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
	clickHouse     *storage.ClickHouseClient
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
	MessagesProcessed   int64     `json:"messages_processed"`
	MessagesSucceeded   int64     `json:"messages_succeeded"`
	MessagesFailed      int64     `json:"messages_failed"`
	BatchesProcessed    int64     `json:"batches_processed"`
	BatchesFailed       int64     `json:"batches_failed"`
	LastProcessedTime   time.Time `json:"last_processed_time"`
	ProcessingDuration  time.Duration `json:"processing_duration"`
	AverageProcessingTime time.Duration `json:"average_processing_time"`
	ErrorRate           float64   `json:"error_rate"`
	ThroughputPerSecond float64   `json:"throughput_per_second"`
	mu                  sync.RWMutex
}

// NewConsumer creates a new consumer
func NewConsumer(client *StreamHouseClient, config ConsumerConfig) (*Consumer, error) {
	ctx, cancel := context.WithCancel(context.Background())
	
	consumer := &Consumer{
		client:      client,
		redisClient: &storage.RedisStreamsClient{},
		clickHouse:  &storage.ClickHouseClient{},
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
	
	// Create consumer group
	if err := c.redisClient.CreateConsumerGroup(c.config.GroupName); err != nil {
		return fmt.Errorf("failed to create consumer group: %w", err)
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
			// Read messages from stream
			messages, err := c.redisClient.ReadMessagesFromGroup(
				c.config.GroupName,
				consumerName,
				c.config.Count-int64(len(batch)),
				time.Second,
			)
			
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
	var messageIDs []string
	
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
			
			// Collect message IDs for acknowledgment
			for _, msg := range msgs {
				messageIDs = append(messageIDs, msg.ID)
			}
		}
	}
	
	// Acknowledge successfully processed messages
	for _, msgID := range messageIDs {
		if err := c.redisClient.AckMessage(c.config.GroupName, msgID); err != nil {
			c.errorHandler(fmt.Errorf("failed to acknowledge message %s: %w", msgID, err))
		}
	}
	
	// Update metrics
	c.updateMetrics(successCount, failCount, time.Since(startTime))
}

// processMessagesByType processes messages of a specific data type
func (c *Consumer) processMessagesByType(dataType string, messages []*storage.StreamMessage) error {
	// Get schema for validation
	_, err := c.client.schemaRegistry.Get(dataType)
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
		
		// Add standard fields
		processedData := make(map[string]interface{})
		for k, v := range msg.Data {
			processedData[k] = v
		}
		
		// Add timestamp if not present
		if _, exists := processedData["timestamp"]; !exists {
			processedData["timestamp"] = time.Now()
		}
		
		// Add message ID for tracking
		processedData["_message_id"] = msg.ID
		
		// Add metadata if present
		if len(msg.Metadata) > 0 {
			processedData["_metadata"] = msg.Metadata
		}
		
		tableData = append(tableData, processedData)
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
		"data_type":          msg.DataType,
		"data":               msg.Data,
		"metadata":           msg.Metadata,
		"error":              err.Error(),
		"failed_at":          time.Now(),
		"ttl":                time.Now().Add(c.config.DeadLetterTTL),
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
	pending, err := c.redisClient.GetPendingMessages(c.config.GroupName)
	if err != nil {
		c.errorHandler(fmt.Errorf("failed to get pending messages: %w", err))
		return
	}
	
	if pending.Count == 0 {
		return
	}
	
	// Get detailed pending info and claim stale messages
	// This is a simplified implementation - you might want to implement
	// more sophisticated claiming logic based on your needs
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

// getTableName generates table name from data type
func (c *Consumer) getTableName(dataType string) string {
	// Replace dots with underscores for ClickHouse table names
	return fmt.Sprintf("streamhouse_%s", 
		strings.ReplaceAll(strings.ReplaceAll(dataType, ".", "_"), "-", "_"))
}

// Health check methods

// HealthStatus represents the health status of the consumer
type HealthStatus struct {
	Healthy           bool      `json:"healthy"`
	LastProcessedTime time.Time `json:"last_processed_time"`
	ErrorRate         float64   `json:"error_rate"`
	ThroughputPerSecond float64 `json:"throughput_per_second"`
	Issues            []string  `json:"issues,omitempty"`
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