package streamhouse

import (
	"testing"
)

func BenchmarkProcessBatch(b *testing.B) {
	env, cleanup := setupBenchmarkClient(b)
	defer cleanup()

	schema := registerBenchmarkSchema(b, env.client)

	consumer, err := NewConsumer(env.client, env.client.config.Consumer)
	if err != nil {
		b.Fatalf("failed to create consumer: %v", err)
	}
	consumer.clickHouse = env.clickhouse
	consumer.errorHandler = func(error) {}

	batchSize := int(env.client.config.BatchSize)
	messages := generateBenchmarkMessages(schema.Name, batchSize)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		consumer.processBatch(messages)
	}

	_ = consumer.Stop()
}
