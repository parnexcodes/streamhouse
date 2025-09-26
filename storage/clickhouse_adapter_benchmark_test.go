package storage

import (
	"testing"
	"time"
)

func BenchmarkConvertEvent(b *testing.B) {
	adapter := &ClickHouseAdapter{}

	event := &StreamEvent{
		ID:        "benchmark",
		Timestamp: time.Now(),
		DataType:  "benchmark_event",
		Data: map[string]interface{}{
			"user_id":     "user-1",
			"session_id":  "session-1",
			"success":     true,
			"duration_ms": 123.45,
			"count":       42,
			"score":       98.7,
			"metadata": map[string]interface{}{
				"source": "benchmark",
			},
		},
		Metadata: map[string]interface{}{
			"region": "us-east-1",
		},
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		adapter.convertEventToMap(event)
	}
}
