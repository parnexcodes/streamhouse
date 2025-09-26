package streamhouse

import (
	"testing"
)

func BenchmarkStream(b *testing.B) {
	env, cleanup := setupBenchmarkClient(b)
	defer cleanup()

	schema := registerBenchmarkSchema(b, env.client)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		event := generateBenchmarkEvent(i)
		if err := env.client.Stream(schema.Name, event); err != nil {
			b.Fatalf("stream failed: %v", err)
		}
	}
}

func BenchmarkStreamAsync(b *testing.B) {
	env, cleanup := setupBenchmarkClient(b)
	defer cleanup()

	schema := registerBenchmarkSchema(b, env.client)

	done := make(chan struct{})
	env.client.wg.Add(1)
	go func() {
		env.client.processAsyncEvents()
		close(done)
	}()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		env.client.StreamAsync(schema.Name, generateBenchmarkEvent(i))
	}

	env.client.cancel()
	<-done
	env.client.wg.Wait()
}
