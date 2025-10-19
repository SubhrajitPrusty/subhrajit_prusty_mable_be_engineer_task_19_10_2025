package metrics

import (
	"context"
	"testing"
	"time"
)

func TestMetricsCollector(t *testing.T) {
	ctx := context.Background()

	// Create collector
	collector, err := NewMetricsCollector(ctx)
	if err != nil {
		t.Skipf("Skipping test as database is not available: %v", err)
	}
	defer collector.Close()

	// Test single metric recording
	metric := Metric{
		Timestamp:      time.Now(),
		PipelineID:     "test-pipeline",
		EventCount:     10,
		ProcessingTime: time.Second,
		MemoryUsage:    1024 * 1024,
		BatchSize:      5,
		WorkerCount:    2,
		ErrorCount:     0,
	}

	collector.Record(metric)

	// Test buffer threshold flush
	for i := 0; i < collector.bufferSize+1; i++ {
		collector.Record(Metric{
			Timestamp:      time.Now(),
			PipelineID:     "test-pipeline",
			EventCount:     int64(i),
			ProcessingTime: time.Millisecond * time.Duration(i),
			MemoryUsage:    uint64(i * 1024),
			BatchSize:      i,
			WorkerCount:    2,
			ErrorCount:     0,
		})
	}

	// Test time threshold flush
	metric.Timestamp = time.Now()
	collector.Record(metric)
	time.Sleep(30 * time.Second) // Wait for the flush interval
	collector.Record(metric)

	// Test manual flush
	if err := collector.Flush(); err != nil {
		t.Errorf("Failed to flush metrics: %v", err)
	}
}

func TestMetricsCollectorConcurrency(t *testing.T) {
	ctx := context.Background()

	// Create collector
	collector, err := NewMetricsCollector(ctx)
	if err != nil {
		t.Skipf("Skipping test as database is not available: %v", err)
	}
	defer collector.Close()

	// Test concurrent recording
	concurrency := 10
	metricsPerGoroutine := 100
	done := make(chan bool)

	for i := 0; i < concurrency; i++ {
		go func(id int) {
			for j := 0; j < metricsPerGoroutine; j++ {
				collector.Record(Metric{
					Timestamp:      time.Now(),
					PipelineID:     "test-pipeline",
					EventCount:     int64(j),
					ProcessingTime: time.Millisecond * time.Duration(j),
					MemoryUsage:    uint64(j * 1024),
					BatchSize:      j,
					WorkerCount:    2,
					ErrorCount:     0,
				})
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < concurrency; i++ {
		<-done
	}

	// Final flush
	if err := collector.Flush(); err != nil {
		t.Errorf("Failed to flush metrics: %v", err)
	}
}
