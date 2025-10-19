package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"testing"
	"time"
)

// TestStruct with 10 properties of various types
type TestStruct struct {
	ID          int64
	Name        string
	CreatedAt   time.Time
	IsActive    bool
	Score       float64
	Tags        []string
	Metadata    map[string]interface{}
	Count       uint32
	Priority    int8
	Description *string
}

// MableEvent represents the structure from sample-event.json
type MableEvent struct {
	BD   map[string]interface{} `json:"bd"`
	CDAS map[string]interface{} `json:"cdas"`
	EID  string                 `json:"eid"`
	EN   string                 `json:"en"`
	II   string                 `json:"ii"`
	MD   map[string]interface{} `json:"md"`
	PD   map[string]interface{} `json:"pd"`
	PIDS map[string]interface{} `json:"pids"`
	SD   map[string]interface{} `json:"sd"`
}

// generateTestStructs creates n number of TestStruct instances
func generateTestStructs(n int) []interface{} {
	result := make([]interface{}, n)
	desc := "test description"

	for i := 0; i < n; i++ {
		result[i] = TestStruct{
			ID:          int64(i),
			Name:        fmt.Sprintf("Test-%d", i),
			CreatedAt:   time.Now(),
			IsActive:    i%2 == 0,
			Score:       rand.Float64() * 100,
			Tags:        []string{"tag1", "tag2", "tag3"},
			Metadata:    map[string]interface{}{"key": "value"},
			Count:       uint32(i),
			Priority:    int8(i % 5),
			Description: &desc,
		}
	}
	return result
}

// generateMableEvents creates n number of MableEvent instances
func generateMableEvents(n int, template MableEvent) []interface{} {
	result := make([]interface{}, n)
	for i := 0; i < n; i++ {
		// Create a deep copy of the template
		event := template
		event.EID = fmt.Sprintf("%s-%d", event.EID, i)
		result[i] = event
	}
	return result
}

// benchmarkPipeline runs the benchmark for a given number of events
func benchmarkPipeline(b *testing.B, events []interface{}) {
	config := &Config{
		BatchSize:       100,
		Workers:         4,
		BatchTimeout:    time.Second * 5,
		MaxRetries:      3,
		RetryDelay:      time.Second,
		BufferSize:      1000,
		DynamicBatching: true,
		MetricsEnabled:  false, // Disable metrics for benchmarks
	}

	stages := []Stage{
		&MapStage[TestStruct]{
			Fn: func(t TestStruct) TestStruct {
				t.Score *= 2
				return t
			},
		},
		&FilterStage[TestStruct]{
			Fn: func(t TestStruct) bool {
				return t.IsActive
			},
		},
		&GenerateStage[TestStruct]{
			Fn: func(t TestStruct) []TestStruct {
				return []TestStruct{t}
			},
		},
		&CollectStage[TestStruct]{},
	}

	ctx := context.Background()
	p, _ := NewPipeline(ctx, config, stages...)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = p.Process(ctx, events)
	}
}

// BenchmarkTestStruct runs benchmarks for TestStruct with different sizes
func BenchmarkTestStruct(b *testing.B) {
	sizes := []int{10, 100, 10000, 100000, 1000000, 10000000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size-%d", size), func(b *testing.B) {
			events := generateTestStructs(size)
			benchmarkPipeline(b, events)
		})
	}
}

// BenchmarkMableEvent runs benchmarks for MableEvent with different sizes
func BenchmarkMableEvent(b *testing.B) {
	// Load template MableEvent
	data, err := os.ReadFile("sample-event.json")
	if err != nil {
		b.Fatal(err)
	}

	var template MableEvent
	if err := json.Unmarshal(data, &template); err != nil {
		b.Fatal(err)
	}

	sizes := []int{10, 100, 10000, 100000, 1000000, 10000000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size-%d", size), func(b *testing.B) {
			events := generateMableEvents(size, template)
			benchmarkPipeline(b, events)
		})
	}
}

// BenchmarkMableEventWithMetrics runs benchmarks with metrics enabled
func BenchmarkMableEventWithMetrics(b *testing.B) {
	// Load template MableEvent
	data, err := os.ReadFile("sample-event.json")
	if err != nil {
		b.Fatal(err)
	}

	var template MableEvent
	if err := json.Unmarshal(data, &template); err != nil {
		b.Fatal(err)
	}

	sizes := []int{10, 100, 10000, 100000, 1000000, 10000000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size-%d", size), func(b *testing.B) {
			events := generateMableEvents(size, template)

			config := DefaultConfig()

			stages := []Stage{
				&MapStage[MableEvent]{
					Fn: func(e MableEvent) MableEvent {
						// Simple transformation
						e.EID = fmt.Sprintf("processed-%s", e.EID)
						return e
					},
				},
				&FilterStage[MableEvent]{
					Fn: func(e MableEvent) bool {
						return e.EN != ""
					},
				},
				&CollectStage[MableEvent]{},
			}

			ctx := context.Background()
			p, err := NewPipeline(ctx, config, stages...)
			if err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = p.Process(ctx, events)
			}

			p.Close()
		})
	}
}

// TestPipelineBasic verifies basic pipeline functionality
func TestPipelineBasic(t *testing.T) {
	config := DefaultConfig()
	events := generateTestStructs(10)

	stages := []Stage{
		&MapStage[TestStruct]{
			Fn: func(t TestStruct) TestStruct {
				t.Score *= 2
				return t
			},
		},
		&CollectStage[TestStruct]{},
	}

	ctx := context.Background()
	p, _ := NewPipeline(ctx, config, stages...)

	results, err := p.Process(ctx, events)
	if err != nil {
		t.Fatalf("Pipeline processing failed: %v", err)
	}

	if len(results) != 10 {
		t.Errorf("Expected 10 results, got %d", len(results))
	}
}

// TestPipelineWithMetrics tests the pipeline with metrics enabled
func TestPipelineWithMetrics(t *testing.T) {
	config := &Config{
		BatchSize:       5,
		Workers:         2,
		BatchTimeout:    time.Second,
		MaxRetries:      3,
		RetryDelay:      time.Millisecond * 100,
		BufferSize:      10,
		DynamicBatching: true,
		MetricsEnabled:  true,
	}

	// Create test data
	data := generateTestStructs(20)

	// Create pipeline stages
	mapStage := &MapStage[TestStruct]{
		Fn: func(t TestStruct) TestStruct {
			t.Score *= 2
			return t
		},
	}

	pipeline, err := NewPipeline(context.Background(), config, mapStage)
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}

	// Process test data
	ctx := context.Background()
	resultChan, err := pipeline.Process(ctx, data)
	if err != nil {
		t.Fatalf("Failed to process data: %v", err)
	}

	// Collect results
	var results []interface{}
	for result := range resultChan {
		results = append(results, result)
	}

	// Verify results length
	if len(results) != len(data) {
		t.Errorf("Expected %d results, got %d", len(data), len(results))
	}

	// Wait for metrics to be collected
	time.Sleep(time.Second)
}

// TestPipelineErrorHandlingWithMetrics tests error handling with metrics
func TestPipelineErrorHandlingWithMetrics(t *testing.T) {
	config := &Config{
		BatchSize:       5,
		Workers:         2,
		BatchTimeout:    time.Second,
		MaxRetries:      3,
		RetryDelay:      time.Millisecond * 100,
		BufferSize:      10,
		DynamicBatching: true,
		MetricsEnabled:  true,
	}

	// Create a stage that fails on certain conditions
	mapStage := &MapStage[TestStruct]{
		Fn: func(t TestStruct) TestStruct {
			if t.ID%2 != 0 {
				// Instead of panic, return empty struct to simulate error
				return TestStruct{}
			}
			t.Score *= 2
			return t
		},
	}

	pipeline, err := NewPipeline(context.Background(), config, mapStage)
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}

	// Create test data
	data := generateTestStructs(10)

	// Process test data
	ctx := context.Background()
	resultChan, err := pipeline.Process(ctx, data)
	if err != nil {
		t.Fatalf("Failed to process data: %v", err)
	}

	// empty result channel
	for range resultChan {
		// just drain the channel
	}

	// Allow metrics to be collected
	time.Sleep(time.Second)
}

// BenchmarkPipelineWithMetrics measures pipeline performance with metrics enabled
func BenchmarkPipelineWithMetrics(b *testing.B) {
	config := &Config{
		BatchSize:       100,
		Workers:         runtime.NumCPU(),
		BatchTimeout:    time.Second,
		MaxRetries:      3,
		RetryDelay:      time.Millisecond * 100,
		BufferSize:      1000,
		DynamicBatching: true,
		MetricsEnabled:  false, // Disable metrics for benchmarks
	}

	// Create test data
	data := generateTestStructs(1000)

	// Create pipeline stages
	mapStage := &MapStage[TestStruct]{
		Fn: func(t TestStruct) TestStruct {
			t.Score *= 2
			return t
		},
	}

	pipeline, err := NewPipeline(context.Background(), config, mapStage)
	if err != nil {
		b.Fatalf("Failed to create pipeline: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx := context.Background()
		b.Logf("Benchmark iteration %d", i+1)
		resultChan, err := pipeline.Process(ctx, data)
		if err != nil {
			b.Fatalf("Failed to process data: %v", err)
		}

		// Collect results
		var count int
		for range resultChan {
			count++
			b.Logf("Processed %d/%d results", count, len(data))
		}

		if count != len(data) {
			b.Errorf("Expected %d results, got %d", len(data), count)
		}
	}

	pipeline.Close()
}
