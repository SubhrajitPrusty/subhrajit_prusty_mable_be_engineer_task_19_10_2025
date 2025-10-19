package pipeline

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"mable/pipeline/metrics"
)

// Pipeline configuration parameters
type Config struct {
	BatchSize       int           // Size of each batch
	Workers         int           // Number of parallel workers
	BatchTimeout    time.Duration // Maximum time to wait before processing a partial batch
	MaxRetries      int           // Maximum number of retries for failed operations
	RetryDelay      time.Duration // Delay between retries
	BufferSize      int           // Size of the channel buffers
	DynamicBatching bool          // Enable dynamic batch size adjustment
	MetricsEnabled  bool          // Enable metrics collection
}

// Stage represents a pipeline processing stage
type Stage interface {
	Process(ctx context.Context, data interface{}) (interface{}, error)
}

// MapStage represents a mapping operation that transforms data of type T
type MapStage[T any] struct {
	Fn func(T) T
}

func (m *MapStage[T]) Process(ctx context.Context, data interface{}) (interface{}, error) {
	switch v := data.(type) {
	case []T:
		results := make([]T, len(v))
		for i, item := range v {
			results[i] = m.Fn(item)
		}
		return results, nil
	case T:
		return m.Fn(v), nil
	default:
		return nil, fmt.Errorf("unexpected type in MapStage: %T", data)
	}
}

// ReduceStage represents a reduction operation that transforms data from type T to type R
type ReduceStage[T, R any] struct {
	Fn       func(T) R
	Initial  R
	resultMu sync.Mutex
	result   R
}

func (r *ReduceStage[T, R]) Process(ctx context.Context, data interface{}) (interface{}, error) {
	r.resultMu.Lock()
	defer r.resultMu.Unlock()

	switch v := data.(type) {
	case []T:
		for _, item := range v {
			r.result = r.Fn(item)
		}
	case T:
		r.result = r.Fn(v)
	default:
		return nil, fmt.Errorf("unexpected type in ReduceStage: %T", data)
	}
	return r.result, nil
}

// FilterStage represents a filtering operation that returns a boolean
type FilterStage[T any] struct {
	Fn func(T) bool
}

func (f *FilterStage[T]) Process(ctx context.Context, data interface{}) (interface{}, error) {
	switch v := data.(type) {
	case []T:
		results := make([]T, 0, len(v))
		for _, item := range v {
			if f.Fn(item) {
				results = append(results, item)
			}
		}
		if len(results) == 0 {
			return nil, nil
		}
		return results, nil
	case T:
		if f.Fn(v) {
			return v, nil
		}
		return nil, nil
	default:
		return nil, fmt.Errorf("unexpected type in FilterStage: %T", data)
	}
}

// GenerateStage represents an operation that can generate additional events
type GenerateStage[T any] struct {
	Fn func(T) []T
}

func (g *GenerateStage[T]) Process(ctx context.Context, data interface{}) (interface{}, error) {
	result := g.Fn(data.(T))
	if len(result) == 1 {
		return result[0], nil // Return single item if only one was generated
	}
	return result, nil
}

// IfStage represents a conditional branching operation
type IfStage[T any] struct {
	Condition func(T) bool
	TruePath  []Stage
	FalsePath []Stage
}

func (i *IfStage[T]) Process(ctx context.Context, data interface{}) (interface{}, error) {
	// Handle both single items and slices
	switch v := data.(type) {
	case []T:
		results := make([]T, 0, len(v))
		for _, item := range v {
			if i.Condition(item) {
				result, err := processPipeline(ctx, item, i.TruePath)
				if err != nil {
					return nil, err
				}
				if result != nil {
					switch r := result.(type) {
					case []T:
						results = append(results, r...)
					case T:
						results = append(results, r)
					}
				}
			} else {
				result, err := processPipeline(ctx, item, i.FalsePath)
				if err != nil {
					return nil, err
				}
				if result != nil {
					switch r := result.(type) {
					case []T:
						results = append(results, r...)
					case T:
						results = append(results, r)
					}
				}
			}
		}
		return results, nil
	case T:
		if i.Condition(v) {
			return processPipeline(ctx, v, i.TruePath)
		}
		return processPipeline(ctx, v, i.FalsePath)
	default:
		return nil, fmt.Errorf("unexpected type in IfStage: %T", data)
	}
}

// CollectStage represents an operation that collects results
type CollectStage[T any] struct {
	results []T
	mu      sync.Mutex
}

func (c *CollectStage[T]) Process(ctx context.Context, data interface{}) (interface{}, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch v := data.(type) {
	case []T:
		c.results = append(c.results, v...)
		return v, nil
	case T:
		c.results = append(c.results, v)
		return v, nil
	default:
		return nil, fmt.Errorf("unexpected type in CollectStage: %T", data)
	}
}

func (c *CollectStage[T]) GetResults() []T {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.results
}

// Pipeline represents a complete processing pipeline
type Pipeline struct {
	Config             *Config
	Stages             []Stage
	metrics            *metrics.MetricsCollector
	id                 string
	lastProcessingTime time.Duration
}

// NewPipeline creates a new pipeline with the given configuration
func NewPipeline(ctx context.Context, config *Config, stages ...Stage) (*Pipeline, error) {
	if config == nil {
		config = DefaultConfig()
	}

	var metricsCollector *metrics.MetricsCollector
	var err error

	if config.MetricsEnabled {
		metricsCollector, err = metrics.NewMetricsCollector(ctx)
		if err != nil {
			return nil, err
		}

		// log successful metrics collector initialization
		fmt.Printf("Metrics collector initialized successfully\n")
	}

	return &Pipeline{
		Config:  config,
		Stages:  stages,
		metrics: metricsCollector,
		id:      time.Now().Format("20060102150405"),
	}, nil
}

// DefaultConfig returns a default pipeline configuration
func DefaultConfig() *Config {
	return &Config{
		BatchSize:       100,
		Workers:         runtime.NumCPU(),
		BatchTimeout:    time.Second * 5,
		MaxRetries:      3,
		RetryDelay:      time.Second,
		BufferSize:      1000,
		DynamicBatching: true,
		MetricsEnabled:  true,
	}
}

// Process processes a batch of events through the pipeline
func (p *Pipeline) Process(ctx context.Context, events []interface{}) ([]interface{}, error) {
	if len(events) == 0 {
		return nil, nil
	}

	startTime := time.Now()
	var memStats runtime.MemStats   // For memory usage metrics
	runtime.ReadMemStats(&memStats) // Initial memory stats
	startAlloc := memStats.Alloc    // Initial memory allocation

	// Dynamic batch size adjustment if enabled
	if p.Config.DynamicBatching {
		p.adjustBatchSize() // Adjust batch size based on previous performance
	}

	// Create worker pool
	workerPool := make(chan struct{}, p.Config.Workers)
	results := make([]interface{}, 0, len(events))
	resultsMu := sync.Mutex{}
	wg := sync.WaitGroup{}
	errorCount := int64(0)

	// Process events in batches
	for i := 0; i < len(events); i += p.Config.BatchSize {
		end := i + p.Config.BatchSize
		if end > len(events) {
			end = len(events)
		}

		batch := events[i:end] // Current batch
		wg.Add(1)

		// Goroutine for processing each batch
		go func(batch []interface{}) {
			defer wg.Done()
			workerPool <- struct{}{}        // Acquire worker
			defer func() { <-workerPool }() // Release worker

			batchResults, err := p.processBatch(ctx, batch)
			if err != nil {
				// Count errors
				atomic.AddInt64(&errorCount, 1)
				return
			}

			resultsMu.Lock()
			results = append(results, batchResults...)
			resultsMu.Unlock()
		}(batch)
	}

	wg.Wait()

	// Store the processing time
	p.lastProcessingTime = time.Since(startTime)

	// Collect metrics if enabled
	if p.metrics != nil {
		runtime.ReadMemStats(&memStats) // Final memory stats
		endAlloc := memStats.Alloc      // Final memory allocation

		p.metrics.Record(metrics.Metric{
			Timestamp:      startTime,
			PipelineID:     p.id,
			EventCount:     int64(len(events)),
			ProcessingTime: time.Since(startTime),
			MemoryUsage:    endAlloc - startAlloc,
			BatchSize:      p.Config.BatchSize,
			WorkerCount:    p.Config.Workers,
			ErrorCount:     errorCount,
		})

		// log metrics recording
		fmt.Printf("Metrics recorded: Events=%d, Errors=%d\n", len(events), errorCount)
	}

	return results, nil
}

// Close closes the pipeline and its resources
func (p *Pipeline) Close() error {
	if p.metrics != nil {
		return p.metrics.Close()
	}
	return nil
}

// processBatch processes a single batch of events through all pipeline stages
func (p *Pipeline) processBatch(ctx context.Context, batch []interface{}) ([]interface{}, error) {
	results := make([]interface{}, 0, len(batch))

	for _, data := range batch {
		result, err := processPipeline(ctx, data, p.Stages)
		if err != nil {
			continue // Skip failed items or implement retry logic
		}
		if result != nil {
			results = append(results, result)
		}
	}

	return results, nil
}

// processPipeline processes a single item through all stages
func processPipeline(ctx context.Context, data interface{}, stages []Stage) (interface{}, error) {
	var result interface{} = data

	for _, stage := range stages {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			var err error
			result, err = stage.Process(ctx, result)
			if err != nil {
				return nil, err
			}
			if result == nil {
				return nil, nil // Item was filtered out
			}
		}
	}

	return result, nil
}

// adjustBatchSize dynamically adjusts the batch size based on performance metrics
func (p *Pipeline) adjustBatchSize() {
	// Simple dynamic batch size adjustment based on recent metrics
	if p.lastProcessingTime > time.Second {
		// Decrease batch size if processing is too slow
		p.Config.BatchSize = max(p.Config.BatchSize/2, 10)
	} else if p.lastProcessingTime < time.Millisecond*100 {
		// Increase batch size if processing is fast
		p.Config.BatchSize = min(p.Config.BatchSize*2, 1000)
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
