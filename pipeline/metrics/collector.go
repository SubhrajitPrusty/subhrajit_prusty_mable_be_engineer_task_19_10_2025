package metrics

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// Metric represents a single pipeline metric
type Metric struct {
	Timestamp      time.Time
	PipelineID     string
	EventCount     int64
	ProcessingTime time.Duration
	MemoryUsage    uint64
	BatchSize      int
	WorkerCount    int
	ErrorCount     int64
}

// MetricsCollector handles metric collection and storage
type MetricsCollector struct {
	db            *sql.DB
	buffer        []Metric
	mu            sync.Mutex
	lastFlushTime time.Time
	bufferSize    int // Size threshold for buffer flush
}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector(ctx context.Context) (*MetricsCollector, error) {
	// Try different connection options
	connectionOptions := []struct {
		addr string
		desc string
	}{
		{"localhost:9000", "localhost"},
		{"127.0.0.1:9000", "loopback IP"},
		// {"clickhouse:9000", "docker service name"},
	}

	var conn *sql.DB
	var lastErr error

	for _, opt := range connectionOptions {
		log.Printf("Attempting to connect to ClickHouse via %s (%s)", opt.addr, opt.desc)
		options := &clickhouse.Options{
			Addr: []string{opt.addr},
			Auth: clickhouse.Auth{
				Database: "pipeline_metrics",
				Username: "admin",
				Password: "admin123",
			},
			Debug: false,
			Compression: &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
			},
			Settings: clickhouse.Settings{
				"max_execution_time": 60,
			},
			DialTimeout:          time.Second * 10,
			ConnOpenStrategy:     clickhouse.ConnOpenInOrder,
			BlockBufferSize:      10,
			MaxCompressionBuffer: 10240,
		}

		conn = clickhouse.OpenDB(options)
		conn.SetConnMaxIdleTime(time.Minute * 5)
		conn.SetConnMaxLifetime(time.Hour)
		conn.SetMaxIdleConns(5)
		conn.SetMaxOpenConns(10)

		// Try to ping
		if err := conn.PingContext(ctx); err != nil {
			lastErr = fmt.Errorf("failed to connect to ClickHouse at %s: %v", opt.addr, err)
			log.Printf("Connection attempt failed: %v", lastErr)
			continue
		}

		// Successfully connected
		log.Printf("Successfully connected to ClickHouse via %s", opt.addr)

		// Create table if not exists
		_, err := conn.ExecContext(ctx, `
			CREATE TABLE IF NOT EXISTS pipeline_metrics (
				timestamp DateTime,
				pipeline_id String,
				event_count Int64,
				processing_time_ms Float64,
				memory_usage UInt64,
				batch_size Int32,
				worker_count Int32,
				error_count Int64
			) ENGINE = MergeTree()
			ORDER BY (timestamp, pipeline_id)
		`)
		if err != nil {
			log.Printf("Failed to create table: %v", err)
			return nil, fmt.Errorf("failed to create metrics table: %v", err)
		}

		collector := &MetricsCollector{
			db:            conn,
			buffer:        make([]Metric, 0, 1000),
			lastFlushTime: time.Now(),
			bufferSize:    1000, // Flush every 10 metrics
		}
		return collector, nil
	}

	return nil, fmt.Errorf("failed to connect to ClickHouse: %v", lastErr)
}

// Record adds a new metric to the buffer
func (mc *MetricsCollector) Record(metric Metric) {
	mc.mu.Lock()
	mc.buffer = append(mc.buffer, metric)
	shouldFlush := len(mc.buffer) >= mc.bufferSize
	mc.mu.Unlock()

	// Flush outside of the lock if needed
	if shouldFlush {
		// log when flushing due to buffer size
		log.Printf("Buffer size reached (%d), flushing metrics...", mc.bufferSize)
		if err := mc.Flush(); err != nil {
			log.Printf("Failed to flush metrics: %v", err)
		}
	}
}

// Flush writes all buffered metrics to ClickHouse
func (mc *MetricsCollector) Flush() error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if len(mc.buffer) == 0 {
		return nil
	}

	log.Printf("Attempting to flush %d metrics", len(mc.buffer))

	// Check connection
	if err := mc.db.Ping(); err != nil {
		return fmt.Errorf("database connection lost: %v", err)
	}

	tx, err := mc.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to start transaction: %v", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO pipeline_metrics (
			timestamp,
			pipeline_id,
			event_count,
			processing_time_ms,
			memory_usage,
			batch_size,
			worker_count,
			error_count
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	startTime := time.Now()
	insertedCount := 0

	for i, metric := range mc.buffer {
		log.Printf("Inserting metric %d/%d: PipelineID=%s, EventCount=%d, ProcessingTime=%v",
			i+1, len(mc.buffer), metric.PipelineID, metric.EventCount, metric.ProcessingTime)

		_, err = stmt.Exec(
			metric.Timestamp,
			metric.PipelineID,
			metric.EventCount,
			float64(metric.ProcessingTime.Milliseconds()),
			metric.MemoryUsage,
			metric.BatchSize,
			metric.WorkerCount,
			metric.ErrorCount,
		)
		if err != nil {
			log.Printf("Failed to insert metric %d/%d: %v", i+1, len(mc.buffer), err)
			return fmt.Errorf("failed to insert metric: %v", err)
		}
		insertedCount++
	}

	log.Printf("Committing transaction with %d metrics...", insertedCount)
	if err := tx.Commit(); err != nil {
		log.Printf("Failed to commit transaction: %v", err)
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	duration := time.Since(startTime)
	log.Printf("Successfully flushed %d metrics in %v (%.2f metrics/sec)",
		insertedCount, duration, float64(insertedCount)/duration.Seconds())

	mc.buffer = mc.buffer[:0]
	mc.lastFlushTime = time.Now()
	return nil
}

// Close flushes remaining metrics and closes the connection
func (mc *MetricsCollector) Close() error {
	if err := mc.Flush(); err != nil {
		return err
	}
	return mc.db.Close()
}
