# Pipeline Library

A high-performance, generic pipeline processing library in Go with metrics collection and visualization.

## Features

- Generic pipeline stages:
  - Map
  - Reduce
  - Filter
  - Generate
  - If (conditional branching)
  - Collect
- Dynamic batch processing with worker pools
- ClickHouse metrics integration
- Comprehensive benchmarks

## Quick Start

### Prerequisites

- Go 1.25+
- Docker and Docker Compose
- ClickHouse


### Installation

```bash
git clone <repository-url>
cd mable
go mod tidy
```

### Running Tests

```bash
# Run all tests
go test ./...

# Run specific benchmark
go test -run ^$ -bench ^BenchmarkMableEventWithMetrics$ -benchmem mable/pipeline
```

### Start Observability Stack

```bash
# Start services
docker-compose up -d

# Check services
docker-compose ps
```

## Basic Usage

```go
package main

import (
    "context"
    "mable/pipeline"
)

func main() {
    // Create pipeline with default config
    config := pipeline.DefaultConfig()
    p, err := pipeline.NewPipeline(context.Background(), config)
    if err != nil {
        log.Fatal(err)
    }
    defer p.Close()

    // Add pipeline stages
    stages := []pipeline.Stage{
        &pipeline.MapStage[Event]{
            Fn: func(e Event) Event {
                e.ProcessedAt = time.Now()
                return e
            },
        },
        // Add more stages as needed
    }

    // Process events
    results, err := p.Process(ctx, events)
    if err != nil {
        log.Fatal(err)
    }
}
```

## Metrics

Metrics are collected in ClickHouse:
- Processing time
- Event throughput
- Memory usage
- Error rates

## Benchmarks

Run full benchmark suite:
```bash
go test -benchmem -bench=. ./pipeline
```