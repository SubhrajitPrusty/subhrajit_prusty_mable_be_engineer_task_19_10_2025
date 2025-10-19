package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"mable/pipeline"
)

// Event represents our sample event structure
type Event struct {
	EventID   string                 `json:"eid"`
	EventName string                 `json:"en"`
	Metadata  map[string]interface{} `json:"md"`
}

func main() {

	// Create context
	ctx := context.Background()

	// Read sample event
	eventData, err := os.ReadFile("pipeline/sample-event.json")
	if err != nil {
		log.Fatalf("Failed to read sample event: %v", err)
	}

	var event Event
	if err := json.Unmarshal(eventData, &event); err != nil {
		log.Fatalf("Failed to parse event: %v", err)
	}

	// Create pipeline configuration
	config := pipeline.DefaultConfig()
	config.BatchSize = 10

	// Create pipeline stages
	stages := []pipeline.Stage{
		// Map stage: Add timestamp to event
		&pipeline.MapStage[Event]{
			Fn: func(e Event) Event {
				if e.Metadata == nil {
					e.Metadata = make(map[string]interface{})
				}
				e.Metadata["processed_at"] = time.Now().UTC()
				return e
			},
		},

		// Filter stage: Only process "Order Completed" events
		&pipeline.FilterStage[Event]{
			Fn: func(e Event) bool {
				return e.EventName == "Order Completed"
			},
		},

		// Generate stage: Create additional events
		&pipeline.GenerateStage[Event]{
			Fn: func(e Event) []Event {
				// Create a notification event for each order
				notificationEvent := Event{
					EventID:   e.EventID + "_notification",
					EventName: "Notification_Created",
					Metadata: map[string]interface{}{
						"original_event_id": e.EventID,
						"created_at":        time.Now().UTC(),
					},
				}
				return []Event{e, notificationEvent}
			},
		},

		// Conditional stage: Route events based on type
		&pipeline.IfStage[Event]{
			Condition: func(e Event) bool {
				return e.EventName == "Notification_Created"
			},
			TruePath: []pipeline.Stage{
				&pipeline.MapStage[Event]{
					Fn: func(e Event) Event {
						e.Metadata["notification_priority"] = "high"
						return e
					},
				},
			},
			FalsePath: []pipeline.Stage{
				&pipeline.MapStage[Event]{
					Fn: func(e Event) Event {
						e.Metadata["order_processed"] = true
						return e
					},
				},
			},
		},

		// Collect stage to gather all results
		&pipeline.CollectStage[Event]{},
	}

	// Create and run pipeline
	p, err := pipeline.NewPipeline(ctx, config, stages...)
	if err != nil {
		log.Fatalf("Failed to create pipeline: %v", err)
	}

	// Process events
	results, err := p.Process(ctx, []interface{}{event})
	if err != nil {
		log.Fatalf("Pipeline processing failed: %v", err)
	}

	// Print results
	fmt.Printf("Processed %d results:\n", len(results))
	for i, result := range results {
		switch v := result.(type) {
		case Event:
			fmt.Printf("%d. Single Event - ID: %s, Type: %s\n", i+1, v.EventID, v.EventName)
		case []Event:
			fmt.Printf("%d. Event Group (%d events):\n", i+1, len(v))
			for j, e := range v {
				fmt.Printf("  %d.%d Event - ID: %s, Type: %s\n", i+1, j+1, e.EventID, e.EventName)
			}
		default:
			fmt.Printf("%d. Unknown result type: %T\n", i+1, v)
		}
	}

	p.Close()

	fmt.Println("Bye!")
}
