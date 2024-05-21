package redisworker

import (
	"fmt"
	"go_core_api/package/worker"
	"time"
)

func Example() {
	// Load configuration from file
	config := DefaultConfig()

	// Display loaded configuration
	fmt.Println("Loaded Configuration:")
	fmt.Printf("%+v\n", config)

	// Create a Redis worker instance with loaded configuration
	rw := NewRedisWorker(config)

	// Define a task processing function
	processFunc := map[string]func(task *worker.Task) error{
		"default": func(task *worker.Task) error {
			fmt.Printf("Processing task: %s\n", task.ID)
			// Simulate task processing
			time.Sleep(2 * time.Second)
			fmt.Printf("Task %s processed successfully\n", task.ID)
			return nil
		},
	}

	// Start worker with task processing function
	go rw.StartWorkers(processFunc)

	// Simulate tasks being enqueued
	go func() {
		for i := 0; i < 5; i++ {
			task := worker.NewTask(fmt.Sprintf("task%d", i), "default", fmt.Sprintf("payload%d", i), 1, time.Now(), 0, 0)
			err := rw.EnqueueTask(task)
			if err != nil {
				fmt.Printf("Error enqueuing task: %s\n", err)
			} else {
				fmt.Printf("Task %s enqueued successfully\n", task.ID)
			}
			time.Sleep(1 * time.Second)
		}
	}()

	// Keep the main goroutine running
	select {}
}
