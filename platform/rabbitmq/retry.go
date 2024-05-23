package rabbitmq

import "time"

// RetryConfig holds the retry configuration settings.
type RetryConfig struct {
	MaxRetries int
	RetryDelay time.Duration
}
