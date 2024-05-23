package rabbitmq

import "time"

// CircuitBreaker helps to prevent constant retries during prolonged failures.
type CircuitBreaker struct {
	Failures     int
	FailureLimit int
	LastFailure  time.Time
	ResetTimeout time.Duration
}
