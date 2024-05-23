package rabbitmq

import "errors"

// Custom errors for better error handling.
var (
	ErrMaxRetriesReached = errors.New("maximum retries reached")
)
