package rabbitmq

import "time"

// Backoff defines the backoff settings for reconnections and retries.
type Backoff struct {
	InitialTimeout time.Duration
	MaxTimeout     time.Duration
}

// Next calculates the next backoff duration with jitter.
func (b *Backoff) Next(current time.Duration) time.Duration {
	next := current * 2
	if next > b.MaxTimeout {
		next = b.MaxTimeout
	}
	return next
}
