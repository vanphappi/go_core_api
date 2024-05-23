package rabbitmq

// Metrics holds the metrics for monitoring RabbitMQ operations.
type Metrics struct {
	ReconnectAttempts int
	PublishAttempts   int
	ConsumeAttempts   int
}
