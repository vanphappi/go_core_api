package rabbitmq

import (
	"context"
	"fmt"
	"go_core_api/package/logging"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

// RabbitMQ represents a RabbitMQ client with reconnection and retry capabilities.
type RabbitMQ struct {
	conn             *amqp.Connection
	ch               *amqp.Channel
	url              string
	reconnectError   chan *amqp.Error
	reconnectBackoff *Backoff
	logger           *log.Logger
	mu               sync.Mutex // Ensure thread safety for reconnections
	metrics          *Metrics
	circuitBreaker   *CircuitBreaker
}

// NewRabbitMQ creates a new RabbitMQ client.
func NewRabbitMQ(url string, logger logging.Logger, backoff *Backoff, cb *CircuitBreaker) (*RabbitMQ, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to dial RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to create channel: %w", err)
	}

	rmq := &RabbitMQ{
		conn:             conn,
		ch:               ch,
		url:              url,
		reconnectError:   make(chan *amqp.Error, 1),
		reconnectBackoff: backoff,
		logger:           log.New(logger, "[RabbitMQ] ", log.LstdFlags),
		metrics:          &Metrics{},
		circuitBreaker:   cb,
	}

	rmq.conn.NotifyClose(rmq.reconnectError)

	go rmq.handleReconnect() // Start a goroutine to handle reconnection

	return rmq, nil
}

// handleReconnect handles reconnection attempts when the connection is closed.
func (rmq *RabbitMQ) handleReconnect() {
	for err := range rmq.reconnectError {
		if err != nil {
			rmq.logger.Printf("Connection closed: %s", err)
		}
		for {
			if err := rmq.Reconnect(); err == nil {
				break
			}
			time.Sleep(rmq.reconnectBackoff.Next(rmq.reconnectBackoff.InitialTimeout))
		}
	}
}

// DeclareExchange declares an exchange in RabbitMQ.
func (rmq *RabbitMQ) DeclareExchange(ctx context.Context, name, kind string, durable bool) error {
	rmq.mu.Lock()
	defer rmq.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return rmq.ch.ExchangeDeclare(
			name,
			kind,
			durable,
			false,
			false,
			false,
			nil,
		)
	}
}

// DeclareQueue declares a queue in RabbitMQ.
func (rmq *RabbitMQ) DeclareQueue(ctx context.Context, name string, durable, autoDelete, exclusive bool) (amqp.Queue, error) {
	rmq.mu.Lock()
	defer rmq.mu.Unlock()

	select {
	case <-ctx.Done():
		return amqp.Queue{}, ctx.Err()
	default:
		return rmq.ch.QueueDeclare(
			name,
			durable,
			autoDelete,
			exclusive,
			false,
			nil,
		)
	}
}

// BindQueue binds a queue to an exchange with a routing key.
func (rmq *RabbitMQ) BindQueue(ctx context.Context, queue, exchange, routingKey string) error {
	rmq.mu.Lock()
	defer rmq.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return rmq.ch.QueueBind(
			queue,
			routingKey,
			exchange,
			false,
			nil,
		)
	}
}

// PublishBatch publishes multiple messages to RabbitMQ.
func (rmq *RabbitMQ) PublishBatch(ctx context.Context, exchange, routingKey, contentType string, messages [][]byte) error {
	rmq.mu.Lock()
	defer rmq.mu.Unlock()

	rmq.metrics.PublishAttempts += len(messages)

	for _, body := range messages {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := rmq.ch.Publish(
				exchange,
				routingKey,
				false,
				false,
				amqp.Publishing{
					ContentType: contentType,
					Body:        body,
				},
			); err != nil {
				return fmt.Errorf("failed to publish message: %w", err)
			}
		}
	}
	return nil
}

// PublishWithRetry publishes a message with retry mechanism.
func (rmq *RabbitMQ) PublishWithRetry(ctx context.Context, exchange, routingKey, contentType string, body []byte, retryConfig RetryConfig) error {
	var err error

	timeout := rmq.reconnectBackoff.InitialTimeout

	for i := 0; i < retryConfig.MaxRetries; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			err = rmq.ch.Publish(
				exchange,
				routingKey,
				false,
				false,
				amqp.Publishing{
					ContentType: contentType,
					Body:        body,
				},
			)
			if err == nil {
				return nil
			}
			rmq.logger.Printf("Error publishing message: %s. Retrying after %s", err, timeout)

			time.Sleep(timeout + time.Duration(rand.Int63n(int64(timeout))))
			timeout = rmq.reconnectBackoff.Next(timeout)

			// Circuit breaker check
			rmq.circuitBreaker.Failures++
			if rmq.circuitBreaker.Failures >= rmq.circuitBreaker.FailureLimit {
				rmq.circuitBreaker.LastFailure = time.Now()
				return fmt.Errorf("%w: %s", ErrMaxRetriesReached, err)
			}
		}
	}

	return fmt.Errorf("failed to publish message after retries: %w", err)
}

// ConsumeWithRetry consumes messages with retry mechanism.
func (rmq *RabbitMQ) ConsumeWithRetry(ctx context.Context, queue, consumer string, autoAck bool, prefetchCount int, retryConfig RetryConfig) (<-chan amqp.Delivery, error) {
	rmq.mu.Lock()
	defer rmq.mu.Unlock()

	if prefetchCount > 0 {
		err := rmq.ch.Qos(prefetchCount, 0, false)
		if err != nil {
			return nil, fmt.Errorf("failed to set QoS: %w", err)
		}
	}

	var msgs <-chan amqp.Delivery

	var err error

	timeout := rmq.reconnectBackoff.InitialTimeout

	for i := 0; i < retryConfig.MaxRetries; i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			msgs, err = rmq.ch.Consume(
				queue,
				consumer,
				autoAck,
				false,
				false,
				false,
				nil,
			)
			if err == nil {
				return msgs, nil
			}

			rmq.logger.Printf("Error consuming messages: %s. Retrying after %s", err, timeout)

			time.Sleep(timeout + time.Duration(rand.Int63n(int64(timeout))))
			timeout = rmq.reconnectBackoff.Next(timeout)

			// Circuit breaker check
			rmq.circuitBreaker.Failures++
			if rmq.circuitBreaker.Failures >= rmq.circuitBreaker.FailureLimit {
				rmq.circuitBreaker.LastFailure = time.Now()
				return nil, fmt.Errorf("%w: %s", ErrMaxRetriesReached, err)
			}
		}
	}
	return nil, fmt.Errorf("failed to consume messages after retries: %w", err)
}

// Ack acknowledges a message in RabbitMQ.
func (rmq *RabbitMQ) Ack(ctx context.Context, deliveryTag uint64, multiple bool) error {
	rmq.mu.Lock()
	defer rmq.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return rmq.ch.Ack(deliveryTag, multiple)
	}
}

// Reject rejects a message in RabbitMQ.
func (rmq *RabbitMQ) Reject(ctx context.Context, deliveryTag uint64, requeue bool) error {
	rmq.mu.Lock()
	defer rmq.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return rmq.ch.Reject(deliveryTag, requeue)
	}
}

// Close closes the RabbitMQ connection and channel.
func (rmq *RabbitMQ) Close() {
	rmq.mu.Lock()
	defer rmq.mu.Unlock()

	if rmq.ch != nil {
		rmq.ch.Close()
	}
	if rmq.conn != nil {
		rmq.conn.Close()
	}
}

// Reconnect attempts to reconnect to RabbitMQ.
func (rmq *RabbitMQ) Reconnect() error {
	rmq.mu.Lock()
	defer rmq.mu.Unlock()

	rmq.logger.Println("Attempting to reconnect to RabbitMQ server...")
	timeout := rmq.reconnectBackoff.InitialTimeout

	for {
		conn, err := amqp.Dial(rmq.url)
		if err != nil {
			rmq.logger.Printf("Error reconnecting to RabbitMQ: %s. Retrying...", err)
			time.Sleep(timeout + time.Duration(rand.Int63n(int64(timeout))))
			timeout = rmq.reconnectBackoff.Next(timeout)
			continue
		}

		ch, err := conn.Channel()
		if err != nil {
			conn.Close()
			rmq.logger.Printf("Error creating channel after reconnect: %s. Retrying...", err)
			time.Sleep(timeout + time.Duration(rand.Int63n(int64(timeout))))
			timeout = rmq.reconnectBackoff.Next(timeout)
			continue
		}

		rmq.conn = conn
		rmq.ch = ch
		rmq.metrics.ReconnectAttempts++

		rmq.conn.NotifyClose(rmq.reconnectError)
		rmq.logger.Println("Reconnected to RabbitMQ server successfully.")
		return nil
	}
}

// ListenForReconnect returns a channel that notifies when a reconnection is needed.
func (rmq *RabbitMQ) ListenForReconnect() <-chan *amqp.Error {
	return rmq.reconnectError
}
