package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/streadway/amqp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
)

// QueueOptions structure
type QueueOptions struct {
	Name           string
	Durable        bool
	AutoDelete     bool
	Exclusive      bool
	NoWait         bool
	Args           amqp.Table
	DeadLetterExch string
	DeadLetterRk   string
}

// ExchangeOptions structure
type ExchangeOptions struct {
	Name       string
	Durable    bool
	Kind       string
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       amqp.Table
}

// BindQueueOptions structure
type BindQueueOptions struct {
	Name     string
	Key      string
	Exchange string
	NoWait   bool
	Args     amqp.Table
}

// Config structure for RabbitMQ
type Config struct {
	URL                  string
	PoolSize             int
	Logger               *zap.Logger
	RetryCount           int
	RetryDelay           time.Duration
	MaxWorkers           int
	WorkerLimit          int64
	BreakerThreshold     int
	BreakerResetInterval time.Duration
	RateLimit            int
	OperationTimeout     time.Duration
}

// RabbitMQ structure
type RabbitMQ struct {
	mu          sync.RWMutex
	connURL     string
	connPool    chan *amqp.Connection
	logger      *zap.Logger
	sem         *semaphore.Weighted
	metricSent  prometheus.Counter
	metricRecv  prometheus.Counter
	closeChan   chan struct{}
	tracer      trace.Tracer
	config      Config
	breaker     *CircuitBreaker
	rateLimiter *RateLimiter
}

// NewRabbitMQ initializes a new RabbitMQ instance
func NewRabbitMQ(config Config) (*RabbitMQ, error) {
	if config.Logger == nil {
		config.Logger, _ = zap.NewProduction()
	}

	tracer := otel.Tracer("rabbitmq")

	metricSent := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "rabbitmq_messages_sent_total",
		Help: "Total number of RabbitMQ messages sent.",
	})
	prometheus.MustRegister(metricSent)

	metricRecv := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "rabbitmq_messages_received_total",
		Help: "Total number of RabbitMQ messages received.",
	})

	prometheus.MustRegister(metricRecv)

	rmq := &RabbitMQ{
		connURL:     config.URL,
		connPool:    make(chan *amqp.Connection, config.PoolSize),
		logger:      config.Logger,
		sem:         semaphore.NewWeighted(config.WorkerLimit),
		metricSent:  metricSent,
		metricRecv:  metricRecv,
		closeChan:   make(chan struct{}),
		tracer:      tracer,
		config:      config,
		breaker:     NewCircuitBreaker(config.BreakerThreshold, config.BreakerResetInterval),
		rateLimiter: NewRateLimiter(config.RateLimit),
	}

	for i := 0; i < config.PoolSize; i++ {
		conn, err := amqp.Dial(config.URL)
		if err != nil {
			return nil, err
		}
		rmq.connPool <- conn
	}

	go rmq.reconnect()

	return rmq, nil
}

// UpdateConfig updates the RabbitMQ configuration dynamically
func (r *RabbitMQ) UpdateConfig(newConfig Config) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.config = newConfig
	r.logger.Info("Configuration updated", zap.Any("newConfig", newConfig))
}

// reconnect attempts to reconnect on connection failure
func (r *RabbitMQ) reconnect() {
	for {
		select {
		case <-r.closeChan:
			return
		default:
			conn := <-r.connPool
			if !conn.IsClosed() {
				r.connPool <- conn
				time.Sleep(10 * time.Second)
				continue
			}
			for {
				r.logger.Warn("Reconnecting to RabbitMQ...")
				conn, err := amqp.Dial(r.connURL)
				if err == nil {
					r.connPool <- conn
					break
				}
				r.logger.Error("Failed to reconnect to RabbitMQ", zap.Error(err))
				time.Sleep(5 * time.Second)
			}
		}
	}
}

// getChannel gets a new channel from the RabbitMQ connection
func (r *RabbitMQ) getChannel() (*amqp.Channel, error) {
	select {
	case conn := <-r.connPool:
		ch, err := conn.Channel()
		if err != nil {
			r.connPool <- conn
			return nil, err
		}
		r.connPool <- conn
		return ch, nil
	case <-time.After(r.config.OperationTimeout):
		return nil, errors.New("timeout getting channel from pool")
	}
}

// DeclareExchange declares a RabbitMQ exchange
func (r *RabbitMQ) DeclareExchange(options ExchangeOptions) error {
	ch, err := r.getChannel()
	if err != nil {
		return err
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(
		options.Name,
		options.Kind,
		options.Durable,
		options.AutoDelete,
		options.Internal,
		options.NoWait,
		options.Args,
	)
	if err != nil {
		return err
	}

	return nil
}

// DeclareQueue declares a RabbitMQ queue with DLX support
func (r *RabbitMQ) DeclareQueue(options QueueOptions) (amqp.Queue, error) {
	ch, err := r.getChannel()
	if err != nil {
		return amqp.Queue{}, err
	}
	defer ch.Close()

	if options.DeadLetterExch != "" {
		if options.Args == nil {
			options.Args = amqp.Table{}
		}
		options.Args["x-dead-letter-exchange"] = options.DeadLetterExch
		if options.DeadLetterRk != "" {
			options.Args["x-dead-letter-routing-key"] = options.DeadLetterRk
		}
	}

	// First, try to delete the existing queue to avoid mismatch in arguments
	_, err = ch.QueueDelete(options.Name, false, false, false)
	if err != nil && !errors.Is(err, amqp.ErrClosed) {
		r.logger.Warn("Failed to delete queue before declaring", zap.Error(err))
	}

	q, err := ch.QueueDeclare(
		options.Name,
		options.Durable,
		options.AutoDelete,
		options.Exclusive,
		options.NoWait,
		options.Args,
	)

	if err != nil {
		r.logger.Error("Failed to declare queue", zap.String("queue", options.Name), zap.Error(err))
		return amqp.Queue{}, err
	}

	r.logger.Info("Queue declared", zap.String("queue", options.Name))
	return q, nil
}

// BindQueue binds a queue to an exchange with a routing key
func (r *RabbitMQ) BindQueue(options BindQueueOptions) error {
	ch, err := r.getChannel()
	if err != nil {
		return err
	}
	defer ch.Close()

	err = ch.QueueBind(
		options.Name,     // name of the queue
		options.Key,      // binding key
		options.Exchange, // source exchange
		options.NoWait,   // noWait
		options.Args,     // arguments
	)
	if err != nil {
		r.logger.Error("Failed to bind queue", zap.String("queue", options.Name), zap.String("exchange", options.Exchange), zap.String("routingKey", options.Key), zap.Error(err))
		return err
	}

	r.logger.Info("Queue bound", zap.String("queue", options.Name), zap.String("exchange", options.Exchange), zap.String("routingKey", options.Key))
	return nil
}

// Publish retries publishing a message with exponential backoff and jitter
func (r *RabbitMQ) Publish(ctx context.Context, exchange, routingKey string, body []byte, maxRetries int, baseDelay time.Duration, contentType string, priority uint8) error {

	var attempt int

	for {
		err := r.publish(ctx, exchange, routingKey, body, contentType, priority)
		if err == nil {
			return nil
		}

		attempt++
		if attempt > maxRetries {
			return err
		}

		jitter := time.Duration(rand.Int63n(int64(baseDelay)))
		delay := baseDelay + jitter
		r.logger.Warn("Retrying publish", zap.Int("attempt", attempt), zap.Error(err), zap.Duration("next_retry_in", delay))
		time.Sleep(delay)

		// Exponential backoff
		baseDelay *= 2
	}
}

// Publish publishes a message to a RabbitMQ exchange
func (r *RabbitMQ) publish(ctx context.Context, exchange, routingKey string, body []byte, contentType string, priority uint8) error {
	if !r.breaker.Allow() {
		r.logger.Warn("Circuit breaker open, rejecting publish request")
		return errors.New("circuit breaker open")
	}

	_, span := r.tracer.Start(ctx, "rabbitmq.Publish")
	defer span.End()

	r.mu.RLock()
	ch, err := r.getChannel()
	r.mu.RUnlock()

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	defer ch.Close()

	err = ch.Publish(
		exchange,
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType:   contentType,
			DeliveryMode:  amqp.Persistent,
			Body:          body,
			CorrelationId: span.SpanContext().TraceID().String(),
			Timestamp:     time.Now(),
			Priority:      priority,
		},
	)

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		r.logger.Error("Failed to publish message", zap.String("exchange", exchange), zap.String("routingKey", routingKey), zap.Error(err))
		r.breaker.RecordFailure()
		return err
	}

	r.metricSent.Inc()
	r.logger.Info("Message published", zap.String("exchange", exchange), zap.String("routingKey", routingKey))
	span.SetAttributes(attribute.String("exchange", exchange), attribute.String("routingKey", routingKey))
	span.SetStatus(codes.Ok, "Message published")
	r.breaker.RecordSuccess()

	return nil
}

// Consume consumes messages from a RabbitMQ queue with concurrent processing
func (r *RabbitMQ) Consume(ctx context.Context, queue string, autoAck bool) (<-chan amqp.Delivery, error) {
	ch, err := r.getChannel()
	if err != nil {
		return nil, err
	}

	msgs, err := ch.Consume(
		queue,
		"",
		autoAck,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	return msgs, nil
}

// Ack acknowledges a RabbitMQ message
func (r *RabbitMQ) Ack(d amqp.Delivery, multiple bool) error {
	err := d.Ack(multiple)
	if err != nil {
		r.logger.Error("Failed to acknowledge message", zap.Error(err))
		return err
	}
	r.logger.Info("Message acknowledged", zap.String("deliveryTag", fmt.Sprintf("%d", d.DeliveryTag)))
	return nil
}

// Nack negatively acknowledges a RabbitMQ message
func (r *RabbitMQ) Nack(d amqp.Delivery, multiple, requeue bool) error {
	err := d.Nack(multiple, requeue)
	if err != nil {
		r.logger.Error("Failed to negatively acknowledge message", zap.Error(err))
		return err
	}
	r.logger.Info("Message negatively acknowledged", zap.String("deliveryTag", fmt.Sprintf("%d", d.DeliveryTag)), zap.Bool("requeue", requeue))
	return nil
}

// QueueInspect inspects a RabbitMQ queue and returns the queue information
func (r *RabbitMQ) QueueInspect(queue string) (amqp.Queue, error) {
	ch, err := r.getChannel()
	if err != nil {
		return amqp.Queue{}, err
	}
	defer ch.Close()

	q, err := ch.QueueInspect(queue)
	if err != nil {
		r.logger.Error("Failed to inspect queue", zap.String("queue", queue), zap.Error(err))
		return amqp.Queue{}, err
	}

	r.logger.Info("Queue inspected", zap.String("queue", queue), zap.Int("messageCount", q.Messages), zap.Int("consumerCount", q.Consumers))
	return q, nil
}

// Handler is a function type that processes an AMQP delivery with context
type Handler func(ctx context.Context, d amqp.Delivery)

// Middleware is a function type that wraps a Handler
type Middleware func(Handler) Handler

// ConsumeWithMiddleware consumes messages with middleware for pre- and post-processing
func (r *RabbitMQ) ConsumeWithMiddleware(ctx context.Context, queue string, autoAck bool, handler Handler, middlewares ...Middleware) error {
	ch, err := r.getChannel()

	if err != nil {
		return err
	}

	msgs, err := ch.Consume(
		queue,
		"",
		autoAck,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	// Apply middlewares
	wrappedHandler := handler
	for i := len(middlewares) - 1; i >= 0; i-- {
		m := middlewares[i]
		next := wrappedHandler
		wrappedHandler = m(next)
	}

	go func() {
		for {
			select {
			case d := <-msgs:
				if d.Body != nil {
					r.sem.Acquire(ctx, 1)
					r.rateLimiter.Acquire()
					go func(d amqp.Delivery) {
						defer r.sem.Release(1)
						defer r.rateLimiter.Release()
						wrappedHandler(ctx, d)
					}(d)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return nil
}

// Close closes the RabbitMQ connections
func (r *RabbitMQ) GracefulShutdown() {
	r.logger.Info("Shutting down RabbitMQ connection pool")
	close(r.closeChan)
	for i := 0; i < r.config.PoolSize; i++ {
		conn := <-r.connPool
		if err := conn.Close(); err != nil {
			r.logger.Error("Failed to close connection", zap.Error(err))
		}
	}
	r.logger.Info("RabbitMQ connections closed gracefully")
}

// SignalHandler handles graceful shutdown on OS signals.
func (r *RabbitMQ) SignalHandler() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	r.logger.Info("Received signal, shutting down", zap.String("signal", sig.String()))
	r.GracefulShutdown()
}

// HealthCheckHandler handles health check requests
func (r *RabbitMQ) HealthCheckHandler(w http.ResponseWriter, req *http.Request) {
	select {
	case conn := <-r.connPool:
		defer func() { r.connPool <- conn }()
		if conn.IsClosed() {
			http.Error(w, "RabbitMQ connection is closed", http.StatusServiceUnavailable)
			return
		}
	default:
		http.Error(w, "No RabbitMQ connections available", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("RabbitMQ connection is healthy"))
}

// StartHealthCheckServer starts an HTTP server for health checks
func (r *RabbitMQ) StartHealthCheckServer(addr string) {
	http.HandleFunc("/health", r.HealthCheckHandler)
	go func() {
		if err := http.ListenAndServe(addr, nil); err != nil {
			r.logger.Fatal("Failed to start health check server", zap.Error(err))
		}
	}()
	r.logger.Info("Health check server started", zap.String("address", addr))
}

// CircuitBreaker implements the circuit breaker pattern
type CircuitBreaker struct {
	failureCount    int
	successCount    int
	threshold       int
	resetInterval   time.Duration
	lastFailureTime time.Time
	lastSuccessTime time.Time
	breakerOpen     bool
	mu              sync.Mutex
}

// NewCircuitBreaker creates a new CircuitBreaker instance
func NewCircuitBreaker(threshold int, resetInterval time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		threshold:     threshold,
		resetInterval: resetInterval,
	}
}

// Allow checks if the circuit is open or closed
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.breakerOpen && time.Since(cb.lastFailureTime) > cb.resetInterval {
		cb.breakerOpen = false
		cb.failureCount = 0
	}

	return !cb.breakerOpen
}

// RecordSuccess records a successful operation and closes the circuit if necessary
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.successCount++
	if cb.successCount >= cb.threshold {
		cb.breakerOpen = false
		cb.successCount = 0
	}
	cb.lastSuccessTime = time.Now()
}

// RecordFailure records a failure and opens the circuit if necessary
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failureCount++
	if cb.failureCount >= cb.threshold {
		go cb.reset()
	}
}

// reset resets the circuit breaker after the reset interval
func (cb *CircuitBreaker) reset() {
	time.Sleep(cb.resetInterval)
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failureCount++
	if cb.failureCount >= cb.threshold {
		cb.breakerOpen = true
		cb.failureCount = 0
	}
	cb.lastFailureTime = time.Now()
}

// RateLimiter implements a simple rate limiter
type RateLimiter struct {
	limit  int
	tokens chan struct{}
}

// NewRateLimiter creates a new RateLimiter instance
func NewRateLimiter(limit int) *RateLimiter {
	tokens := make(chan struct{}, limit)
	for i := 0; i < limit; i++ {
		tokens <- struct{}{}
	}
	return &RateLimiter{
		limit:  limit,
		tokens: tokens,
	}
}

// Acquire acquires a token from the rate limiter
func (rl *RateLimiter) Acquire() {
	<-rl.tokens
}

// Release releases a token back to the rate limiter
func (rl *RateLimiter) Release() {
	rl.tokens <- struct{}{}
}
