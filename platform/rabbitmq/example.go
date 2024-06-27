package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"go_core_api/package/utils"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/streadway/amqp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.uber.org/zap"
)

type ExampleMessage struct {
	Content string `json:"content"`
}

func Example() {
	utils.InitTracer()
	logger, _ := zap.NewProduction()
	config := Config{
		URL:                  "amqp://guest:guest@localhost:5672/",
		PoolSize:             5,
		Logger:               logger,
		RetryCount:           5,
		RetryDelay:           2 * time.Second,
		MaxWorkers:           10,
		WorkerLimit:          5,
		BreakerThreshold:     3,
		BreakerResetInterval: 30 * time.Second,
		RateLimit:            10,
	}

	rabbitMQ, err := NewRabbitMQ(config)
	if err != nil {
		logger.Fatal("Failed to connect to RabbitMQ", zap.Error(err))
	}
	defer rabbitMQ.SignalHandler()

	// rabbitMQ.StartHealthCheckServer(":8080")

	exchangeOptions := ExchangeOptions{
		Name:       "test_exchange",
		Durable:    true,
		Kind:       "direct",
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
		Args:       nil,
	}

	queueOptions := QueueOptions{
		Name:           "test_queue",
		Durable:        true,
		AutoDelete:     false,
		Exclusive:      false,
		NoWait:         false,
		Args:           nil,
		DeadLetterExch: "dlx_exchange",
		DeadLetterRk:   "dlx_routing_key",
	}

	bindQueueOptions := BindQueueOptions{
		Name:     queueOptions.Name,
		Key:      "test_routing_key",
		Exchange: exchangeOptions.Name,
		NoWait:   false,
		Args:     nil,
	}

	err = rabbitMQ.DeclareExchange(exchangeOptions)
	if err != nil {
		logger.Fatal("Failed to declare exchange", zap.Error(err))
	}

	_, err = rabbitMQ.DeclareQueue(queueOptions)

	if err != nil {
		logger.Fatal("Failed to declare queue", zap.Error(err))
	}

	err = rabbitMQ.BindQueue(bindQueueOptions)
	if err != nil {
		logger.Fatal("Failed to bind queue", zap.Error(err))
	}

	//Publish a JSON message with retries
	message := ExampleMessage{Content: "Hello, World!"}

	bytes, err := json.Marshal(message)

	if err != nil {
		fmt.Println(err)
		return
	}

	ctx := context.Background()

	err = rabbitMQ.Publish(ctx, bindQueueOptions.Exchange, bindQueueOptions.Key, bytes, config.RetryCount, config.RetryDelay, "application/json", 0)
	if err != nil {
		logger.Fatal("Failed to publish a message after retries", zap.Error(err))
	}

	// Setup a context for graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Middleware example: Logging middleware
	loggingMiddleware := func(next Handler) Handler {
		return func(ctx context.Context, d amqp.Delivery) {
			logger.Info("Processing message", zap.ByteString("body", d.Body))
			next(ctx, d)
		}
	}

	// Middleware example: Tracing middleware
	tracingMiddleware := func(next Handler) Handler {
		return func(ctx context.Context, d amqp.Delivery) {
			tr := otel.Tracer("rabbitmq")
			ctx, span := tr.Start(ctx, "ConsumeMessage")
			defer span.End()

			next(ctx, d)

			span.SetAttributes(attribute.String("message_body", string(d.Body)))
			span.SetStatus(codes.Ok, "Message processed")
		}
	}

	// Consume messages with middleware
	err = rabbitMQ.ConsumeWithMiddleware(ctx, queueOptions.Name, false, func(ctx context.Context, d amqp.Delivery) {
		var msg ExampleMessage
		err := json.Unmarshal(d.Body, &msg)
		if err != nil {
			logger.Error("Failed to unmarshal message", zap.Error(err))
			d.Nack(false, true)
			return
		}

		logger.Info("Received a message", zap.String("content", msg.Content))
		d.Ack(false)
	}, loggingMiddleware, tracingMiddleware)

	if err != nil {
		logger.Fatal("Failed to consume messages", zap.Error(err))
	}

	logger.Info("Waiting for messages. To exit press CTRL+C")
	<-ctx.Done()
	logger.Info("Shutting down")
}
