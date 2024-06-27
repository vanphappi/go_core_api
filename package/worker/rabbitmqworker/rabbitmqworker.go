package rabbitmqworker

import (
	"context"
	"encoding/json"
	"go_core_api/package/worker"
	"go_core_api/platform/rabbitmq"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.uber.org/zap"
)

type RabbitMQWorker struct {
	rbw                *rabbitmq.RabbitMQ
	ctx                context.Context
	cancel             context.CancelFunc
	errorCount         prometheus.Counter
	taskCount          prometheus.Counter
	workerInstances    int
	workerInstancesMux sync.Mutex
	logger             *logrus.Logger
	config             *Config
	processedTasks     map[string]time.Time
	processedTasksMux  sync.Mutex
}

func NewRabbitMQWorker(config *Config, rbw *rabbitmq.RabbitMQ) (*RabbitMQWorker, error) {
	logger := logrus.New()

	ctx, cancel := context.WithCancel(context.Background())

	rmqw := &RabbitMQWorker{
		ctx:             ctx,
		cancel:          cancel,
		errorCount:      prometheus.NewCounter(prometheus.CounterOpts{Name: "error_count", Help: "Number of task processing errors"}),
		taskCount:       prometheus.NewCounter(prometheus.CounterOpts{Name: "task_count", Help: "Number of tasks processed"}),
		logger:          logger,
		config:          config,
		workerInstances: config.WorkerCount,
		rbw:             rbw,
		processedTasks:  make(map[string]time.Time),
	}

	prometheus.MustRegister(rmqw.errorCount, rmqw.taskCount)

	go rmqw.cleanupProcessedTasks()

	return rmqw, nil
}

func (rmqw *RabbitMQWorker) EnqueueTask(task *worker.Task) error {
	taskByte, err := task.ToByte()

	if err != nil {
		return err
	}

	err = rmqw.rbw.Publish(rmqw.ctx, rmqw.config.BindQueueOptions.Exchange, rmqw.config.BindQueueOptions.Key, taskByte, rmqw.config.RabbitMQConfig.RetryCount, rmqw.config.RabbitMQConfig.RetryDelay, "application/json", uint8(task.Priority))

	if err != nil {
		return err
	}

	rmqw.logger.Info("Task enqueued")

	return nil
}

func (rmqw *RabbitMQWorker) StartWorkers(processFunc map[string]func(task *worker.Task) error) {
	signalChannel := make(chan os.Signal, 1)

	signal.Notify(signalChannel, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(rmqw.config.MetricsAddr, nil)
	}()

	for i := 0; i < rmqw.config.WorkerCount; i++ {
		go rmqw.worker(processFunc)
	}

	if rmqw.config.WorkerScaling {
		go rmqw.scaleWorkers(processFunc)
	}

	<-signalChannel

	rmqw.Shutdown()
}

func (rmqw *RabbitMQWorker) worker(processFunc map[string]func(task *worker.Task) error) {
	for {
		select {
		case <-rmqw.ctx.Done():
			return
		default:
			rmqw.processTasks(processFunc)
			time.Sleep(time.Second / time.Duration(rmqw.config.RateLimit))
		}
	}
}

func (rmqw *RabbitMQWorker) processTasks(processFunc map[string]func(task *worker.Task) error) {
	// Middleware example: Logging middleware
	loggingMiddleware := func(next rabbitmq.Handler) rabbitmq.Handler {
		return func(ctx context.Context, d amqp.Delivery) {
			var task worker.Task
			err := json.Unmarshal(d.Body, &task)
			if err != nil {
				rmqw.logger.Error("Failed to unmarshal task", zap.Error(err))
				d.Nack(false, true)
				return
			}
			rmqw.logger.Info("Processing task:", task.ID)
			next(ctx, d)
		}
	}

	// Middleware example: Tracing middleware
	tracingMiddleware := func(next rabbitmq.Handler) rabbitmq.Handler {
		return func(ctx context.Context, d amqp.Delivery) {
			tr := otel.Tracer("rabbitmq worker")
			ctx, span := tr.Start(ctx, "ConsumeTask")
			defer span.End()

			next(ctx, d)

			span.SetAttributes(attribute.String("Task_body", string(d.Body)))
			span.SetStatus(codes.Ok, "Task processed")
		}
	}

	// Consume messages with middleware
	err := rmqw.rbw.ConsumeWithMiddleware(rmqw.ctx, rmqw.config.QueueOptions.Name, false, func(ctx context.Context, d amqp.Delivery) {
		var task worker.Task
		err := json.Unmarshal(d.Body, &task)
		if err != nil {
			rmqw.logger.Error("Failed to unmarshal task", zap.Error(err))
			d.Nack(false, true)
			return
		}

		// Check for duplicate tasks
		if rmqw.isDuplicateTask(task.ID) {
			rmqw.logger.WithFields(logrus.Fields{"task_id": task.ID}).Warn("Duplicate task detected")
		}

		//Process the task using the provided function
		handler, exists := processFunc[task.Type]

		if !exists {
			rmqw.logger.WithFields(logrus.Fields{"task_id": task.ID}).Error("No handler for task type")
		}

		// Execute the task handler
		if err := handler(&task); err != nil {
			rmqw.handleProcessingError(d.Body, err)
			// Log and handle processing errors
		}

		// Log successful task processing
		rmqw.logger.WithFields(logrus.Fields{"task_id": task.ID}).Info("Task processed")
		rmqw.taskCount.Inc()
		rmqw.markTaskProcessed(task.ID)

		d.Ack(false)
	}, tracingMiddleware, loggingMiddleware)

	if err != nil {
		rmqw.logger.Fatal("Failed to consume tasks", zap.Error(err))
	}

	<-rmqw.ctx.Done()
}

func (rmqw *RabbitMQWorker) handleProcessingError(body []byte, err error) {
	retryCount, ok := rmqw.getRetryCount(body)

	if !ok {
		retryCount = 0
	}

	if retryCount < rmqw.config.MaxRetries {
		retryCount++

		rmqw.logger.WithError(err).Warnf("Failed to process task. Retrying (%d/%d)", retryCount, rmqw.config.MaxRetries)

		time.AfterFunc(rmqw.config.RetryInterval, func() {
			if err := rmqw.requeueTask(body, retryCount); err != nil {
				rmqw.logger.WithError(err).Error("Failed to requeue task")
			}
		})
	} else {
		rmqw.errorCount.Inc()
		rmqw.logger.WithError(err).Error("Failed to process task. Max retry attempts reached")
	}
}

func (rmqw *RabbitMQWorker) getRetryCount(body []byte) (int, bool) {
	var task map[string]interface{}

	err := json.Unmarshal(body, &task)

	if err != nil {
		rmqw.logger.WithError(err).Error("Failed to unmarshal task")
		return 0, false
	}

	retryCount, ok := task["retries"].(int)

	return retryCount, ok
}

func (rmqw *RabbitMQWorker) requeueTask(body []byte, retryCount int) error {
	var task map[string]interface{}

	err := json.Unmarshal(body, &task)

	if err != nil {
		return err
	}

	task["retries"] = retryCount

	taskByte, err := json.Marshal(task)

	if err != nil {
		return err
	}

	err = rmqw.rbw.Publish(rmqw.ctx, rmqw.config.BindQueueOptions.Exchange, rmqw.config.BindQueueOptions.Key, taskByte, rmqw.config.RabbitMQConfig.RetryCount, rmqw.config.RabbitMQConfig.RetryDelay, "application/json", uint8(task["priority"].(int)))

	return err
}

func (rmqw *RabbitMQWorker) scaleWorkers(processFunc map[string]func(task *worker.Task) error) {
	for {
		time.Sleep(30 * time.Second)

		// Get current number of messages in the queue
		queue, err := rmqw.rbw.QueueInspect(rmqw.config.QueueOptions.Name)

		if err != nil {
			rmqw.logger.WithError(err).Error("Failed to inspect queue")
			continue
		}

		// Calculate the average number of messages per worker
		avgMessagesPerWorker := float64(queue.Messages) / float64(rmqw.config.WorkerCount)

		// If the average exceeds the scaling rate, add more workers
		if avgMessagesPerWorker > rmqw.config.WorkerScalingRate {
			rmqw.workerInstancesMux.Lock()

			newWorkerCount := int(avgMessagesPerWorker / rmqw.config.WorkerScalingRate)

			if newWorkerCount > rmqw.workerInstances {
				// Provide both arguments to addWorker function
				rmqw.addWorker(newWorkerCount-rmqw.workerInstances, processFunc)

				rmqw.workerInstances += newWorkerCount - rmqw.workerInstances // Update the total worker count

			} else if newWorkerCount < rmqw.workerInstances {
				rmqw.removeWorker(rmqw.workerInstances - newWorkerCount)
			}

			rmqw.workerInstancesMux.Unlock()
		}
	}
}

func (rmqw *RabbitMQWorker) addWorker(count int, processFunc map[string]func(task *worker.Task) error) {
	for i := 0; i < count; i++ {
		go rmqw.worker(processFunc)

		rmqw.workerInstances++

		if rmqw.config.Logging {
			rmqw.logger.Info("Worker added")
		}
	}
}

func (rmqw *RabbitMQWorker) removeWorker(count int) {
	for i := 0; i < count; i++ {
		rmqw.cancel()

		rmqw.workerInstances--

		if rmqw.config.Logging {
			rmqw.logger.Info("Worker removed")
		}
	}
}

func (rmqw *RabbitMQWorker) Shutdown() {
	if rmqw.config.Logging {
		rmqw.logger.Println("Shutting down workers gracefully...")
	}

	time.Sleep(rmqw.config.GracefulStop)

	rmqw.logger.Println("Workers shut down complete")
}

func (rmqw *RabbitMQWorker) isDuplicateTask(taskID string) bool {
	rmqw.processedTasksMux.Lock()

	defer rmqw.processedTasksMux.Unlock()

	_, exists := rmqw.processedTasks[taskID]

	if exists {
		return true
	}

	rmqw.processedTasks[taskID] = time.Now()

	return false
}

func (rmqw *RabbitMQWorker) markTaskProcessed(taskID string) {
	rmqw.processedTasksMux.Lock()

	defer rmqw.processedTasksMux.Unlock()

	rmqw.processedTasks[taskID] = time.Now()
}

func (rmqw *RabbitMQWorker) cleanupProcessedTasks() {
	ticker := time.NewTicker(time.Hour)

	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rmqw.processedTasksMux.Lock()

			for taskID, timestamp := range rmqw.processedTasks {

				if time.Since(timestamp) > 24*time.Hour {
					delete(rmqw.processedTasks, taskID)
				}
			}

			rmqw.processedTasksMux.Unlock()

		case <-rmqw.ctx.Done():
			return
		}
	}
}
