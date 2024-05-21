package rabbitmqworker

import (
	"context"
	"encoding/json"
	"go_core_api/package/worker"
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
)

type RabbitMQWorker struct {
	conn               *amqp.Connection
	ch                 *amqp.Channel
	queueName          string
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

func NewRabbitMQWorker(config *Config) (*RabbitMQWorker, error) {
	conn, err := amqp.Dial(config.RabbitMQURL)

	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()

	if err != nil {
		return nil, err
	}

	q, err := ch.QueueDeclare(
		config.QueueName,
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return nil, err
	}

	logger := logrus.New()

	ctx, cancel := context.WithCancel(context.Background())

	rmqw := &RabbitMQWorker{
		conn:            conn,
		ch:              ch,
		queueName:       q.Name,
		ctx:             ctx,
		cancel:          cancel,
		errorCount:      prometheus.NewCounter(prometheus.CounterOpts{Name: "error_count", Help: "Number of task processing errors"}),
		taskCount:       prometheus.NewCounter(prometheus.CounterOpts{Name: "task_count", Help: "Number of tasks processed"}),
		logger:          logger,
		config:          config,
		workerInstances: config.WorkerCount,
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

	err = rmqw.ch.Publish(
		"",             // exchange
		rmqw.queueName, // routing key
		false,          // mandatory
		false,          // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        taskByte,
			Priority:    uint8(task.Priority),
		},
	)

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
	// Consume messages from RabbitMQ
	msgs, err := rmqw.ch.Consume(
		rmqw.queueName, // Queue name
		"",             // Consumer tag
		true,           // Auto acknowledge
		false,          // Exclusive
		false,          // No local
		false,          // No wait
		nil,            // Args
	)

	if err != nil {
		rmqw.logger.Fatalf("Failed to register a consumer: %v", err)
	}

	for msg := range msgs {
		// Convert message bytes to a string
		msgBody := string(msg.Body)

		// Deserialize the message into a task
		task, err := worker.TaskFromJSON(msgBody)

		if err != nil {
			// Log and handle parsing errors
			rmqw.handleProcessingError(msg.Body, err)
			continue
		}

		// Check for duplicate tasks
		if rmqw.isDuplicateTask(task.ID) {
			rmqw.logger.WithFields(logrus.Fields{"task_id": task.ID}).Warn("Duplicate task detected")
			continue
		}

		// Process the task using the provided function
		handler, exists := processFunc[task.Type]

		if !exists {
			rmqw.logger.WithFields(logrus.Fields{"task_id": task.ID}).Error("No handler for task type")
			continue
		}

		// Execute the task handler
		if err := handler(task); err != nil {
			// Log and handle processing errors
			rmqw.handleProcessingError(msg.Body, err)
			continue
		}

		// Log successful task processing
		rmqw.logger.WithFields(logrus.Fields{"task_id": task.ID}).Info("Task processed")
		rmqw.taskCount.Inc()
		rmqw.markTaskProcessed(task.ID)
	}
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

	retryCount, ok := task["retry_count"].(int)

	return retryCount, ok
}

func (rmqw *RabbitMQWorker) requeueTask(body []byte, retryCount int) error {
	var task map[string]interface{}

	err := json.Unmarshal(body, &task)

	if err != nil {
		return err
	}

	task["retry_count"] = retryCount

	taskJSON, err := json.Marshal(task)

	if err != nil {
		return err
	}

	return rmqw.ch.Publish(
		"",             // exchange
		rmqw.queueName, // routing key
		false,          // mandatory
		false,          // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        taskJSON,
		},
	)
}

func (rmqw *RabbitMQWorker) scaleWorkers(processFunc map[string]func(task *worker.Task) error) {
	for {
		time.Sleep(30 * time.Second)

		// Get current number of messages in the queue
		queueSize, err := rmqw.ch.QueueInspect(rmqw.queueName)
		if err != nil {
			rmqw.logger.WithError(err).Error("Failed to inspect queue")
			continue
		}

		// Calculate the average number of messages per worker
		avgMessagesPerWorker := float64(queueSize.Messages) / float64(rmqw.config.WorkerCount)

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

	if err := rmqw.ch.Close(); err != nil {
		rmqw.logger.WithError(err).Error("Error closing RabbitMQ channel")
	}

	if err := rmqw.conn.Close(); err != nil {
		rmqw.logger.WithError(err).Error("Error closing RabbitMQ connection")
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
