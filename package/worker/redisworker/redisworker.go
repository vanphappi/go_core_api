package redisworker

import (
	"context"
	"errors"
	"fmt"
	"go_core_api/package/worker"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

type RedisWorker struct {
	client              *redis.Client
	ctx                 context.Context
	cancel              context.CancelFunc
	config              *Config
	stopChannel         chan os.Signal
	taskCount           prometheus.Counter
	errorCount          prometheus.Counter
	rateLimiter         map[string]*time.Ticker
	deadLetterKey       string
	logger              *logrus.Logger
	workerInstances     int
	workerInstancesLock sync.Mutex
}

func NewRedisWorker(config *Config) *RedisWorker {
	client := redis.NewClient(&redis.Options{
		Addr: config.RedisAddr,
	})

	ctx, cancel := context.WithCancel(context.Background())

	logger := logrus.New()

	rw := &RedisWorker{
		client:      client,
		ctx:         ctx,
		cancel:      cancel,
		config:      config,
		stopChannel: make(chan os.Signal, 1),
		taskCount: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "task_count",
			Help: "Number of tasks processed",
		}),
		errorCount: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "error_count",
			Help: "Number of task processing errors",
		}),
		rateLimiter:     make(map[string]*time.Ticker),
		deadLetterKey:   "dead_letter_queue",
		logger:          logger,
		workerInstances: config.WorkerCount,
	}

	prometheus.MustRegister(rw.taskCount, rw.errorCount)

	return rw
}

func (rw *RedisWorker) EnqueueTask(task *worker.Task) error {
	taskJSON, err := task.ToJSON()

	if err != nil {
		return err
	}

	// Check duplicate task
	dupCheck := rw.client.Get(rw.ctx, task.ID).Err()

	if dupCheck != redis.Nil {
		if rw.config.Logging {
			rw.logger.WithFields(logrus.Fields{"task_id": task.ID}).Warn("Task already exists")
		}

		return errors.New("duplicate task")
	}

	err = rw.client.ZAdd(rw.ctx, rw.config.QueueName, redis.Z{
		Score:  float64(task.Priority),
		Member: taskJSON,
	}).Err()

	if err != nil {
		if rw.config.Logging {
			rw.logger.WithError(err).WithFields(logrus.Fields{"task_id": task.ID}).Error("Failed to enqueue task")
		}
		return err
	}

	rw.client.Set(rw.ctx, task.ID, taskJSON, rw.config.TaskTTL).Err()

	if rw.config.Logging {
		rw.logger.WithFields(logrus.Fields{"task_id": task.ID}).Info("Task enqueued")
	}

	return nil
}

func (rw *RedisWorker) StartWorkers(processFunc map[string]func(task *worker.Task) error) {
	signal.Notify(rw.stopChannel, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(rw.config.MetricsAddr, nil)
	}()

	for i := 0; i < rw.config.WorkerCount; i++ {
		go rw.worker(processFunc)
	}

	if rw.config.DynamicScaling {
		go rw.scaleWorkers(processFunc) // Pass the processFunc map here
	}

	<-rw.stopChannel

	rw.Shutdown()
}

func (rw *RedisWorker) worker(processFunc map[string]func(task *worker.Task) error) {
	for {
		select {
		case <-rw.ctx.Done():
			return
		default:
			rw.processTasks(processFunc)
			time.Sleep(time.Second / time.Duration(rw.config.RateLimit))
		}
	}
}

func (rw *RedisWorker) processTasks(processFunc map[string]func(task *worker.Task) error) {
	now := time.Now().Unix()

	result, err := rw.client.ZRangeByScore(rw.ctx, rw.config.QueueName, &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%d", now),
	}).Result()

	if err != nil {
		if rw.config.Logging {
			rw.logger.WithError(err).Error("Failed to retrieve tasks")
		}
		return
	}

	var tasks []*worker.Task

	for _, taskJSON := range result {
		task, err := worker.TaskFromJSON(taskJSON)
		if err != nil {
			if rw.config.Logging {
				rw.logger.WithError(err).Error("Failed to parse task")
			}
			continue
		}
		if task.IsExpired(rw.config.TaskTTL) {
			if rw.config.Logging {
				rw.logger.WithFields(logrus.Fields{"task_id": task.ID}).Warn("Task expired")
			}
			rw.client.ZRem(rw.ctx, rw.config.QueueName, taskJSON)
			continue
		}
		tasks = append(tasks, task)
	}

	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].Priority > tasks[j].Priority
	})

	for _, task := range tasks {
		handler, exists := processFunc[task.Type]
		if !exists {
			if rw.config.Logging {
				rw.logger.WithFields(logrus.Fields{"task_id": task.ID}).Error("No handler for task type")
			}
			continue
		}
		err := handler(task)
		if err != nil {
			if rw.config.Logging {
				rw.logger.WithError(err).WithFields(logrus.Fields{"task_id": task.ID}).Error("Failed to process task")
			}
			rw.errorCount.Inc()
			err = task.IncrementRetries(rw.config.MaxRetries)
			if err != nil {
				if rw.config.Logging {
					rw.logger.WithFields(logrus.Fields{"task_id": task.ID}).Error("Task exceeded max retries and will be discarded")
				}
			} else {
				time.Sleep(rw.config.RetryInterval)
				rw.EnqueueTask(task)
			}
		} else {
			if rw.config.Logging {
				rw.logger.WithFields(logrus.Fields{"task_id": task.ID}).Info("Task processed")
			}
			rw.taskCount.Inc()
			taskJSON, _ := task.ToJSON() // Convert Task struct to JSON
			rw.client.ZRem(rw.ctx, rw.config.QueueName, taskJSON)
			rw.client.Del(rw.ctx, task.ID)
		}
	}
}

func (rw *RedisWorker) scaleWorkers(processFunc map[string]func(task *worker.Task) error) {
	for {
		time.Sleep(30 * time.Second)
		taskCount := rw.client.ZCard(rw.ctx, rw.config.QueueName).Val()
		avgTaskCount := float64(taskCount) / float64(rw.config.WorkerCount)
		if avgTaskCount > float64(rw.config.WorkerScalingFactor) {
			rw.workerInstancesLock.Lock()
			newWorkerCount := int(avgTaskCount / float64(rw.config.WorkerScalingFactor))
			if newWorkerCount > rw.workerInstances {
				rw.addWorker(newWorkerCount-rw.workerInstances, processFunc) // Adjusted call here

			} else if newWorkerCount < rw.workerInstances {
				rw.removeWorker(rw.workerInstances - newWorkerCount)
			}
			rw.workerInstancesLock.Unlock()
		}
	}
}

func (rw *RedisWorker) addWorker(count int, processFunc map[string]func(task *worker.Task) error) {
	for i := 0; i < count; i++ {
		go rw.worker(processFunc)
		rw.workerInstances++
		if rw.config.Logging {
			rw.logger.Info("Worker added")
		}
	}
}

func (rw *RedisWorker) removeWorker(count int) {
	for i := 0; i < count; i++ {
		rw.cancel()
		rw.workerInstances--
		if rw.config.Logging {
			rw.logger.Info("Worker removed")
		}
	}
}

func (rw *RedisWorker) Shutdown() {
	if rw.config.Logging {
		rw.logger.Println("Shutting down workers gracefully...")
	}
	rw.cancel()
	time.Sleep(rw.config.GracefulStop)
	if rw.config.Logging {
		rw.logger.Println("Workers shut down complete")
	}
}
