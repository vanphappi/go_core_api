package redisworker

import (
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
)

type Config struct {
	RedisAddr           string        `json:"redis_addr"`
	QueueName           string        `json:"queue_name"`
	WorkerCount         int           `json:"worker_count"`
	Logging             bool          `json:"logging"`
	MaxRetries          int           `json:"max_retries"`
	RetryInterval       time.Duration `json:"retry_interval"`
	GracefulStop        time.Duration `json:"graceful_stop"`
	TaskTTL             time.Duration `json:"task_ttl"`
	RateLimit           int           `json:"rate_limit"`
	MetricsAddr         string        `json:"metrics_addr"`
	TaskTimeout         time.Duration `json:"task_timeout"`
	TaskRetryDelay      time.Duration `json:"task_retry_delay"`
	ThrottleLimit       int           `json:"throttle_limit"`
	ThrottleInterval    time.Duration `json:"throttle_interval"`
	DynamicScaling      bool          `json:"dynamic_scaling"`
	WorkerScalingFactor float64       `json:"worker_scaling_factor"`
	TaskDeduplication   bool          `json:"task_deduplication"`
	PriorityPolicy      string        `json:"priority_policy"`
	TaskChaining        bool          `json:"task_chaining"`
	TaskMiddleware      bool          `json:"task_middleware"`
}

func DefaultConfig() *Config {
	return &Config{
		RedisAddr:           "localhost:6379",
		QueueName:           "task_queue",
		WorkerCount:         10,
		Logging:             true,
		MaxRetries:          3,
		RetryInterval:       5 * time.Second,
		GracefulStop:        10 * time.Second,
		TaskTTL:             24 * time.Hour,
		RateLimit:           10,
		MetricsAddr:         ":2112",
		TaskTimeout:         60 * time.Second,
		TaskRetryDelay:      1 * time.Minute,
		ThrottleLimit:       100,
		ThrottleInterval:    1 * time.Second,
		DynamicScaling:      false,
		WorkerScalingFactor: 1.5,
		TaskDeduplication:   true,
		PriorityPolicy:      "fifo",
		TaskChaining:        false,
		TaskMiddleware:      false,
	}
}

func LoadConfig(filename string) (*Config, error) {
	viper.SetConfigFile(filename)
	err := viper.ReadInConfig()
	if err != nil {
		return nil, err
	}

	var config Config
	err = viper.Unmarshal(&config)
	if err != nil {
		return nil, err
	}

	viper.WatchConfig()
	viper.OnConfigChange(func(e fsnotify.Event) {
		err := viper.Unmarshal(&config)
		if err != nil {
			panic(err)
		}
	})

	return &config, nil
}
