package rabbitmqworker

import (
	"go_core_api/package/utils"
	"go_core_api/platform/rabbitmq"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

type Config struct {
	MetricsAddr       string
	WorkerCount       int
	WorkerScaling     bool
	WorkerScalingRate float64
	MaxRetries        int
	RateLimit         int
	RetryInterval     time.Duration
	GracefulStop      time.Duration
	Logging           bool
	QueueOptions      rabbitmq.QueueOptions
	ExchangeOptions   rabbitmq.ExchangeOptions
	BindQueueOptions  rabbitmq.BindQueueOptions
	RabbitMQConfig    rabbitmq.Config
}

func DefaultConfig() *Config {

	utils.InitTracer()
	logger, _ := zap.NewProduction()

	exchangeOptions := rabbitmq.ExchangeOptions{
		Name:       "test_exchange",
		Durable:    true,
		Kind:       "direct",
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
		Args:       nil,
	}

	queueOptions := rabbitmq.QueueOptions{
		Name:           "test_queue",
		Durable:        true,
		AutoDelete:     false,
		Exclusive:      false,
		NoWait:         false,
		Args:           nil,
		DeadLetterExch: "dlx_exchange",
		DeadLetterRk:   "dlx_routing_key",
	}

	bindQueueOptions := rabbitmq.BindQueueOptions{
		Name:     queueOptions.Name,
		Key:      "test_routing_key",
		Exchange: exchangeOptions.Name,
		NoWait:   false,
		Args:     nil,
	}

	rabbitMQConfig := rabbitmq.Config{
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

	return &Config{
		WorkerCount:       1,
		Logging:           true,
		MaxRetries:        3,
		RetryInterval:     5 * time.Second,
		GracefulStop:      10 * time.Second,
		MetricsAddr:       ":2112",
		WorkerScaling:     false,
		WorkerScalingRate: 1.0,
		QueueOptions:      queueOptions,
		ExchangeOptions:   exchangeOptions,
		BindQueueOptions:  bindQueueOptions,
		RabbitMQConfig:    rabbitMQConfig,
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
