package rabbitmqworker

import (
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
)

type Config struct {
	RabbitMQURL       string
	QueueName         string
	MetricsAddr       string
	WorkerCount       int
	WorkerScaling     bool
	WorkerScalingRate float64
	MaxRetries        int
	RetryInterval     time.Duration
	GracefulStop      time.Duration
	Logging           bool
}

func DefaultConfig() *Config {
	return &Config{
		RabbitMQURL:       "amqp://guest:guest@localhost:5672/",
		QueueName:         "task_queue",
		WorkerCount:       1,
		Logging:           true,
		MaxRetries:        3,
		RetryInterval:     5 * time.Second,
		GracefulStop:      10 * time.Second,
		MetricsAddr:       ":2112",
		WorkerScaling:     false,
		WorkerScalingRate: 1.0,
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
