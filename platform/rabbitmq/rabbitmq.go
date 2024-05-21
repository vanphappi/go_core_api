package rabbitmq

import (
	"errors"
	"log"
	"sync"
)

// RabbitMQ struct holds the necessary fields for managing RabbitMQ connections
type RabbitMQ struct {
	consumers   map[string]*Consumer
	producers   map[string]*Producer
	errorLogger *log.Logger
	mu          sync.Mutex
}

// ErrClusterAlreadyExists is returned when trying to create a consumer or producer for an already existing cluster
var ErrClusterAlreadyExists = errors.New("cluster already exists")

// NewRabbitMQ creates a new RabbitMQ instance
func NewRabbitMQ(errorLogger *log.Logger) *RabbitMQ {
	return &RabbitMQ{
		consumers:   make(map[string]*Consumer),
		producers:   make(map[string]*Producer),
		errorLogger: errorLogger,
	}
}

// NewConsumer creates a new RabbitMQ consumer instance associated with a specific cluster
func (r *RabbitMQ) NewConsumer(connectionURL, queueName, clusterName string, prefetchCount int) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.consumers[clusterName]; exists {
		return ErrClusterAlreadyExists
	}

	consumer, err := NewConsumer(connectionURL, queueName, prefetchCount, r.errorLogger)
	if err != nil {
		return err
	}

	r.consumers[clusterName] = consumer
	return nil
}

// NewProducer creates a new RabbitMQ producer instance associated with a specific cluster
func (r *RabbitMQ) NewProducer(connectionURL, clusterName string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.producers[clusterName]; exists {
		return ErrClusterAlreadyExists
	}

	producer, err := NewProducer(connectionURL, r.errorLogger)
	if err != nil {
		return err
	}

	r.producers[clusterName] = producer
	return nil
}

// Close closes all RabbitMQ connections and channels
func (r *RabbitMQ) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, consumer := range r.consumers {
		consumer.Close()
	}
	for _, producer := range r.producers {
		producer.Close()
	}
}
