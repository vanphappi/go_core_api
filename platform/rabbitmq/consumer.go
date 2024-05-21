package rabbitmq

import (
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/streadway/amqp"
)

// Consumer struct holds the necessary fields for RabbitMQ consumer
type Consumer struct {
	conn           *amqp.Connection
	channel        *amqp.Channel
	msgs           <-chan amqp.Delivery
	errorLogger    *log.Logger
	connectionURL  string
	queueName      string
	connectionDone chan struct{}
	prefetchCount  int
}

// NewConsumer creates a new RabbitMQ consumer instance
func NewConsumer(connectionURL, queueName string, prefetchCount int, errorLogger *log.Logger) (*Consumer, error) {
	c := &Consumer{
		errorLogger:    errorLogger,
		connectionURL:  connectionURL,
		queueName:      queueName,
		connectionDone: make(chan struct{}),
		prefetchCount:  prefetchCount,
	}

	err := c.connect()
	if err != nil {
		return nil, err
	}

	go c.handleReconnect()

	return c, nil
}

func (c *Consumer) connect() error {
	conn, err := amqp.Dial(c.connectionURL)
	if err != nil {
		return err
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return err
	}

	err = channel.Qos(
		c.prefetchCount, // prefetch count
		0,               // prefetch size
		false,           // global
	)
	if err != nil {
		conn.Close()
		channel.Close()
		return err
	}

	_, err = channel.QueueDeclare(
		c.queueName,
		true,  // durable queue
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,
	)
	
	if err != nil {
		conn.Close()
		channel.Close()
		return err
	}

	msgs, err := channel.Consume(
		c.queueName,
		"",
		false, // manual acknowledgment
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,
	)
	if err != nil {
		conn.Close()
		channel.Close()
		return err
	}

	c.conn = conn
	c.channel = channel
	c.msgs = msgs

	c.errorLogger.Println("Connected to RabbitMQ")
	return nil
}

func (c *Consumer) handleReconnect() {
	for {
		select {
		case <-c.connectionDone:
			return
		case <-time.After(5 * time.Second): // Retry after 5 seconds
			c.errorLogger.Println("Attempting to reconnect to RabbitMQ...")
			if err := c.connect(); err != nil {
				c.errorLogger.Printf("Failed to reconnect to RabbitMQ: %v", err)
			}
		}
	}
}

// ConsumeMessages starts consuming messages from the queue
func (c *Consumer) ConsumeMessages(messageHandler func([]byte) error) {
	for msg := range c.msgs {
		err := messageHandler(msg.Body)
		if err != nil {
			c.errorLogger.Printf("Error processing message: %v", err)
		} else {
			msg.Ack(false) // Acknowledge the message
		}
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	<-stop
	c.Close()
}

// Close closes the RabbitMQ connection and channel
func (c *Consumer) Close() {
	c.connectionDone <- struct{}{}
	c.channel.Close()
	c.conn.Close()
}
