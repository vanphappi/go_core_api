package rabbitmq

import (
	"log"

	"github.com/streadway/amqp"
)

// Producer struct holds the necessary fields for RabbitMQ producer
type Producer struct {
	conn        *amqp.Connection
	channel     *amqp.Channel
	errorLogger *log.Logger
}

// NewProducer creates a new RabbitMQ producer instance
func NewProducer(connectionURL string, errorLogger *log.Logger) (*Producer, error) {
	conn, err := amqp.Dial(connectionURL)
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()

	if err != nil {
		conn.Close()
		return nil, err
	}

	return &Producer{
		conn:        conn,
		channel:     channel,
		errorLogger: errorLogger,
	}, nil
}

// PublishMessage publishes a message to the specified queue
func (p *Producer) PublishMessage(exchange, queueName, routingKey, contentType string, body []byte) error {
	err := p.channel.Publish(
		exchange,
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType: contentType,
			Body:        body,
		},
	)
	if err != nil {
		p.errorLogger.Printf("Failed to publish message: %v", err)
		return err
	}

	return nil
}

// Close closes the RabbitMQ connection and channel
func (p *Producer) Close() {
	p.channel.Close()
	p.conn.Close()
}
