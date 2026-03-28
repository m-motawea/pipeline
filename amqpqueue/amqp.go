package amqpqueue

import (
	"context"
	"encoding/json"

	"github.com/m-motawea/pipeline"
	amqp091 "github.com/rabbitmq/amqp091-go"
)

// AMQPQueue implements the Pipeline Queue using RabbitMQ/AMQP broker architectures.
// Auto-loadbalances directly among consumers natively using AMQP protocols.
type AMQPQueue struct {
	conn *amqp091.Connection
}

// NewAMQPQueue dials the provided amqp URL connection.
func NewAMQPQueue(url string) (*AMQPQueue, error) {
	conn, err := amqp091.Dial(url)
	if err != nil {
		return nil, err
	}
	return &AMQPQueue{conn: conn}, nil
}

// Publish enqueues pipeline messages directly onto a declared durable AMQP queue.
func (a *AMQPQueue) Publish(ctx context.Context, topic string, msg pipeline.PipelineMessage) error {
	ch, err := a.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(topic, true, false, false, false, nil)
	if err != nil {
		return err
	}

	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	return ch.PublishWithContext(ctx, "", q.Name, false, false, amqp091.Publishing{
		ContentType: "application/json",
		Body:        b,
	})
}

// Subscribe returns an abstraction channel piping all delivered deliveries out.
func (a *AMQPQueue) Subscribe(ctx context.Context, topic string, consumerGroup string) (<-chan pipeline.PipelineMessage, error) {
	ch, err := a.conn.Channel()
	if err != nil {
		return nil, err
	}

	q, err := ch.QueueDeclare(topic, true, false, false, false, nil)
	if err != nil {
		return nil, err
	}

	msgs, err := ch.Consume(q.Name, consumerGroup, true, false, false, false, nil)
	if err != nil {
		return nil, err
	}

	out := make(chan pipeline.PipelineMessage)
	go func() {
		defer ch.Close()
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case d, ok := <-msgs:
				if !ok {
					return
				}
				var msg pipeline.PipelineMessage
				if err := json.Unmarshal(d.Body, &msg); err == nil {
					select {
					case out <- msg:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	return out, nil
}

// Close gracefully closes down the AMQP connection.
func (a *AMQPQueue) Close() error {
	return a.conn.Close()
}
