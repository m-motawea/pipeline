package pipeline

import "context"

// Queue is the interface that abstracts the message queue backend
// for scaling the pipeline across goroutines and nodes.
type Queue interface {
	// Publish sends a PipelineMessage to the specified topic
	Publish(ctx context.Context, topic string, msg PipelineMessage) error

	// Subscribe starts a consumer on the given topic. The consumerGroup
	// allows multiple nodes to process messages from the same topic
	// in a load-balanced way.
	// Returns a channel that receives messages and an error if subscription fails.
	Subscribe(ctx context.Context, topic string, consumerGroup string) (<-chan PipelineMessage, error)

	// Close terminates the connection to the queue and cleans up resources.
	Close() error
}
