package localqueue

import (
	"context"
	"sync"

	"github.com/m-motawea/pipeline"
)

// LocalQueue implements pipeline.Queue using Go channels.
// It allows passing messages between components and workers
// running inside the same OS process.
type LocalQueue struct {
	mu     sync.RWMutex
	topics map[string]chan pipeline.PipelineMessage
}

// NewLocalQueue initializes and returns a new LocalQueue
// instance ready for use within the pipeline framework.
func NewLocalQueue() *LocalQueue {
	return &LocalQueue{
		topics: make(map[string]chan pipeline.PipelineMessage),
	}
}

func (l *LocalQueue) getTopicCh(topic string) chan pipeline.PipelineMessage {
	l.mu.RLock()
	ch, ok := l.topics[topic]
	l.mu.RUnlock()
	if ok {
		return ch
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	ch, ok = l.topics[topic]
	if !ok {
		// Buffered channel decouples publish latency
		ch = make(chan pipeline.PipelineMessage, 100)
		l.topics[topic] = ch
	}
	return ch
}

// Publish sends the message into the specified topic's channel.
func (l *LocalQueue) Publish(ctx context.Context, topic string, msg pipeline.PipelineMessage) error {
	ch := l.getTopicCh(topic)
	select {
	case ch <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Subscribe returns the channel tied to the topic. Because the channels
// are shared by all local subscribers, consumerGroup acts strictly as
// a metadata parameter, simulating automatic load balancing of messages.
func (l *LocalQueue) Subscribe(ctx context.Context, topic string, consumerGroup string) (<-chan pipeline.PipelineMessage, error) {
	ch := l.getTopicCh(topic)
	return ch, nil
}

// Close terminates connections and closes all underlying communication channels.
func (l *LocalQueue) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, ch := range l.topics {
		close(ch)
	}
	l.topics = make(map[string]chan pipeline.PipelineMessage)
	return nil
}
