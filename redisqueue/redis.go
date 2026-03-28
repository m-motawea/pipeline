package redisqueue

import (
	"context"
	"encoding/json"

	"github.com/go-redis/redis/v8"
	"github.com/m-motawea/pipeline"
)

// RedisQueue uses Redis Lists to implement the Pipeline Queue interface.
// It uses RPUSH and BLPOP for efficient and reliable queuing that automatically
// load balances amongst multiple subscriber workers on different OS nodes.
type RedisQueue struct {
	client *redis.Client
}

// NewRedisQueue returns an initialized RedisQueue.
func NewRedisQueue(opts *redis.Options) *RedisQueue {
	return &RedisQueue{
		client: redis.NewClient(opts),
	}
}

// Publish serializes the message as JSON and pushes it to the tail of the list.
func (r *RedisQueue) Publish(ctx context.Context, topic string, msg pipeline.PipelineMessage) error {
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return r.client.RPush(ctx, topic, b).Err()
}

// Subscribe returns a channel that receives parsed JSON messages popped from the front of the list.
func (r *RedisQueue) Subscribe(ctx context.Context, topic string, consumerGroup string) (<-chan pipeline.PipelineMessage, error) {
	ch := make(chan pipeline.PipelineMessage)
	go func() {
		defer close(ch)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				// BLPop behaves as a blocking pop to retrieve element from queue
				res, err := r.client.BLPop(ctx, 0, topic).Result()
				if err != nil {
					// If context is canceled, BLPop returns an error - bubble down
					if ctx.Err() != nil {
						return
					}
					continue
				}
				if len(res) == 2 {
					var msg pipeline.PipelineMessage
					if err := json.Unmarshal([]byte(res[1]), &msg); err == nil {
						select {
						case ch <- msg:
						case <-ctx.Done():
							return
						}
					}
				}
			}
		}
	}()
	return ch, nil
}

// Close gracefully stops the redis connections.
func (r *RedisQueue) Close() error {
	return r.client.Close()
}
