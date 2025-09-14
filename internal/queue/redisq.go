package queue

// TODO:
// - Add helpers returning keys:
//   KeyRetry(priority) -> "q:{priority}:retry"
//   KeyDLQ(priority) -> "q:{priority}:dlq"
// - Expose Client if needed for ZSET ops (we already use it elsewhere).
// - Keep existing Enqueue, Ack, Processing helpers unchanged.

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisQ struct{ C *redis.Client }

func New(addr, pass string) *RedisQ {
	return &RedisQ{C: redis.NewClient(&redis.Options{Addr: addr, Password: pass})}
}

func (q *RedisQ) Q(pri string) string { return "q:" + pri }
func (q *RedisQ) Processing() string  { return "q:processing" }

func (q *RedisQ) KeyRetry(pri string) string { return "q:" + pri + ":retry" }
func (q *RedisQ) KeyDLQ(pri string) string   { return "q:" + pri + ":dlq" }

func (q *RedisQ) Enqueue(ctx context.Context, pri, payload string) error {
	return q.C.RPush(ctx, q.Q(pri), payload).Err()
}

// Blocking pop → processing (visibility handled later; MVP keeps it simple)
func (q *RedisQ) BRPopLPush(ctx context.Context, pri string, timeout time.Duration) (string, error) {
	return q.C.BRPopLPush(ctx, q.Q(pri), q.Processing(), timeout).Result()
}

func (q *RedisQ) Ack(ctx context.Context, payload string) error {
	return q.C.LRem(ctx, q.Processing(), 1, payload).Err()
}
