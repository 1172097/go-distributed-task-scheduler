package queue

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
