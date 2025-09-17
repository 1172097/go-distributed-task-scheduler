package queue

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisQ struct {
	C *redis.Client
}

func New(addr, pass string, poolSize, minIdle int) *RedisQ {
	opts := &redis.Options{Addr: addr, Password: pass}
	if poolSize > 0 {
		opts.PoolSize = poolSize
	}
	if minIdle > 0 {
		opts.MinIdleConns = minIdle
	}
	return &RedisQ{C: redis.NewClient(opts)}
}

func (q *RedisQ) Q(pri string) string        { return "q:" + pri }
func (q *RedisQ) Processing() string         { return "q:processing" }
func (q *RedisQ) KeyProcDeadlines() string   { return "q:processing:deadlines" }
func (q *RedisQ) KeyRetry(pri string) string { return "q:" + pri + ":retry" }
func (q *RedisQ) KeyDLQ(pri string) string   { return "q:" + pri + ":dlq" }

func (q *RedisQ) Enqueue(ctx context.Context, pri, payload string) error {
	payload = ensureMetadata(pri, payload)
	return q.C.RPush(ctx, q.Q(pri), payload).Err()
}

// Blocking pop → processing, also recording a visibility deadline
func (q *RedisQ) BRPopLPush(ctx context.Context, pri string, timeout time.Duration, visTimeout time.Duration) (string, error) {
	payload, err := q.C.BRPopLPush(ctx, q.Q(pri), q.Processing(), timeout).Result()
	if err != nil || payload == "" {
		return payload, err
	}
	deadline := time.Now().Add(visTimeout).UnixMilli()
	_ = q.C.ZAdd(ctx, q.KeyProcDeadlines(), redis.Z{Score: float64(deadline), Member: payload}).Err()
	return payload, nil
}

// Ack removes payload from processing list and deadline set
func (q *RedisQ) Ack(ctx context.Context, payload string) error {
	_, err := q.AckOwned(ctx, payload)
	return err
}

// AckOwned tries to ack the payload and reports whether anything was removed.
// Returns (owned=true) when this worker still held the lease (i.e., the item
// was present in the processing list and/or deadline set). If owned=false,
// another actor (e.g., the reaper) already reclaimed it.
func (q *RedisQ) AckOwned(ctx context.Context, payload string) (bool, error) {
	pipe := q.C.TxPipeline()
	lrem := pipe.LRem(ctx, q.Processing(), 1, payload)
	zrem := pipe.ZRem(ctx, q.KeyProcDeadlines(), payload)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return false, err
	}
	removed := lrem.Val() + zrem.Val()
	return removed > 0, nil
}

// LLen returns the length of the given list key
func (q *RedisQ) LLen(ctx context.Context, key string) (int64, error) {
	return q.C.LLen(ctx, key).Result()
}

func (q *RedisQ) QueueDepth(ctx context.Context, pri string) (int64, error) {
	return q.C.LLen(ctx, q.Q(pri)).Result()
}

func (q *RedisQ) RetryDepth(ctx context.Context, pri string) (int64, error) {
	return q.C.ZCard(ctx, q.KeyRetry(pri)).Result()
}

func (q *RedisQ) DLQDepth(ctx context.Context, pri string) (int64, error) {
	return q.C.LLen(ctx, q.KeyDLQ(pri)).Result()
}

func (q *RedisQ) ProcessingDepth(ctx context.Context) (int64, error) {
	return q.C.LLen(ctx, q.Processing()).Result()
}

func ensureMetadata(pri, payload string) string {
	if payload == "" {
		return payload
	}
	var body map[string]any
	if err := json.Unmarshal([]byte(payload), &body); err != nil {
		return payload
	}
	updated := false
	if pri != "" {
		if v, ok := body["priority"].(string); !ok || v != pri {
			body["priority"] = pri
			updated = true
		}
	}
	enqueueTs := time.Now().UnixMilli()
	if v, ok := body["enqueued_at"].(float64); !ok || int64(v) != enqueueTs {
		body["enqueued_at"] = enqueueTs
		updated = true
	}
	if !updated {
		return payload
	}
	if b, err := json.Marshal(body); err == nil {
		return string(b)
	}
	return payload
}
