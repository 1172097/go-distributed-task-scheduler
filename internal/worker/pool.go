package worker

// TODO:
// - In the worker failure path:
//   Read max_retries for the task (from DB). If attempt >= max_retries: Ack, push to q:{priority}:dlq, update DB failed, inc tasks_failed_total. Else: compute backoff = base*2^(attempt-1)+jitter (clamped), Ack, add to q:{priority}:retry ZSET with score now+backoff, inc tasks_retried_total, optionally set status back to queued.
// - Add a retry pump goroutine scanning each priority's retry ZSET every RETRY_SCAN_INTERVAL_MS, popping up to RETRY_BATCH_SIZE due items and re-enqueuing to main queue. Update gauges for retry_set_depth and dlq_depth periodically (optional).
// - Make backoff parameters configurable with sane defaults (wired via config).

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/1172097/scheduler/internal/metrics"
	"github.com/1172097/scheduler/internal/queue"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type Handler func(ctx context.Context, task map[string]any) error

var priorities = []string{"critical", "high", "default", "low"}

type Pool struct {
	Q        *queue.RedisQ
	DB       *pgxpool.Pool
	Handlers map[string]Handler
	PoolSize int

	// retry/backoff configuration
	RetryBase         time.Duration
	RetryMax          time.Duration
	RetryJitter       time.Duration
	RetryScanInterval time.Duration
	RetryBatchSize    int

	// visibility timeout / reaper configuration
	VisTimeout         time.Duration
	ReaperScanInterval time.Duration
	ReaperBatchSize    int
}

func (p *Pool) Start(ctx context.Context) {
	// seed jitter randomness
	rand.Seed(time.Now().UnixNano())
	// start background goroutines
	go p.retryPump(ctx)
	go p.reaper(ctx)
	for i := 0; i < p.PoolSize; i++ {
		go func() {
			for {
				for _, pri := range priorities {
					payload, err := p.Q.BRPopLPush(ctx, pri, 2*time.Second, p.VisTimeout)
					if err != nil || payload == "" {
						continue
					}
					metrics.TasksDequeued.WithLabelValues(pri).Inc()

					func() {
						var t map[string]any
						_ = json.Unmarshal([]byte(payload), &t)
						ttype, _ := t["type"].(string)
						taskID, _ := t["task_id"].(string)
						// mark running
						p.DB.Exec(ctx, `UPDATE tasks SET status='running', attempts=attempts+1, updated_at=now() WHERE id=$1`, taskID)

						// run
						err = p.Handlers[ttype](ctx, t)
						if err == nil {
							// Success path: finalize only if we still own the lease
							owned, ackErr := p.Q.AckOwned(ctx, payload)
							if ackErr != nil {
								println("[worker] ack error on success, task_id=", taskID, "err=", ackErr.Error())
								return
							}
							if owned {
								println("[worker] done task_id=", taskID, "type=", ttype)
								p.DB.Exec(ctx, `UPDATE tasks SET status='succeeded', updated_at=now() WHERE id=$1`, taskID)
								metrics.TasksSucceeded.Inc()
							}
						} else {
							// Failure path: decide retry or DLQ
							println("[worker] FAIL task_id=", taskID, "type=", ttype, "err=", err)

							// read attempts and max_retries
							var attempts, maxRetries int
							if qerr := p.DB.QueryRow(ctx, `SELECT attempts, max_retries FROM tasks WHERE id=$1`, taskID).Scan(&attempts, &maxRetries); qerr != nil {
								// best effort: if we cannot read, DLQ only if still owned
								if owned, _ := p.Q.AckOwned(ctx, payload); owned {
									_ = p.Q.C.RPush(ctx, p.Q.KeyDLQ(pri), payload).Err()
									metrics.TasksFailed.Inc()
									p.DB.Exec(ctx, `UPDATE tasks SET status='failed', updated_at=now() WHERE id=$1`, taskID)
								}
								return
							}

							if attempts >= maxRetries {
								// Exhausted retries → DLQ (only if still owned)
								if owned, _ := p.Q.AckOwned(ctx, payload); owned {
									_ = p.Q.C.RPush(ctx, p.Q.KeyDLQ(pri), payload).Err()
									metrics.TasksFailed.Inc()
									p.DB.Exec(ctx, `UPDATE tasks SET status='failed', updated_at=now() WHERE id=$1`, taskID)
								}
							} else {
								// Schedule retry with exponential backoff + jitter (only if still owned)
								backoff := p.computeBackoff(time.Duration(attempts))
								dueAt := time.Now().Add(backoff).UnixMilli()
								if owned, _ := p.Q.AckOwned(ctx, payload); owned {
									// add to retry ZSET (score = dueAt)
									_ = p.Q.C.ZAdd(ctx, p.Q.KeyRetry(pri), redis.Z{Score: float64(dueAt), Member: payload}).Err()
									metrics.TasksRetried.Inc()
									// set back to queued (optional)
									p.DB.Exec(ctx, `UPDATE tasks SET status='queued', updated_at=now() WHERE id=$1`, taskID)
								}
							}
						}
					}()
				}
			}
		}()
	}
}

func (p *Pool) computeBackoff(attempt time.Duration) time.Duration {
	// attempt is 1-based (we increment when marking running)
	base := p.RetryBase
	max := p.RetryMax
	jitterMax := p.RetryJitter
	exp := time.Duration(math.Pow(2, float64(attempt-1)))
	backoff := base * exp
	if backoff > max {
		backoff = max
	}
	if jitterMax > 0 {
		j := time.Duration(rand.Int63n(int64(jitterMax)))
		backoff += j
		if backoff > max {
			backoff = max
		}
	}
	return backoff
}

func (p *Pool) retryPump(ctx context.Context) {
	if p.RetryScanInterval <= 0 {
		p.RetryScanInterval = 500 * time.Millisecond
	}
	if p.RetryBatchSize <= 0 {
		p.RetryBatchSize = 100
	}
	ticker := time.NewTicker(p.RetryScanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			nowMs := time.Now().UnixMilli()
			for _, pri := range priorities {
				// fetch due items up to batch size
				zr := &redis.ZRangeBy{Min: "-inf", Max: fmt.Sprintf("%d", nowMs), Offset: 0, Count: int64(p.RetryBatchSize)}
				items, err := p.Q.C.ZRangeByScore(ctx, p.Q.KeyRetry(pri), zr).Result()
				if err != nil || len(items) == 0 {
					// update gauges even if empty
					p.updateGauges(ctx, pri)
					continue
				}
				for _, m := range items {
					// best-effort removal guard to avoid duplicates if multiple pumps (shouldn't happen)
					removed, _ := p.Q.C.ZRem(ctx, p.Q.KeyRetry(pri), m).Result()
					if removed == 0 {
						continue
					}
					// re-enqueue to main queue
					_ = p.Q.Enqueue(ctx, pri, m)
				}
				// update gauges after processing
				p.updateGauges(ctx, pri)
			}
		}
	}
}

func (p *Pool) updateGauges(ctx context.Context, pri string) {
	// retry set depth
	if sz, err := p.Q.C.ZCard(ctx, p.Q.KeyRetry(pri)).Result(); err == nil {
		metrics.RetrySetDepth.WithLabelValues(pri).Set(float64(sz))
	}
	if dlqSz, err := p.Q.C.LLen(ctx, p.Q.KeyDLQ(pri)).Result(); err == nil {
		metrics.DLQDepth.WithLabelValues(pri).Set(float64(dlqSz))
	}
	// processing inflight gauge (not per-priority)
	if proc, err := p.Q.LLen(ctx, p.Q.Processing()); err == nil {
		metrics.ProcessingInflight.Set(float64(proc))
	}
}

func (p *Pool) reaper(ctx context.Context) {
	if p.ReaperScanInterval <= 0 {
		p.ReaperScanInterval = 2 * time.Second
	}
	if p.ReaperBatchSize <= 0 {
		p.ReaperBatchSize = 100
	}
	ticker := time.NewTicker(p.ReaperScanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			nowMs := time.Now().UnixMilli()
			zr := &redis.ZRangeBy{Min: "-inf", Max: fmt.Sprintf("%d", nowMs), Offset: 0, Count: int64(p.ReaperBatchSize)}
			expired, err := p.Q.C.ZRangeByScore(ctx, p.Q.KeyProcDeadlines(), zr).Result()
			if err != nil || len(expired) == 0 {
				// still update gauge
				if proc, err := p.Q.LLen(ctx, p.Q.Processing()); err == nil {
					metrics.ProcessingInflight.Set(float64(proc))
				}
				continue
			}
			for _, payload := range expired {
				// remove from deadlines set first
				removed, _ := p.Q.C.ZRem(ctx, p.Q.KeyProcDeadlines(), payload).Result()
				if removed == 0 {
					continue
				}
				// best-effort remove from processing list
				_, _ = p.Q.C.LRem(ctx, p.Q.Processing(), 1, payload).Result()
				var t map[string]any
				if err := json.Unmarshal([]byte(payload), &t); err != nil {
					continue
				}
				pri, _ := t["priority"].(string)
				if pri == "" {
					continue
				}
				_ = p.Q.Enqueue(ctx, pri, payload)
				metrics.TasksReaped.Inc()
			}
			if proc, err := p.Q.LLen(ctx, p.Q.Processing()); err == nil {
				metrics.ProcessingInflight.Set(float64(proc))
			}
		}
	}
}
