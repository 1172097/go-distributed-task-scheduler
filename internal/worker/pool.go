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
	"strconv"
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

	MetricsSampleInterval time.Duration

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
	go p.sampleDepths(ctx)
	for i := 0; i < p.PoolSize; i++ {
		go func() {
			for {
				for _, pri := range priorities {
					payload, err := p.Q.BRPopLPush(ctx, pri, 2*time.Second, p.VisTimeout)
					if err != nil || payload == "" {
						continue
					}
					metrics.IncTasksDequeued(pri)

					func() {
						var t map[string]any
						_ = json.Unmarshal([]byte(payload), &t)
						ttype, _ := t["type"].(string)
						if ttype == "" {
							ttype = "unknown"
						}
						taskID, _ := t["task_id"].(string)
						if enqAt, ok := parseEnqueuedAt(t["enqueued_at"]); ok {
							metrics.ObserveEnqueueToStart(pri, time.Since(enqAt))
						}
						// mark running
						p.DB.Exec(ctx, `UPDATE tasks SET status='running', attempts=attempts+1, updated_at=now() WHERE id=$1`, taskID)

						// run
						handler, ok := p.Handlers[ttype]
						runStarted := time.Now()
						if !ok {
							err = fmt.Errorf("no handler for type %s", ttype)
						} else {
							err = handler(ctx, t)
						}
						metrics.ObserveTaskRun(ttype, time.Since(runStarted))
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
								metrics.TasksSucceeded.WithLabelValues(ttype, pri).Inc()
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
									metrics.TasksFailed.WithLabelValues(ttype, pri).Inc()
									p.DB.Exec(ctx, `UPDATE tasks SET status='failed', updated_at=now() WHERE id=$1`, taskID)
								}
								return
							}

							if attempts >= maxRetries {
								// Exhausted retries → DLQ (only if still owned)
								if owned, _ := p.Q.AckOwned(ctx, payload); owned {
									_ = p.Q.C.RPush(ctx, p.Q.KeyDLQ(pri), payload).Err()
									metrics.TasksFailed.WithLabelValues(ttype, pri).Inc()
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
				zr := &redis.ZRangeBy{Min: "-inf", Max: fmt.Sprintf("%d", nowMs), Offset: 0, Count: int64(p.RetryBatchSize)}
				items, err := p.Q.C.ZRangeByScore(ctx, p.Q.KeyRetry(pri), zr).Result()
				if err != nil || len(items) == 0 {
					p.updateGauges(ctx, pri)
					continue
				}
				for _, m := range items {
					removed, _ := p.Q.C.ZRem(ctx, p.Q.KeyRetry(pri), m).Result()
					if removed == 0 {
						continue
					}
					_ = p.Q.Enqueue(ctx, pri, m)
				}
				p.updateGauges(ctx, pri)
			}
			p.updateProcessingGauge(ctx)
		}
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
				p.updateProcessingGauge(ctx)
				continue
			}
			for _, payload := range expired {
				removed, _ := p.Q.C.ZRem(ctx, p.Q.KeyProcDeadlines(), payload).Result()
				if removed == 0 {
					continue
				}
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
			p.updateProcessingGauge(ctx)
		}
	}
}

func (p *Pool) sampleDepths(ctx context.Context) {
	interval := p.MetricsSampleInterval
	if interval <= 0 {
		interval = 2 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, pri := range priorities {
				p.updateGauges(ctx, pri)
			}
			p.updateProcessingGauge(ctx)
		}
	}
}

func (p *Pool) updateGauges(ctx context.Context, pri string) {
	if depth, err := p.Q.QueueDepth(ctx, pri); err == nil {
		metrics.SetQueueDepth(pri, float64(depth))
	}
	if retryDepth, err := p.Q.RetryDepth(ctx, pri); err == nil {
		metrics.SetRetryDepth(pri, float64(retryDepth))
	}
	if dlqDepth, err := p.Q.DLQDepth(ctx, pri); err == nil {
		metrics.SetDLQDepth(pri, float64(dlqDepth))
	}
}

func (p *Pool) updateProcessingGauge(ctx context.Context) {
	if proc, err := p.Q.ProcessingDepth(ctx); err == nil {
		metrics.SetProcessingInflight(float64(proc))
	}
}
func parseEnqueuedAt(raw any) (time.Time, bool) {
	switch v := raw.(type) {
	case float64:
		return time.UnixMilli(int64(v)), true
	case int64:
		return time.UnixMilli(v), true
	case int:
		return time.UnixMilli(int64(v)), true
	case string:
		if v == "" {
			return time.Time{}, false
		}
		if ms, err := strconv.ParseInt(v, 10, 64); err == nil {
			return time.UnixMilli(ms), true
		}
	}
	return time.Time{}, false
}
