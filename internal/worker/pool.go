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

	MetricsSampleInterval time.Duration
}

func (p *Pool) Start(ctx context.Context) {
	// seed jitter randomness
	rand.Seed(time.Now().UnixNano())
	// start background goroutines
	go p.retryPump(ctx)
	go p.reaper(ctx)
	go p.metricsSampler(ctx)
	for i := 0; i < p.PoolSize; i++ {
		go func() {
			for {
				for _, pri := range priorities {
					payload, err := p.Q.BRPopLPush(ctx, pri, 2*time.Second, p.VisTimeout)
					if err != nil || payload == "" {
						continue
					}
					metrics.IncDequeued(pri)
					p.processPayload(ctx, pri, payload)
				}
			}
		}()
	}
}

func (p *Pool) processPayload(ctx context.Context, pri, payload string) {
	var task map[string]any
	if err := json.Unmarshal([]byte(payload), &task); err != nil {
		// invalid payload: best effort to ack and drop
		_, _ = p.Q.AckOwned(ctx, payload)
		return
	}

	taskType, _ := task["type"].(string)
	taskID, _ := task["task_id"].(string)
	priority := extractPriority(task, pri)
	now := time.Now()
	if wait := queueWait(now, task); wait >= 0 {
		metrics.ObserveEnqueueToStart(priority, wait)
	}

	// mark running and increment attempts
	p.DB.Exec(ctx, `UPDATE tasks SET status='running', attempts=attempts+1, updated_at=now() WHERE id=$1`, taskID)

	handler := p.Handlers[taskType]
	var runErr error
	start := time.Now()
	if handler != nil {
		runErr = handler(ctx, task)
	} else {
		runErr = fmt.Errorf("no handler for type %s", taskType)
	}
	metrics.ObserveTaskRun(taskType, time.Since(start))

	if runErr == nil {
		owned, ackErr := p.Q.AckOwned(ctx, payload)
		if ackErr != nil {
			// println("[worker] ack error on success, task_id=", taskID, "err=", ackErr.Error())
			return
		}
		if owned {
			// println("[worker] done task_id=", taskID, "type=", taskType)
			p.DB.Exec(ctx, `UPDATE tasks SET status='succeeded', updated_at=now() WHERE id=$1`, taskID)
			metrics.IncSucceeded(taskType, priority)
		}
		return
	}

	metrics.IncFailed(taskType, priority)
	// println("[worker] FAIL task_id=", taskID, "type=", taskType, "err=", runErr)

	var attempts, maxRetries int
	if qerr := p.DB.QueryRow(ctx, `SELECT attempts, max_retries FROM tasks WHERE id=$1`, taskID).Scan(&attempts, &maxRetries); qerr != nil {
		if owned, _ := p.Q.AckOwned(ctx, payload); owned {
			_ = p.Q.C.RPush(ctx, p.Q.KeyDLQ(pri), payload).Err()
			p.DB.Exec(ctx, `UPDATE tasks SET status='failed', updated_at=now() WHERE id=$1`, taskID)
		}
		return
	}

	if attempts >= maxRetries {
		if owned, _ := p.Q.AckOwned(ctx, payload); owned {
			_ = p.Q.C.RPush(ctx, p.Q.KeyDLQ(pri), payload).Err()
			p.DB.Exec(ctx, `UPDATE tasks SET status='failed', updated_at=now() WHERE id=$1`, taskID)
		}
		return
	}

	backoff := p.computeBackoff(time.Duration(attempts))
	dueAt := time.Now().Add(backoff).UnixMilli()
	if owned, _ := p.Q.AckOwned(ctx, payload); owned {
		_ = p.Q.C.ZAdd(ctx, p.Q.KeyRetry(pri), redis.Z{Score: float64(dueAt), Member: payload}).Err()
		metrics.IncRetried()
		p.DB.Exec(ctx, `UPDATE tasks SET status='queued', updated_at=now() WHERE id=$1`, taskID)
	}
}

func (p *Pool) metricsSampler(ctx context.Context) {
	interval := p.MetricsSampleInterval
	if interval <= 0 {
		interval = 2 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	p.sampleAllGauges(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.sampleAllGauges(ctx)
		}
	}
}

func (p *Pool) sampleAllGauges(ctx context.Context) {
	for _, pri := range priorities {
		p.samplePriorityGauges(ctx, pri)
	}
	p.sampleProcessingGauge(ctx)
}

func (p *Pool) samplePriorityGauges(ctx context.Context, pri string) {
	if depth, err := p.Q.QueueDepth(ctx, pri); err == nil {
		metrics.SetQueueDepth(pri, depth)
	}
	if depth, err := p.Q.RetryDepth(ctx, pri); err == nil {
		metrics.SetRetryDepth(pri, depth)
	}
	if depth, err := p.Q.DLQDepth(ctx, pri); err == nil {
		metrics.SetDLQDepth(pri, depth)
	}
}

func (p *Pool) sampleProcessingGauge(ctx context.Context) {
	if proc, err := p.Q.ProcessingDepth(ctx); err == nil {
		metrics.SetProcessingInflight(proc)
	}
}

func extractPriority(task map[string]any, fallback string) string {
	if v, ok := task["priority"].(string); ok && v != "" {
		return v
	}
	return fallback
}

func queueWait(now time.Time, task map[string]any) time.Duration {
	if ts, ok := asInt64(task["enqueued_at"]); ok {
		wait := now.Sub(time.UnixMilli(ts))
		if wait < 0 {
			return 0
		}
		return wait
	}
	return -1
}

func asInt64(v any) (int64, bool) {
	switch val := v.(type) {
	case float64:
		return int64(val), true
	case int64:
		return val, true
	case int:
		return int64(val), true
	default:
		return 0, false
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
	p.samplePriorityGauges(ctx, pri)
	p.sampleProcessingGauge(ctx)
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
				p.sampleProcessingGauge(ctx)
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
				metrics.IncReaped()
			}
			p.sampleProcessingGauge(ctx)
		}
	}
}
