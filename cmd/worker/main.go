package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/1172097/scheduler/internal/config"
	"github.com/1172097/scheduler/internal/db"
	"github.com/1172097/scheduler/internal/metrics"
	"github.com/1172097/scheduler/internal/queue"
	"github.com/1172097/scheduler/internal/worker"
)

type attemptTracker struct {
	mu     sync.Mutex
	counts map[string]int
}

func newAttemptTracker() *attemptTracker {
	return &attemptTracker{counts: make(map[string]int)}
}

func (t *attemptTracker) next(taskID string) int {
	if taskID == "" {
		return 1
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	attempt := t.counts[taskID] + 1
	t.counts[taskID] = attempt
	return attempt
}

func (t *attemptTracker) clear(taskID string) {
	if taskID == "" {
		return
	}
	t.mu.Lock()
	delete(t.counts, taskID)
	t.mu.Unlock()
}

func intFromAny(v any, def int) int {
	switch val := v.(type) {
	case float64:
		return int(val)
	case int:
		return val
	case int64:
		return int(val)
	default:
		return def
	}
}

func main() {
	// TODO:
	// - Ensure the worker Pool starts the retry pump goroutine.
	// - Wire the new metrics (registered once).
	// - No change to handler map or API surface.
	cfg := config.FromEnv()
	ctx := context.Background()
	pg := db.MustOpen(ctx, cfg.PostgresURL, cfg.PGPoolMaxConns)
	rq := queue.New(cfg.RedisAddr, cfg.RedisPass, cfg.RedisPoolSize, cfg.RedisMinIdle)
	metrics.MustRegister()
	log.Printf("Worker metrics listening on %s", cfg.MetricsAddr)
	go func() { _ = http.ListenAndServe(cfg.MetricsAddr, promhttp.Handler()) }()

	log.Println("Worker is starting...") // Added log statement

	// handlers := map[string]worker.Handler{
	// 	"noop": func(ctx context.Context, t map[string]any) error { time.Sleep(100 * time.Millisecond); return nil },
	// }
	// handlers := map[string]worker.Handler{
	// 	"fail": func(ctx context.Context, t map[string]any) error { return fmt.Errorf("always fails") },
	// }
	flakyTracker := newAttemptTracker()
	handlers := map[string]worker.Handler{
		"noop": func(ctx context.Context, t map[string]any) error {
			time.Sleep(10 * time.Millisecond)
			return nil
		},
		"always_fail": func(ctx context.Context, t map[string]any) error {
			return fmt.Errorf("always fails")
		},
		"flaky": func(ctx context.Context, t map[string]any) error {
			taskID, _ := t["task_id"].(string)
			failFirst := 2
			if payload, ok := t["payload"].(map[string]any); ok {
				if v, ok := payload["fail_first_n"]; ok {
					if n := intFromAny(v, failFirst); n > 0 {
						failFirst = n
					}
				}
			}
			attempt := flakyTracker.next(taskID)
			if attempt <= failFirst {
				return fmt.Errorf("flaky fail attempt %d of %d", attempt, failFirst)
			}
			flakyTracker.clear(taskID)
			return nil
		},
		"sleeper": func(ctx context.Context, t map[string]any) error {
			sleepMS := 100
			if payload, ok := t["payload"].(map[string]any); ok {
				if v, ok := payload["ms"]; ok {
					sleepMS = intFromAny(v, sleepMS)
				}
			}
			if sleepMS < 0 {
				sleepMS = 0
			}
			time.Sleep(time.Duration(sleepMS) * time.Millisecond)
			return nil
		},
	}

	p := &worker.Pool{
		Q:        rq,
		DB:       pg,
		Handlers: handlers,
		PoolSize: cfg.WorkerPool,
		// retry configuration
		RetryBase:         time.Duration(cfg.RetryBaseMS) * time.Millisecond,
		RetryMax:          time.Duration(cfg.RetryMaxMS) * time.Millisecond,
		RetryJitter:       time.Duration(cfg.RetryJitterMS) * time.Millisecond,
		RetryScanInterval: time.Duration(cfg.RetryScanIntervalMS) * time.Millisecond,
		RetryBatchSize:    cfg.RetryBatchSize,
		// visibility / reaper configuration
		VisTimeout:            time.Duration(cfg.VisTimeoutMS) * time.Millisecond,
		ReaperScanInterval:    time.Duration(cfg.ReaperScanIntervalMS) * time.Millisecond,
		ReaperBatchSize:       cfg.ReaperBatchSize,
		MetricsSampleInterval: time.Duration(cfg.MetricsSampleMS) * time.Millisecond,
	}
	p.Start(ctx)
	select {}
}
