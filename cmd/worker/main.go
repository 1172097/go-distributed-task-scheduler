package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/1172097/scheduler/internal/config"
	"github.com/1172097/scheduler/internal/db"
	"github.com/1172097/scheduler/internal/metrics"
	"github.com/1172097/scheduler/internal/queue"
	"github.com/1172097/scheduler/internal/worker"
)

func main() {
	// TODO:
	// - Ensure the worker Pool starts the retry pump goroutine.
	// - Wire the new metrics (registered once).
	// - No change to handler map or API surface.
	cfg := config.FromEnv()
	ctx := context.Background()
	pg := db.MustOpen(ctx, cfg.PostgresURL)
	rq := queue.New(cfg.RedisAddr, cfg.RedisPass)
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
	var failCount int
	handlers := map[string]worker.Handler{
		"fail": func(ctx context.Context, t map[string]any) error { return fmt.Errorf("always fails") },
		"flaky": func(ctx context.Context, t map[string]any) error {
			if failCount < 3 {
				failCount++
				return fmt.Errorf("fail %d", failCount)
			}
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
	}
	p.Start(ctx)
	select {}
}
