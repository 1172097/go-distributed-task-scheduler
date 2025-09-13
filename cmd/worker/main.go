package main

import (
	"context"
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
	cfg := config.FromEnv()
	ctx := context.Background()
	pg := db.MustOpen(ctx, cfg.PostgresURL)
	rq := queue.New(cfg.RedisAddr, cfg.RedisPass)
	metrics.MustRegister()
	log.Printf("Worker metrics listening on %s", cfg.MetricsAddr)
	go func() { _ = http.ListenAndServe(cfg.MetricsAddr, promhttp.Handler()) }()

	log.Println("Worker is starting...") // Added log statement

	handlers := map[string]worker.Handler{
		"noop": func(ctx context.Context, t map[string]any) error { time.Sleep(100 * time.Millisecond); return nil },
	}
	p := &worker.Pool{Q: rq, DB: pg, Handlers: handlers, PoolSize: cfg.WorkerPool}
	p.Start(ctx)
	select {}
}
