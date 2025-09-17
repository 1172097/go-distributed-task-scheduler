package config

// TODO:
// - Add backoff parameters configurable via env with sane defaults:
//   RETRY_BASE_MS (default: 500)
//   RETRY_MAX_MS (default: 30000)
//   RETRY_JITTER_MS (default: 500)
//   RETRY_SCAN_INTERVAL_MS (default: 500)
//   RETRY_BATCH_SIZE (default: 100)

import (
	"fmt"
	"os"
)

type Config struct {
	PostgresURL          string
	RedisAddr            string
	RedisPass            string
	MetricsAddr          string
	WorkerPool           int
	PGPoolMaxConns       int
	RedisPoolSize        int
	RedisMinIdle         int
	VisTimeoutMS         int
	ReaperScanIntervalMS int
	ReaperBatchSize      int
	MetricsSampleMS      int

	RetryBaseMS         int
	RetryMaxMS          int
	RetryJitterMS       int
	RetryScanIntervalMS int
	RetryBatchSize      int
}

func FromEnv() Config {
	cfg := Config{
		PostgresURL:          getenv("POSTGRES_URL", "postgres://app:app@localhost:5432/app?sslmode=disable"),
		RedisAddr:            getenv("REDIS_ADDR", "localhost:6379"),
		RedisPass:            os.Getenv("REDIS_PASS"),
		MetricsAddr:          getenv("METRICS_ADDR", ":2112"),
		WorkerPool:           atoi(getenv("WORKER_POOL", "12")),
		PGPoolMaxConns:       atoi(getenv("PGPOOL_MAX_CONNS", "20")),
		RedisPoolSize:        atoi(getenv("REDIS_POOL_SIZE", "50")),
		RedisMinIdle:         atoi(getenv("REDIS_MIN_IDLE", "10")),
		VisTimeoutMS:         atoi(getenv("VIS_TIMEOUT_MS", "60000")),
		ReaperScanIntervalMS: atoi(getenv("REAPER_SCAN_INTERVAL_MS", "2000")),
		ReaperBatchSize:      atoi(getenv("REAPER_BATCH_SIZE", "100")),
		MetricsSampleMS:      atoi(getenv("METRICS_SAMPLE_MS", "500")),

		RetryBaseMS:         atoi(getenv("RETRY_BASE_MS", "500")),
		RetryMaxMS:          atoi(getenv("RETRY_MAX_MS", "30000")),
		RetryJitterMS:       atoi(getenv("RETRY_JITTER_MS", "500")),
		RetryScanIntervalMS: atoi(getenv("RETRY_SCAN_INTERVAL_MS", "500")),
		RetryBatchSize:      atoi(getenv("RETRY_BATCH_SIZE", "100")),
	}
	// basic validation: visibility timeout should exceed handler p95 (~1s)
	const handlerP95MS = 1000
	if cfg.VisTimeoutMS < handlerP95MS {
		cfg.VisTimeoutMS = handlerP95MS
	}
	return cfg
}
func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func atoi(s string) int { var n int; fmt.Sscanf(s, "%d", &n); return n }
