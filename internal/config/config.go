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
	PostgresURL string
	RedisAddr   string
	RedisPass   string
	MetricsAddr string
	WorkerPool  int
	VisTimeoutS int

	RetryBaseMS         int
	RetryMaxMS          int
	RetryJitterMS       int
	RetryScanIntervalMS int
	RetryBatchSize      int
}

func FromEnv() Config {
	return Config{
		PostgresURL: getenv("POSTGRES_URL", "postgres://app:app@localhost:5432/app?sslmode=disable"),
		RedisAddr:   getenv("REDIS_ADDR", "localhost:6379"),
		RedisPass:   os.Getenv("REDIS_PASS"),
		MetricsAddr: getenv("METRICS_ADDR", ":2112"),
		WorkerPool:  atoi(getenv("WORKER_POOL", "8")),
		VisTimeoutS: atoi(getenv("VIS_TIMEOUT_SEC", "60")),

		RetryBaseMS:         atoi(getenv("RETRY_BASE_MS", "500")),
		RetryMaxMS:          atoi(getenv("RETRY_MAX_MS", "30000")),
		RetryJitterMS:       atoi(getenv("RETRY_JITTER_MS", "500")),
		RetryScanIntervalMS: atoi(getenv("RETRY_SCAN_INTERVAL_MS", "500")),
		RetryBatchSize:      atoi(getenv("RETRY_BATCH_SIZE", "100")),
	}
}
func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func atoi(s string) int { var n int; fmt.Sscanf(s, "%d", &n); return n }
