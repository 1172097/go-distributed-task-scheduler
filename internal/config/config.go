package config

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
}

func FromEnv() Config {
	return Config{
		PostgresURL: getenv("POSTGRES_URL", "postgres://app:app@localhost:5432/app?sslmode=disable"),
		RedisAddr:   getenv("REDIS_ADDR", "localhost:6379"),
		RedisPass:   os.Getenv("REDIS_PASS"),
		MetricsAddr: getenv("METRICS_ADDR", ":2112"),
		WorkerPool:  atoi(getenv("WORKER_POOL", "8")),
		VisTimeoutS: atoi(getenv("VIS_TIMEOUT_SEC", "60")),
	}
}
func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func atoi(s string) int { var n int; fmt.Sscanf(s, "%d", &n); return n }
