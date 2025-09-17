PG_URL=postgres://app:app@localhost:5432/app?sslmode=disable
REDIS_ADDR=localhost:6379

export WORKER_POOL      ?= 12     # 12 handler goroutines per process
export PGPOOL_MAX_CONNS ?= 10     # bump Postgres pool to avoid throttling
export REDIS_POOL_SIZE  ?= 80     # larger Redis pool (80 sockets)
export REDIS_MIN_IDLE   ?= 20     # keep 20 idle connections ready
export METRICS_SAMPLE_MS ?= 500   # faster sampling for Grafana
export VIS_TIMEOUT_MS   ?= 60000  # 60s visibility timeout
export RETRY_BASE_MS    ?= 500
export RETRY_MAX_MS     ?= 30000
export RETRY_JITTER_MS  ?= 500



dev:
	@echo "Starting postgres+redis"; docker compose up -d
api:
	POSTGRES_URL=$(PG_URL) REDIS_ADDR=$(REDIS_ADDR) go run ./cmd/api
worker:
	POSTGRES_URL=$(PG_URL) REDIS_ADDR=$(REDIS_ADDR) WORKER_POOL=$(WORKER_POOL) METRICS_ADDR=:2113 go run ./cmd/worker

migrate:
	docker exec -i $$(docker ps -qf name=postgres) psql -U app -d app < migrations/001_init.sql

enqueue:
	curl -s -X POST localhost:8080/tasks -H 'Content-Type: application/json' \
	 -d '{"type":"noop","priority":"high","payload":{"n":1}}'
