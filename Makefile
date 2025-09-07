PG_URL=postgres://app:app@localhost:5432/app?sslmode=disable
REDIS_ADDR=localhost:6379

dev:
	@echo "Starting postgres+redis"; docker compose up -d
api:
	POSTGRES_URL=$(PG_URL) REDIS_ADDR=$(REDIS_ADDR) go run ./cmd/api
worker:
	POSTGRES_URL=$(PG_URL) REDIS_ADDR=$(REDIS_ADDR) WORKER_POOL=8 METRICS_ADDR=:2113 go run ./cmd/worker

migrate:
	docker exec -i $$(docker ps -qf name=postgres) psql -U app -d app < migrations/001_init.sql

enqueue:
	curl -s -X POST localhost:8080/tasks -H 'Content-Type: application/json' \
	 -d '{"type":"noop","priority":"high","payload":{"n":1}}'
