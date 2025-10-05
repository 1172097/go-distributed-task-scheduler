# Scheduler

A Go-based task scheduling system that accepts jobs over HTTP, stores state in Postgres, and fans work out to a Redis-backed, priority-aware worker pool. Prometheus metrics, alert rules, and a Grafana dashboard are included so you can observe queue health, throughput, retries, and handler latency out of the box.

> ⚠️ **Work In Progress**
>
> This project is under active development. Interfaces, environment variable names, metrics, and dashboard queries may change without notice. Known rough edges / TODOs include:
>
> - Retry & DLQ handling: logic is functional but may evolve (recording richer failure metadata, bulk retry tooling, DLQ reprocessing CLI).
> - Grafana dashboard: current panels use longer PromQL windows; a “realtime” dashboard variant and short-window panels are planned.
> - Logging: plain `log.Printf` (no structured/JSON logger); worker and load-generator logs can be noisy at high throughput.
> - Backoff configuration: defaults exist, but advanced per-task tuning and jitter strategies are not finalized.
> - Operational hardening: no auth, no rate limits, no multi-tenant isolation yet.
> - Test coverage: integration & property tests for queue/retry pathways still to be added.

<!-- > Contributions, issues, and design feedback are welcome while the surface is still settling. -->

## ✨ Features

- Priority queues (`critical`, `high`, `default`, `low`) with Lua-assisted fairness.
- Worker pools powered by goroutines; scale horizontally by running more processes.
- Visibility timeout plus reaper for at-least-once delivery guarantees.
- Exponential backoff with jitter, retries, and per-priority dead-letter queues.
- Idempotency keys and Postgres persistence for reliable delivery semantics.
- Prometheus metrics, alert rules, and a Grafana dashboard for live observability.
- Stress-test load generator with a configurable workload mix.

## 🏗 Architecture at a Glance

- **API service (`cmd/api`)** – Persists incoming tasks (with optional idempotency keys) and enqueues lightweight messages to Redis.
- **Redis queue (`internal/queue`)** – Maintains four priority lists plus processing, retry, and dead-letter collections. Lua scripts ensure the next ready payload is popped atomically across priorities.
- **Worker pool (`cmd/worker`, `internal/worker`)** – Leases work with a visibility timeout, invokes handlers, manages exponential backoff retries, re-enqueues timed-out payloads, and records task status in Postgres.
- **Postgres (`migrations/`)** – Holds the authoritative task record, attempts, idempotency mapping, and per-run history.
- **Observability** – Prometheus metrics, alert rules (`alerts.yml`), and a Grafana dashboard (`grafana/dashboards/observability.json`).

## 📂 Repository Layout

- `cmd/api` – HTTP API for enqueueing tasks and exporting metrics.
- `cmd/worker` – Runnable worker with pool, retry pump, and reaper.
- `cmd/load` – Load generator for stress testing.
- `internal/config` – Environment-driven configuration with sane defaults.
- `internal/queue` – Redis list/ZSET operations and queue metadata helpers.
- `internal/worker` – Pool implementation, backoff math, reaper, retry pump, and metrics sampling.
- `internal/metrics` – Prometheus counters, histograms, and gauges.
- `internal/db` – Postgres connection helpers.
- `migrations/` – SQL schema migrations.
- `docker-compose.yml` – Postgres, Redis, Prometheus, Grafana dev stack.

## 🚀 Quick Start

### Prerequisites

- Go 1.23+
- Docker and GNU Make

### Steps

1. **Bring up infra**
   ```sh
   make dev
   ```
2. **Apply migrations** (run both scripts; `make migrate` streams `001_init.sql`)
   ```sh
   make migrate
   docker exec -i $(docker ps -qf name=postgres) psql -U app -d app < migrations/002_idempotency.sql
   ```
3. **Run API** – HTTP `:8080`, metrics `:2112`
   ```sh
   make api
   ```
4. **Run worker** – Metrics exposed on `:2113`
   ```sh
   make worker
   ```
5. **Enqueue a task**
   ```sh
   curl -s -X POST localhost:8080/tasks \
     -H 'Content-Type: application/json' \
     -d '{
           "type": "noop",
           "priority": "high",
           "payload": {"example": true},
           "idempotency_key": "demo-123"
         }'
   ```
6. **Generate load (optional)**
   ```sh
   go run ./cmd/load -n 2000 -w 8 -pct-flaky 15 -pct-fail 5
   ```

_Scale tip:_ override the environment knobs in the Makefile when you need more throughput or resilience. For example, run more processes or bump `WORKER_POOL`, `RETRY_*`, and other settings inline (`WORKER_POOL=24 RETRY_BATCH_SIZE=250 make worker`). The configuration table below lists every tunable value.

Prometheus scrapes the API (`host.docker.internal:2112`) and worker (`host.docker.internal:2113`) every second. Grafana runs at http://localhost:3000 with the dashboard pre-provisioned.

## ⚙️ Configuration Reference

All binaries read from the environment via `internal/config/config.go`. Defaults aim for local development.

| Variable                  | Default                                                 | Description                                                                   |
| ------------------------- | ------------------------------------------------------- | ----------------------------------------------------------------------------- |
| `POSTGRES_URL`            | `postgres://app:app@localhost:5432/app?sslmode=disable` | Postgres DSN.                                                                 |
| `REDIS_ADDR`              | `localhost:6379`                                        | Redis address.                                                                |
| `REDIS_PASS`              | _(empty)_                                               | Redis password if required.                                                   |
| `METRICS_ADDR`            | `:2112`                                                 | Prometheus metrics bind address (set to `:2113` for the worker via Makefile). |
| `WORKER_POOL`             | `12`                                                    | Goroutines per worker process.                                                |
| `PGPOOL_MAX_CONNS`        | `20`                                                    | Max Postgres connections per worker.                                          |
| `REDIS_POOL_SIZE`         | `50`                                                    | Redis client pool size.                                                       |
| `REDIS_MIN_IDLE`          | `10`                                                    | Minimum idle Redis connections.                                               |
| `VIS_TIMEOUT_MS`          | `60000`                                                 | Visibility timeout in milliseconds.                                           |
| `REAPER_SCAN_INTERVAL_MS` | `2000`                                                  | Reaper cadence in milliseconds.                                               |
| `REAPER_BATCH_SIZE`       | `100`                                                   | Reaper batch size.                                                            |
| `METRICS_SAMPLE_MS`       | `500`                                                   | Gauge sampling interval in milliseconds.                                      |
| `RETRY_BASE_MS`           | `500`                                                   | Retry base backoff.                                                           |
| `RETRY_MAX_MS`            | `30000`                                                 | Retry max backoff.                                                            |
| `RETRY_JITTER_MS`         | `500`                                                   | Backoff jitter.                                                               |
| `RETRY_SCAN_INTERVAL_MS`  | `500`                                                   | Retry pump cadence in milliseconds.                                           |
| `RETRY_BATCH_SIZE`        | `100`                                                   | Retry pump batch size.                                                        |

## 🔄 Task Lifecycle

1. **Enqueue** – API validates input, persists the task, records idempotency metadata, and pushes a lean payload to `q:<priority>` in Redis.
2. **Dispatch** – Worker pops the next ready task across priorities via Lua and places it on the processing list with a visibility deadline.
3. **Execution** – Handler runs with the unmarshalled payload (`noop`, `flaky`, `always_fail`, `sleeper`, or custom logic).
4. **Success** – Worker ACKs the payload, clears the deadline, marks Postgres `status='succeeded'`, and emits success metrics.
5. **Failure** – Attempts and max retries are read from Postgres. Tasks retry with exponential backoff plus jitter until max retries, otherwise they land in the DLQ.
6. **Reaper & Retry Pump** – Background goroutines reclaim expired leases and requeue retry-eligible tasks while keeping gauges current.

## 📊 Metrics & Observability

**Counters** – `tasks_enqueued_total`, `tasks_dequeued_total{priority}`, `tasks_succeeded_total{type,priority}`, `tasks_failed_total{type,priority}`, `tasks_retried_total`, `tasks_reaped_total`, `acks_total`.

**Histograms** – `task_run_seconds{type}`, `enqueue_to_start_seconds{priority}`.

**Gauges** – `queue_depth{priority}`, `retry_set_depth{priority}`, `dlq_depth{priority}`, `processing_inflight`.

Alert examples in `alerts.yml` cover queue saturation, elevated failure rate, and slow p95 handler latency. The Grafana dashboard visualizes queue depth, throughput, success vs failure, retry/DLQ size, handler latency, and in-flight processing.

## 🖥️ Grafana Setup

1. Ensure the dev stack is running (`make dev` starts Grafana alongside Postgres/Redis/Prometheus).
2. Visit http://localhost:3000 and log in with the default credentials (`admin`/`admin`).
3. Grafana immediately prompts you to set a new password—follow the prompt.
4. A Prometheus data source is pre-provisioned (points to `http://prometheus:9090` inside Docker, which resolves to Prometheus).
5. Open the **Scheduler / Observability** dashboard (already imported from `grafana/dashboards/observability.json`) to visualize queue depth, retries, latency, and worker health.

## 🖼️ Screenshots

Below are example Grafana dashboards visualizing queue health, throughput, retries, and latency:

![Grafana Overview 1](docs/images/grafana-overview-1.png)
![Grafana Overview 2](docs/images/grafana-overview-2.png)

## 🧪 Demo & Stress Test

Tune the load generator to explore different mixes:

```sh
go run ./cmd/load \
  -n 10000 -w 16 \
  -pct-noop 80 \
  -pct-flaky 10 \
  -pct-fail 5 \
  -pct-sleep 5 \
  -sleep-ms 80000 \
  -flaky-attempts 2
```

This simulates:

- `noop` – Instant success.
- `flaky` – Fails the first _N_ attempts then succeeds.
- `always_fail` – Permanent failure → DLQ.
- `sleeper` – Sleeps longer than the visibility timeout → reaper reclaim.

Scale worker processes and goroutines, then watch queue depth, throughput, retries, and DLQ growth in Grafana.

## 📈 Performance Snapshot

Example local run with 3 worker processes × 12 goroutines each:

| Burst | Workers × Goroutines | In-flight | Drain Time | Throughput | p95 Run | p95 Wait | Fail % | Retries | Reaped |
| ----- | -------------------- | --------- | ---------- | ---------- | ------- | -------- | ------ | ------- | ------ |
| 10k   | 3 × 12               | ~36       | 22s        | ~450/s     | 25 ms   | 0.8 s    | 2%     | 180     | 12     |

## 🔮 Future Work

- Multi-tenant quotas and rate limiting to prevent noisy neighbors.
- DAG-style workflows with fan-out/fan-in dependencies.
- Autoscaling policies tied to queue depth and handler latency.
- Kubernetes deployment manifests.
- gRPC-based worker communication channel.

## 🎯 Why It Matters

This project demonstrates systems-level backend skills (concurrency, retries, DLQs, reapers), infrastructure thinking (Redis patterns, DB pooling, idempotency), and an observability mindset (Prometheus histograms, Grafana dashboards, alerts). It highlights the ability to build distributed, reliable, observable systems end to end.
