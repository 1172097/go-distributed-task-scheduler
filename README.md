# Scheduler

This service coordinates API-driven task submission with a Redis-backed work queue and a pool of Go workers. It now exposes production-grade observability so you can understand queue health, latency, and reliability at a glance.

## Running locally

```bash
docker-compose up --build
```

Prometheus will scrape the API (`:2112`) and worker (`:2113`) metric endpoints every two seconds. Grafana is available on `http://localhost:3000` (default credentials `admin`/`admin`). Import the dashboard at [`grafana/dashboard.json`](grafana/dashboard.json) to visualize the metrics described below.

## Observability highlights

- **Histograms**
  - `task_run_seconds{type}` measures handler execution latency across 10ms–30s buckets.
  - `enqueue_to_start_seconds{priority}` captures queueing latency from enqueue until a worker starts processing.
- **Gauges**
  - `queue_depth{priority}`, `retry_set_depth{priority}`, and `dlq_depth{priority}` track backlog health.
  - `processing_inflight` reports the number of tasks currently leased by workers.
- **Counters**
  - Coverage for enqueued, dequeued (by priority), succeeded/failed (by type & priority), retried, and reaped tasks.
- **Sampling**
  - Workers refresh queue, retry, DLQ, and in-flight gauges every `METRICS_SAMPLE_MS` (default 2000ms).
- **API summary**
  - `GET /metrics/summary` returns the latest gauge snapshot plus a lightweight throughput and p95 runtime estimate for UI use without scraping Prometheus directly.


> **Grafana preview**
>
> The dashboard screenshot lives in `docs/grafana-dashboard.b64` so the repository stays text-only. Re-create the PNG locally with:
>
> ```bash
> base64 -d docs/grafana-dashboard.b64 > docs/grafana-dashboard.png
> ```
>
> You can then open the generated `docs/grafana-dashboard.png` or import it into documentation where binary assets are permitted.

## Alerting

Prometheus loads alert rules from [`alerts.yml`](alerts.yml) covering:

- Queue depth exceeding 1000 items for 5 minutes.
- Failure rate spikes (>10% over 5 minutes).
- Slow p95 task execution (>500ms for 5 minutes).
- Large retry surges (>500 retries in 5 minutes).

These alerts target early detection of backlogs, instability, and handler regressions.

## Configuration

Environment knobs relevant to observability:

- `METRICS_SAMPLE_MS`: gauge sampling interval (default `2000`).
- `METRICS_ADDR`: Prometheus scrape address (default `:2112`).
- Existing retry and reaper environment variables remain available (see [`internal/config/config.go`](internal/config/config.go)).

Importing the bundled Grafana dashboard and keeping Prometheus + alerts running ensures parity between local and production monitoring.
