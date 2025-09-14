package metrics

// TODO:
// - Register a counter tasks_retried_total.
// - Register gauges (optional but preferred) for:
//   retry_set_depth{priority}
//   dlq_depth{priority}
// - Ensure existing counters remain: enqueued, dequeued, succeeded, failed.
// - No histogram changes in this step.

import "github.com/prometheus/client_golang/prometheus"

var (
	TasksEnqueued  = prometheus.NewCounter(prometheus.CounterOpts{Name: "tasks_enqueued_total"})
	TasksDequeued  = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "tasks_dequeued_total"}, []string{"priority"})
	TasksSucceeded = prometheus.NewCounter(prometheus.CounterOpts{Name: "tasks_succeeded_total"})
	TasksFailed    = prometheus.NewCounter(prometheus.CounterOpts{Name: "tasks_failed_total"})
	TasksRetried   = prometheus.NewCounter(prometheus.CounterOpts{Name: "tasks_retried_total"})
	TasksReaped    = prometheus.NewCounter(prometheus.CounterOpts{Name: "tasks_reaped_total"})

	RetrySetDepth      = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "retry_set_depth"}, []string{"priority"})
	DLQDepth           = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "dlq_depth"}, []string{"priority"})
	ProcessingInflight = prometheus.NewGauge(prometheus.GaugeOpts{Name: "processing_inflight"})
)

func MustRegister() {
	prometheus.MustRegister(
		TasksEnqueued,
		TasksDequeued,
		TasksSucceeded,
		TasksFailed,
		TasksRetried,
		TasksReaped,
		RetrySetDepth,
		DLQDepth,
		ProcessingInflight,
	)
}
