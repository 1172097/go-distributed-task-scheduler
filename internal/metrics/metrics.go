package metrics

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	tasksEnqueued = prometheus.NewCounter(prometheus.CounterOpts{Name: "tasks_enqueued_total"})
	tasksDequeued = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "tasks_dequeued_total", Help: "Total tasks dequeued by priority."},
		[]string{"priority"},
	)
	tasksSucceeded = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "tasks_succeeded_total", Help: "Total successful task executions."},
		[]string{"type", "priority"},
	)
	tasksFailed = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "tasks_failed_total", Help: "Total task execution failures."},
		[]string{"type", "priority"},
	)
	tasksRetried = prometheus.NewCounter(prometheus.CounterOpts{Name: "tasks_retried_total", Help: "Total tasks scheduled for retry."})
	tasksReaped  = prometheus.NewCounter(prometheus.CounterOpts{Name: "tasks_reaped_total", Help: "Total tasks reclaimed by the reaper."})

	taskRunSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{Name: "task_run_seconds", Help: "Histogram of task handler execution time."},
		[]string{"type"},
	)
	enqueueToStartSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{Name: "enqueue_to_start_seconds", Help: "Histogram of time spent waiting in queue by priority."},
		[]string{"priority"},
	)

	queueDepthGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "queue_depth", Help: "Current queue depth by priority."}, []string{"priority"})
	retrySetGauge   = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "retry_set_depth", Help: "Current retry set depth by priority."}, []string{"priority"})
	dlqDepthGauge   = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "dlq_depth", Help: "Current DLQ depth by priority."}, []string{"priority"})
	processingGauge = prometheus.NewGauge(prometheus.GaugeOpts{Name: "processing_inflight", Help: "Number of tasks currently in-flight."})

	registerOnce sync.Once
)

// MustRegister registers all metrics with the global Prometheus registry. Safe to call multiple times.
func MustRegister() {
	registerOnce.Do(func() {
		prometheus.MustRegister(
			tasksEnqueued,
			tasksDequeued,
			tasksSucceeded,
			tasksFailed,
			tasksRetried,
			tasksReaped,
			taskRunSeconds,
			enqueueToStartSeconds,
			queueDepthGauge,
			retrySetGauge,
			dlqDepthGauge,
			processingGauge,
		)
	})
}

// Counter helpers.
func IncEnqueued() { tasksEnqueued.Inc() }

func IncDequeued(priority string) { tasksDequeued.WithLabelValues(priority).Inc() }

func IncSucceeded(taskType, priority string) {
	tasksSucceeded.WithLabelValues(taskType, priority).Inc()
}

func IncFailed(taskType, priority string) { tasksFailed.WithLabelValues(taskType, priority).Inc() }

func IncRetried() { tasksRetried.Inc() }

func IncReaped() { tasksReaped.Inc() }

// Histogram helpers.
func ObserveTaskRun(taskType string, dur time.Duration) {
	if taskType == "" {
		taskType = "unknown"
	}
	taskRunSeconds.WithLabelValues(taskType).Observe(dur.Seconds())
}

func ObserveEnqueueToStart(priority string, dur time.Duration) {
	if priority == "" {
		priority = "unknown"
	}
	if dur < 0 {
		dur = 0
	}
	enqueueToStartSeconds.WithLabelValues(priority).Observe(dur.Seconds())
}

// Gauge helpers.
func SetQueueDepth(priority string, depth int64) {
	queueDepthGauge.WithLabelValues(priority).Set(float64(depth))
}

func SetRetryDepth(priority string, depth int64) {
	retrySetGauge.WithLabelValues(priority).Set(float64(depth))
}

func SetDLQDepth(priority string, depth int64) {
	dlqDepthGauge.WithLabelValues(priority).Set(float64(depth))
}

func SetProcessingInflight(count int64) {
	processingGauge.Set(float64(count))
}
