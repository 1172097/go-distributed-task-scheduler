package metrics

import (
	"math"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	latencyBuckets = []float64{0.01, 0.025, 0.05, 0.1, 0.2, 0.3, 0.5, 0.75, 1, 1.5, 2, 3, 5, 8, 13, 20, 30}

	TasksEnqueued  = prometheus.NewCounter(prometheus.CounterOpts{Name: "tasks_enqueued_total"})
	TasksDequeued  = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "tasks_dequeued_total"}, []string{"priority"})
	TasksSucceeded = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "tasks_succeeded_total"}, []string{"type", "priority"})
	TasksFailed    = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "tasks_failed_total"}, []string{"type", "priority"})
	TasksRetried   = prometheus.NewCounter(prometheus.CounterOpts{Name: "tasks_retried_total"})
	TasksReaped    = prometheus.NewCounter(prometheus.CounterOpts{Name: "tasks_reaped_total"})

	TaskRunSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "task_run_seconds",
		Help:    "Duration of task handler execution.",
		Buckets: latencyBuckets,
	}, []string{"type"})

	EnqueueToStartSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "enqueue_to_start_seconds",
		Help:    "Time spent waiting in the queue before a worker starts processing the task.",
		Buckets: latencyBuckets,
	}, []string{"priority"})

	QueueDepth         = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "queue_depth"}, []string{"priority"})
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
		TaskRunSeconds,
		EnqueueToStartSeconds,
		QueueDepth,
		RetrySetDepth,
		DLQDepth,
		ProcessingInflight,
	)
}

const (
	throughputWindow = 10 * time.Second
	runWindow        = 5 * time.Minute
)

type durationSample struct {
	ts     time.Time
	second float64
}

type aggregator struct {
	mu           sync.Mutex
	queueDepth   map[string]float64
	retryDepth   map[string]float64
	dlqDepth     map[string]float64
	processing   float64
	dequeues     []time.Time
	runDurations []durationSample
}

var agg = &aggregator{
	queueDepth: make(map[string]float64),
	retryDepth: make(map[string]float64),
	dlqDepth:   make(map[string]float64),
}

// SummarySnapshot is a lightweight view of the most recent queue health stats.
type SummarySnapshot struct {
	QueueDepth          map[string]float64 `json:"queue_depth"`
	RetryDepth          map[string]float64 `json:"retry_depth"`
	DLQDepth            map[string]float64 `json:"dlq_depth"`
	ProcessingInflight  float64            `json:"processing_inflight"`
	ThroughputPerSecond float64            `json:"throughput_per_second"`
	P95RunSeconds       float64            `json:"p95_run_seconds"`
}

// SetQueueDepth records the depth of the live queue for a priority.
func SetQueueDepth(priority string, depth float64) {
	QueueDepth.WithLabelValues(priority).Set(depth)
	agg.setQueueDepth(priority, depth)
}

// SetRetryDepth records the number of tasks waiting in the retry set.
func SetRetryDepth(priority string, depth float64) {
	RetrySetDepth.WithLabelValues(priority).Set(depth)
	agg.setRetryDepth(priority, depth)
}

// SetDLQDepth records the dead-letter queue depth.
func SetDLQDepth(priority string, depth float64) {
	DLQDepth.WithLabelValues(priority).Set(depth)
	agg.setDLQDepth(priority, depth)
}

// SetProcessingInflight updates the number of in-flight tasks.
func SetProcessingInflight(depth float64) {
	ProcessingInflight.Set(depth)
	agg.setProcessingInflight(depth)
}

// IncTasksDequeued increments the dequeue counter and tracks the event for summary statistics.
func IncTasksDequeued(priority string) {
	TasksDequeued.WithLabelValues(priority).Inc()
	agg.recordDequeued(time.Now())
}

// ObserveTaskRun records how long a handler took to execute.
func ObserveTaskRun(taskType string, duration time.Duration) {
	if taskType == "" {
		taskType = "unknown"
	}
	TaskRunSeconds.WithLabelValues(taskType).Observe(duration.Seconds())
	agg.recordTaskRun(time.Now(), duration.Seconds())
}

// ObserveEnqueueToStart captures queue latency from enqueue to worker start.
func ObserveEnqueueToStart(priority string, latency time.Duration) {
	if latency < 0 {
		latency = 0
	}
	EnqueueToStartSeconds.WithLabelValues(priority).Observe(latency.Seconds())
}

// Summary returns the most recent snapshot used by the API summary endpoint.
func Summary() SummarySnapshot {
	now := time.Now()
	agg.mu.Lock()
	agg.pruneDequeuesLocked(now)
	agg.pruneRunsLocked(now)
	queue := copyMap(agg.queueDepth)
	retry := copyMap(agg.retryDepth)
	dlq := copyMap(agg.dlqDepth)
	processing := agg.processing
	dequeueCount := len(agg.dequeues)
	durations := make([]float64, len(agg.runDurations))
	for i, sample := range agg.runDurations {
		durations[i] = sample.second
	}
	agg.mu.Unlock()

	throughput := 0.0
	if dequeueCount > 0 {
		throughput = float64(dequeueCount) / throughputWindow.Seconds()
	}

	return SummarySnapshot{
		QueueDepth:          queue,
		RetryDepth:          retry,
		DLQDepth:            dlq,
		ProcessingInflight:  processing,
		ThroughputPerSecond: throughput,
		P95RunSeconds:       calculateP95(durations),
	}
}

func (a *aggregator) setQueueDepth(priority string, depth float64) {
	a.mu.Lock()
	a.queueDepth[priority] = depth
	a.mu.Unlock()
}

func (a *aggregator) setRetryDepth(priority string, depth float64) {
	a.mu.Lock()
	a.retryDepth[priority] = depth
	a.mu.Unlock()
}

func (a *aggregator) setDLQDepth(priority string, depth float64) {
	a.mu.Lock()
	a.dlqDepth[priority] = depth
	a.mu.Unlock()
}

func (a *aggregator) setProcessingInflight(depth float64) {
	a.mu.Lock()
	a.processing = depth
	a.mu.Unlock()
}

func (a *aggregator) recordDequeued(now time.Time) {
	a.mu.Lock()
	a.dequeues = append(a.dequeues, now)
	a.pruneDequeuesLocked(now)
	a.mu.Unlock()
}

func (a *aggregator) recordTaskRun(now time.Time, seconds float64) {
	a.mu.Lock()
	a.runDurations = append(a.runDurations, durationSample{ts: now, second: seconds})
	a.pruneRunsLocked(now)
	a.mu.Unlock()
}

func (a *aggregator) pruneDequeuesLocked(now time.Time) {
	cutoff := now.Add(-throughputWindow)
	idx := 0
	for idx < len(a.dequeues) && a.dequeues[idx].Before(cutoff) {
		idx++
	}
	if idx > 0 {
		copy(a.dequeues, a.dequeues[idx:])
		a.dequeues = a.dequeues[:len(a.dequeues)-idx]
	}
}

func (a *aggregator) pruneRunsLocked(now time.Time) {
	cutoff := now.Add(-runWindow)
	idx := 0
	for idx < len(a.runDurations) && a.runDurations[idx].ts.Before(cutoff) {
		idx++
	}
	if idx > 0 {
		copy(a.runDurations, a.runDurations[idx:])
		a.runDurations = a.runDurations[:len(a.runDurations)-idx]
	}
}

func calculateP95(samples []float64) float64 {
	if len(samples) == 0 {
		return 0
	}
	sorted := append([]float64(nil), samples...)
	sort.Float64s(sorted)
	index := int(math.Ceil(0.95*float64(len(sorted))) - 1)
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

func copyMap(src map[string]float64) map[string]float64 {
	if len(src) == 0 {
		return map[string]float64{}
	}
	dst := make(map[string]float64, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
