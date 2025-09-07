package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	TasksEnqueued  = prometheus.NewCounter(prometheus.CounterOpts{Name: "tasks_enqueued_total"})
	TasksDequeued  = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "tasks_dequeued_total"}, []string{"priority"})
	TasksSucceeded = prometheus.NewCounter(prometheus.CounterOpts{Name: "tasks_succeeded_total"})
	TasksFailed    = prometheus.NewCounter(prometheus.CounterOpts{Name: "tasks_failed_total"})
)

func MustRegister() {
	prometheus.MustRegister(TasksEnqueued, TasksDequeued, TasksSucceeded, TasksFailed)
}
