package worker

import (
	"context"
	"encoding/json"
	"time"

	"github.com/1172097/scheduler/internal/metrics"
	"github.com/1172097/scheduler/internal/queue"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Handler func(ctx context.Context, task map[string]any) error

var priorities = []string{"critical", "high", "default", "low"}

type Pool struct {
	Q        *queue.RedisQ
	DB       *pgxpool.Pool
	Handlers map[string]Handler
	PoolSize int
}

func (p *Pool) Start(ctx context.Context) {
	for i := 0; i < p.PoolSize; i++ {
		go func() {
			for {
				for _, pri := range priorities {
					payload, err := p.Q.BRPopLPush(ctx, pri, 2*time.Second)
					if err != nil || payload == "" {
						continue
					}
					metrics.TasksDequeued.WithLabelValues(pri).Inc()

					var t map[string]any
					_ = json.Unmarshal([]byte(payload), &t)
					ttype := t["type"].(string)
					taskID := t["task_id"].(string)
					// mark running
					p.DB.Exec(ctx, `UPDATE tasks SET status='running', attempts=attempts+1, updated_at=now() WHERE id=$1`, taskID)

					// run
					err = p.Handlers[ttype](ctx, t)
					if err == nil {
						// Success log
						// ...existing code...
						println("[worker] done task_id=", taskID, "type=", ttype)
						p.DB.Exec(ctx, `UPDATE tasks SET status='succeeded', updated_at=now() WHERE id=$1`, taskID)
						metrics.TasksSucceeded.Inc()
						_ = p.Q.Ack(ctx, payload)
					} else {
						// Failure log
						// ...existing code...
						println("[worker] FAIL task_id=", taskID, "type=", ttype, "err=", err)
						p.DB.Exec(ctx, `UPDATE tasks SET status='failed', updated_at=now() WHERE id=$1`, taskID)
						metrics.TasksFailed.Inc()
						_ = p.Q.Ack(ctx, payload) // MVP: no retries yet
					}
				}
			}
		}()
	}
}
