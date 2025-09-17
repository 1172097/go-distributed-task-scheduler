package main

// TODO:
// - No changes in this step (idempotency already added in step 1).

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/1172097/scheduler/internal/config"
	"github.com/1172097/scheduler/internal/db"
	"github.com/1172097/scheduler/internal/metrics"
	"github.com/1172097/scheduler/internal/queue"
)

type EnqueueRequest struct {
	Type       string         `json:"type" binding:"required"`
	Priority   string         `json:"priority" binding:"required,oneof=critical high default low"`
	Payload    map[string]any `json:"payload" binding:"required"`
	IdemKey    *string        `json:"idempotency_key"` // NEW
	MaxRetries *int           `json:"max_retries"`     // optional, used later
}

func main() {
	cfg := config.FromEnv()
	ctx := context.Background()
	pg := db.MustOpen(ctx, cfg.PostgresURL, cfg.PGPoolMaxConns)
	rq := queue.New(cfg.RedisAddr, cfg.RedisPass, cfg.RedisPoolSize, cfg.RedisMinIdle)
	metrics.MustRegister()

	go func() { _ = http.ListenAndServe(cfg.MetricsAddr, promhttp.Handler()) }()

	r := gin.Default()
	r.POST("/tasks", func(c *gin.Context) {
		var req EnqueueRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}

		ctx := c.Request.Context()
		tx, err := pg.Begin(ctx)
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		defer tx.Rollback(ctx)

		// If an idempotency key is provided and we've seen it, return the same task
		var existingTaskID string
		if req.IdemKey != nil {
			if err := tx.QueryRow(ctx, `SELECT task_id FROM idempotency_keys WHERE key=$1`, *req.IdemKey).Scan(&existingTaskID); err == nil {
				// We’ve already created this task for this key
				if err := tx.Commit(ctx); err != nil {
					c.JSON(500, gin.H{"error": err.Error()})
					return
				}
				c.JSON(200, gin.H{"task_id": existingTaskID, "status": "duplicate"})
				return
			}
		}

		// Create a new task
		maxRetries := 5
		if req.MaxRetries != nil {
			maxRetries = *req.MaxRetries
		}

		payloadBytes, _ := json.Marshal(req.Payload)
		var taskID string
		if err := tx.QueryRow(ctx, `
	 		INSERT INTO tasks(type, priority, payload, idempotency_key, max_retries)
	 		VALUES ($1,$2,$3,$4,$5) RETURNING id`,
			req.Type, req.Priority, payloadBytes, req.IdemKey, maxRetries,
		).Scan(&taskID); err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}

		// Record the idempotency mapping (no-op if key is nil)
		if req.IdemKey != nil {
			if _, err := tx.Exec(ctx, `
	 			INSERT INTO idempotency_keys(key, task_id)
	 			VALUES ($1,$2) ON CONFLICT DO NOTHING`,
				*req.IdemKey, taskID,
			); err != nil {
				c.JSON(500, gin.H{"error": err.Error()})
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}

		// Enqueue to Redis (same as before)
		msg := map[string]any{
			"task_id": taskID, "type": req.Type, "priority": req.Priority, "enqueued_at": time.Now().UnixMilli(),
		}
		b, _ := json.Marshal(msg)
		if err := rq.Enqueue(ctx, req.Priority, string(b)); err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			println("[API] Redis enqueue error:", err.Error())
			return
		}

		metrics.IncEnqueued()
		c.JSON(202, gin.H{"task_id": taskID, "status": "queued"})
	})

	r.GET("/healthz", func(c *gin.Context) { c.String(200, "ok") })
	_ = r.Run(":8080")
}
