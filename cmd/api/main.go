package main

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
	Type     string         `json:"type" binding:"required"`
	Priority string         `json:"priority" binding:"required,oneof=critical high default low"`
	Payload  map[string]any `json:"payload" binding:"required"`
	IdemKey  *string        `json:"idempotency_key"`
}

func main() {
	cfg := config.FromEnv()
	ctx := context.Background()
	pg := db.MustOpen(ctx, cfg.PostgresURL)
	rq := queue.New(cfg.RedisAddr, cfg.RedisPass)
	metrics.MustRegister()

	go func() { _ = http.ListenAndServe(cfg.MetricsAddr, promhttp.Handler()) }()

	r := gin.Default()
	r.POST("/tasks", func(c *gin.Context) {
		var req EnqueueRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}

		// Insert task (idempotency-lite: let duplicates happen for MVP; we’ll harden later)
		var id string
		payloadBytes, _ := json.Marshal(req.Payload)
		err := pg.QueryRow(ctx, `
      INSERT INTO tasks (type, priority, payload) VALUES ($1,$2,$3) RETURNING id`,
			req.Type, req.Priority, payloadBytes).Scan(&id)
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}

		// Enqueue minimal message
		msg := map[string]any{"task_id": id, "type": req.Type, "priority": req.Priority, "enqueued_at": time.Now().UnixMilli()}
		b, _ := json.Marshal(msg)
		if err := rq.Enqueue(ctx, req.Priority, string(b)); err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}

		metrics.TasksEnqueued.Inc()
		c.JSON(202, gin.H{"task_id": id, "status": "queued"})
	})

	r.GET("/healthz", func(c *gin.Context) { c.String(200, "ok") })
	_ = r.Run(":8080")
}
