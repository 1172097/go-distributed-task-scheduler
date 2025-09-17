package db

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

func MustOpen(ctx context.Context, url string, maxConns int) *pgxpool.Pool {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		panic(err)
	}
	if maxConns > 0 {
		cfg.MaxConns = int32(maxConns)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		panic(err)
	}
	if err := pool.Ping(ctx); err != nil {
		panic(err)
	}
	return pool
}
