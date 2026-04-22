package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tradaokamsa/go-taskqueue/internal/config"
	"github.com/tradaokamsa/go-taskqueue/internal/queue"
	"github.com/tradaokamsa/go-taskqueue/internal/store"
	"github.com/tradaokamsa/go-taskqueue/internal/worker"
)

func main() {
	cfg := config.Load()

	if cfg.Env == "production" {
		slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	} else {
		slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	}

	ctx := context.Background()

	db, err := store.NewPostgresStore(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	slog.Info("connected to database")

	q, err := queue.NewRedisQueue(ctx, cfg.RedisURL)
	if err != nil {
		slog.Error("failed to connect to redis", "error", err)
		os.Exit(1)
	}
	defer q.Close()
	slog.Info("connected to redis")

	executor := worker.NewFakeExecutor()

	pool := worker.NewWorkerPool(cfg.WorkerCount, q, db, executor)
	pool.Start(ctx)

	slog.Info("worker started", "workers", cfg.WorkerCount)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.Info("shutting down worker...")

	if err := pool.Shutdown(30 * time.Second); err != nil {
		slog.Error("shutdown error", "error", err)
		os.Exit(1)
	}

	slog.Info("worker stopped")
}
