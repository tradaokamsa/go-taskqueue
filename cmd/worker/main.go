package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

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

	// Connect to database
	db, err := store.NewPostgresStore(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	slog.Info("connected to database")

	// Connect to Redis
	q, err := queue.NewRedisQueue(ctx, cfg.RedisURL)
	if err != nil {
		slog.Error("failed to connect to redis", "error", err)
		os.Exit(1)
	}
	defer func() { _ = q.Close() }()
	slog.Info("connected to redis")

	// Create executor
	executor := worker.NewFakeExecutor()

	// Start worker pool
	pool := worker.NewWorkerPool(cfg.WorkerCount, q, db, executor)
	pool.Start(ctx)

	slog.Info("worker started", "workers", cfg.WorkerCount)

	// Start metrics server
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("OK"))
		})

		addr := ":" + cfg.WorkerMetricsPort
		slog.Info("starting worker metrics server", "addr", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			slog.Error("worker metrics server error", "error", err)
		}
	}()

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
