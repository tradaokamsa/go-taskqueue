package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/tradaokamsa/go-taskqueue/internal/api"
	"github.com/tradaokamsa/go-taskqueue/internal/config"
	"github.com/tradaokamsa/go-taskqueue/internal/queue"
	"github.com/tradaokamsa/go-taskqueue/internal/store"
	"github.com/tradaokamsa/go-taskqueue/internal/telemetry"
)

func main() {
	cfg := config.Load()

	if cfg.Env == "production" {
		slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	} else {
		slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	}

	ctx := context.Background()

	shutdownTracer, err := telemetry.InitTracer(ctx, "taskqueue-api", cfg.OTLPEndpoint)
	if err != nil {
		slog.Error("failed to initialize tracer", "error", err)
	} else {
		defer func() { _ = shutdownTracer(ctx) }()
	}

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
	defer func() { _ = q.Close() }()

	slog.Info("connected to redis")

	handler := api.NewHandler(db, q, q.Client())
	router := api.NewRouter(handler)

	srv := &http.Server{
		Addr:    ":" + strconv.Itoa(cfg.Port),
		Handler: router,
	}

	go func() {
		slog.Info("starting server", "port", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	// Wait for interrupt signal to gracefully shutdown the server
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.Info("shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("server forced to shutdown", "error", err)
	}

	slog.Info("server stopped")
}
