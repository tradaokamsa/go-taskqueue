package store

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/tradaokamsa/go-taskqueue/internal/domain"
)

func setupTestDB(t *testing.T) *PostgresStore {
	t.Helper()
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("failed to start postgres container: %v", err)
	}

	t.Cleanup(func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate postgres container: %v", err)
		}
	})

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	t.Cleanup(func() { pool.Close() })

	runMigrations(t, pool)

	return &PostgresStore{pool: pool}
}

func runMigrations(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()

	migrationPath := filepath.Join("..", "..", "migrations", "001_create_jobs.up.sql")
	migrationSQL, err := os.ReadFile(migrationPath)
	if err != nil {
		t.Fatalf("failed to read migration file: %v", err)
	}

	_, err = pool.Exec(ctx, string(migrationSQL))
	if err != nil {
		t.Fatalf("failed to run migration: %v", err)
	}
}

func TestCreateJob(t *testing.T) {
	store := setupTestDB(t)
	ctx := context.Background()

	job := &domain.Job{
		Type:       "test",
		Priority:   5,
		Status:     domain.StatusPending,
		Payload:    []byte(`{"key":"value"}`),
		MaxRetries: 3,
		TimeoutSec: 3600,
	}

	err := store.CreateJob(ctx, job)
	if err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}

	if job.ID == "" {
		t.Fatal("expected job ID to be set")
	}

	if job.CreatedAt.IsZero() {
		t.Fatal("expected CreatedAt to be set")
	}

	fetched, err := store.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("GetJob failed: %v", err)
	}

	if fetched.Type != job.Type {
		t.Fatalf("expected type %s, got %s", job.Type, fetched.Type)
	}

	if fetched.Priority != job.Priority {
		t.Fatalf("expected priority %d, got %d", job.Priority, fetched.Priority)
	}
}

func TestGetJob_NotFound(t *testing.T) {
	store := setupTestDB(t)
	ctx := context.Background()

	_, err := store.GetJob(ctx, "00000000-0000-0000-0000-000000000000")
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if err != domain.ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestUpdateJob(t *testing.T) {
	store := setupTestDB(t)
	ctx := context.Background()

	job := &domain.Job{
		Type:       "test",
		Priority:   5,
		Status:     domain.StatusPending,
		MaxRetries: 3,
		TimeoutSec: 3600,
	}

	err := store.CreateJob(ctx, job)
	if err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}

	// Update the job
	job.Status = domain.StatusRunning
	job.WorkerID = "worker-1"
	now := time.Now()
	job.StartedAt = &now

	err = store.UpdateJob(ctx, job)
	if err != nil {
		t.Fatalf("UpdateJob failed: %v", err)
	}

	// Verify update
	fetched, err := store.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("GetJob failed: %v", err)
	}

	if fetched.Status != domain.StatusRunning {
		t.Errorf("expected status %s, got %s", domain.StatusRunning, fetched.Status)
	}

	if fetched.WorkerID != "worker-1" {
		t.Errorf("expected worker_id worker-1, got %v", fetched.WorkerID)
	}
}
