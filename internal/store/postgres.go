package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tradaokamsa/go-taskqueue/internal/api"
	"github.com/tradaokamsa/go-taskqueue/internal/domain"
)

var _ api.JobStore = (*PostgresStore)(nil)

type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(ctx context.Context, connString string) (*PostgresStore, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("store.NewPostgresStore: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("store.NewPostgresStore ping: %w", err)
	}
	return &PostgresStore{pool: pool}, nil
}

func (s *PostgresStore) Close() {
	s.pool.Close()
}

func (s *PostgresStore) CreateJob(ctx context.Context, job *domain.Job) error {
	query := `
		INSERT INTO jobs (type, priority, status, payload, constraints, max_retries, timeout_sec, scheduled_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING id, created_at, updated_at
	`
	err := s.pool.QueryRow(ctx, query,
		job.Type,
		job.Priority,
		job.Status,
		job.Payload,
		job.Constraints,
		job.MaxRetries,
		job.TimeoutSec,
		job.ScheduledAt,
	).Scan(&job.ID, &job.CreatedAt, &job.UpdatedAt)
	if err != nil {
		return fmt.Errorf("store.CreateJob: %w", err)
	}
	return nil
}

func (s *PostgresStore) GetJob(ctx context.Context, id string) (*domain.Job, error) {
	query := `
		SELECT id, type, priority, status, payload, constraints, result, error, max_retries, attempt, timeout_sec, worker_id, created_at, updated_at, scheduled_at, started_at, completed_at
		FROM jobs
		WHERE id = $1
	`
	job := &domain.Job{}
	err := s.pool.QueryRow(ctx, query, id).Scan(
		&job.ID,
		&job.Type,
		&job.Priority,
		&job.Status,
		&job.Payload,
		&job.Constraints,
		&job.Result,
		&job.Error,
		&job.MaxRetries,
		&job.Attempt,
		&job.TimeoutSec,
		&job.WorkerID,
		&job.CreatedAt,
		&job.UpdatedAt,
		&job.ScheduledAt,
		&job.StartedAt,
		&job.CompletedAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, domain.ErrNotFound
		}
		return nil, fmt.Errorf("store.GetJob: %w", err)
	}
	return job, nil
}

func (s *PostgresStore) UpdateJob(ctx context.Context, job *domain.Job) error {
	query := `
		UPDATE jobs
		SET status = $2, result = $3, error = $4, attempt = $5, worker_id = $6, started_at = $7, completed_at = $8, updated_at = NOW()
		WHERE id = $1
	`
	result, err := s.pool.Exec(ctx, query,
		job.ID,
		job.Status,
		job.Result,
		job.Error,
		job.Attempt,
		job.WorkerID,
		job.StartedAt,
		job.CompletedAt,
	)
	if err != nil {
		return fmt.Errorf("store.UpdateJob: %w", err)
	}
	if result.RowsAffected() == 0 {
		return domain.ErrNotFound
	}
	return nil
}

func (s *PostgresStore) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}
