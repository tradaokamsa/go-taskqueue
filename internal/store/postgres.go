package store

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

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

type cursor struct {
	CreatedAt time.Time `json:"c"`
	ID        string    `json:"i"`
}

func encodeCursor(c cursor) string {
	data, _ := json.Marshal(c)
	return base64.URLEncoding.EncodeToString(data)
}
func decodeCursor(s string) (cursor, error) {
	var c cursor
	data, err := base64.URLEncoding.DecodeString(s)
	if err != nil {
		return c, err
	}
	err = json.Unmarshal(data, &c)
	return c, err
}

func (s *PostgresStore) ListJobs(ctx context.Context, opts api.ListOptions) ([]*domain.Job, string, error) {
	if opts.Limit <= 0 || opts.Limit > 100 {
		opts.Limit = 20
	}

	query := `
		SELECT id, type, priority, status, payload, constraints, result, error,
				max_retries, attempt, timeout_sec, worker_id,
				scheduled_at, started_at, completed_at, created_at, updated_at
		FROM jobs
		WHERE 1=1
    `
	args := []any{}
	argIdx := 1

	if opts.Status != "" {
		query += fmt.Sprintf(" AND status = $%d", argIdx)
		args = append(args, opts.Status)
		argIdx++
	}

	if opts.Type != "" {
		query += fmt.Sprintf(" AND type = $%d", argIdx)
		args = append(args, opts.Type)
		argIdx++
	}

	if opts.Cursor != "" {
		c, err := decodeCursor(opts.Cursor)
		if err == nil {
			query += fmt.Sprintf(" AND (created_at, id) < ($%d, $%d)", argIdx, argIdx+1)
			args = append(args, c.CreatedAt, c.ID)
			argIdx += 2
		}
	}

	query += fmt.Sprintf(" ORDER BY created_at DESC, id DESC LIMIT $%d", argIdx)
	args = append(args, opts.Limit+1)

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, "", fmt.Errorf("store.ListJobs: %w", err)
	}
	defer rows.Close()

	jobs := []*domain.Job{}
	for rows.Next() {
		job := &domain.Job{}
		err := rows.Scan(
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
			&job.ScheduledAt,
			&job.StartedAt,
			&job.CompletedAt,
			&job.CreatedAt,
			&job.UpdatedAt,
		)
		if err != nil {
			return nil, "", fmt.Errorf("store.ListJobs scan: %w", err)
		}
		jobs = append(jobs, job)
	}

	var nextCursor string
	if len(jobs) > opts.Limit {
		jobs = jobs[:opts.Limit]
		last := jobs[len(jobs)-1]
		nextCursor = encodeCursor(cursor{CreatedAt: last.CreatedAt, ID: last.ID})
	}

	return jobs, nextCursor, nil
}

func (s *PostgresStore) CancelJob(ctx context.Context, id string) error {
	query := `                                                                                                   
		UPDATE jobs                                                                                          
		SET status = 'cancelled', updated_at = now()                                                         
		WHERE id = $1 AND status IN ('pending', 'scheduled', 'running')                                      
	`

	result, err := s.pool.Exec(ctx, query, id)
	if err != nil {
		return fmt.Errorf("store.CancelJob: %w", err)
	}

	if result.RowsAffected() == 0 {
		var exist bool
		err := s.pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM jobs WHERE id = $1)", id).Scan(&exist)
		if err != nil {
			return fmt.Errorf("store.CancelJob check exist: %w", err)
		}
		if !exist {
			return domain.ErrNotFound
		}
		return domain.ErrInvalidTransition
	}

	return nil
}

func (s *PostgresStore) RetryJob(ctx context.Context, id string) (*domain.Job, error) {
	query := `
		UPDATE jobs                                                                                          
		SET status = 'pending',
			attempt = attempt + 1,
			error = NULL,
			result = NULL,
			worker_id = NULL,
			started_at = NULL,
			completed_at = NULL,                                                                             
			updated_at = now()
		WHERE id = $1 AND status IN ('failed', 'dead')                                                       
		RETURNING id, type, priority, status, payload, constraints, result, error,                           
				max_retries, attempt, timeout_sec, worker_id,                                              
				scheduled_at, started_at, completed_at, created_at, updated_at                             
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
		&job.ScheduledAt,
		&job.StartedAt,
		&job.CompletedAt,
		&job.CreatedAt,
		&job.UpdatedAt,
	)

	if err != nil {
		if err == pgx.ErrNoRows {
			var exist bool
			checkErr := s.pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM jobs WHERE id = $1)", id).Scan(&exist)
			if checkErr != nil {
				return nil, fmt.Errorf("store.RetryJob check exist: %w", checkErr)
			}
			if !exist {
				return nil, domain.ErrNotFound
			}
			return nil, domain.ErrInvalidTransition
		}
		return nil, fmt.Errorf("store.RetryJob: %w", err)
	}

	return job, nil
}
