package store

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

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
	ctx, span := otel.Tracer("store").Start(ctx, "CreateJob")
	defer span.End()

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
		span.RecordError(err)
		return fmt.Errorf("store.CreateJob: %w", err)
	}

	span.SetAttributes(attribute.String("job.id", job.ID))
	return nil
}

func (s *PostgresStore) GetJob(ctx context.Context, id string) (*domain.Job, error) {
	ctx, span := otel.Tracer("store").Start(ctx, "GetJob")
	defer span.End()

	span.SetAttributes(attribute.String("job.id", id))

	query := `
		SELECT id, type, priority, status, payload, constraints, result, 
				COALESCE(error, '') as error,
				max_retries, attempt, timeout_sec, 
				COALESCE(worker_id, '') as worker_id, 
				created_at, updated_at, scheduled_at, started_at, completed_at
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
		span.RecordError(err)
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
		SELECT id, type, priority, status, payload, constraints, result, 
				COALESCE(error, '') as error,
				max_retries, attempt, timeout_sec, 
				COALESCE(worker_id, '') as worker_id,
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
		RETURNING id, type, priority, status, payload, constraints, result, 
				COALESCE(error, '') as error,                           
				max_retries, attempt, timeout_sec, 
				COALESCE(worker_id, '') as worker_id,                                              
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

// atomically claims a job
func (s *PostgresStore) ClaimJob(ctx context.Context, jobID string, workerID string) (*domain.Job, error) {
	query := `                                                                                                                                        
		UPDATE jobs                                                                                                                               
		SET status = 'running',                                                                                                                   
			worker_id = $2,                                                                                                                       
			started_at = now(),                                                                                                                   
			attempt = attempt + 1,                                                                                                                
			updated_at = now()                                                                                                                    
		WHERE id = $1 AND status IN ('pending', 'scheduled')                                                                                      
		RETURNING id, type, priority, status, payload, constraints, result, 
					COALESCE(error, '') as error,                                                                
					max_retries, attempt, timeout_sec, 
					COALESCE(worker_id, '') as worker_id,                                                                                   
					scheduled_at, started_at, completed_at, created_at, updated_at                                                                  
    `
	job := &domain.Job{}
	err := s.pool.QueryRow(ctx, query, jobID, workerID).Scan(
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
			return nil, domain.ErrAlreadyClaimed
		}
		return nil, fmt.Errorf("store.ClaimJob: %w", err)
	}

	return job, nil
}

func (s *PostgresStore) CompleteJob(ctx context.Context, jobID string, result []byte) error {
	query := `
		UPDATE jobs
		SET status = 'completed',
			result = $2,
			error = NULL,
			completed_at = now(),
			updated_at = now()
		WHERE id = $1 AND status = 'running'
	`
	resultExec, err := s.pool.Exec(ctx, query, jobID, result)
	if err != nil {
		return fmt.Errorf("store.CompleteJob: %w", err)
	}
	if resultExec.RowsAffected() == 0 {
		return domain.ErrInvalidTransition
	}
	return nil
}

func (s *PostgresStore) FailJob(ctx context.Context, jobID string, jobErr string, attempt int, maxRetries int) (shouldRetry bool, err error) {
	if attempt < maxRetries {
		query := `                                                                                                                                
			UPDATE jobs                                                                                                                       
			SET status = 'pending',                                                                                                           
				error = $2,
				worker_id = NULL,                                                                                                             
				started_at = NULL,                                                                                                            
				completed_at = NULL,                                                                                                          
				updated_at = now()                                                                                                            
			WHERE id = $1 AND status = 'running'                                                                                              
		`
		if _, err := s.pool.Exec(ctx, query, jobID, jobErr); err != nil {
			return false, fmt.Errorf("store.FailJob retry: %w", err)
		}
		return true, nil
	} else {
		query := `
		UPDATE jobs                                                                                                                               
		SET status = 'dead',
			error = $2,                                                                                                                           
			completed_at = now(),                                                                                                                 
			updated_at = now()                                                                                                                    
		WHERE id = $1 AND status = 'running'                                                                                                      
    	`
		if _, err := s.pool.Exec(ctx, query, jobID, jobErr); err != nil {
			return false, fmt.Errorf("store.FailJob dead: %w", err)
		}
		return false, nil
	}
}

// Failed job with exponential backoff retry time
func (s *PostgresStore) FailJobWithBackoff(ctx context.Context, jobID string, jobErr string, attempt int, maxRetries int) (shouldRetry bool, err error) {
	if attempt < maxRetries {
		// 10s, 20s, 40s, 80s, ...
		backoff := time.Duration(10*(1<<attempt)) * time.Second
		if backoff > 10*time.Minute {
			backoff = 10 * time.Minute
		}
		retryAt := time.Now().Add(backoff)

		query := `                                                                                                                                
			UPDATE jobs                                                                                                                       
			SET status = 'scheduled',                                                                                                           
				error = $2,
				worker_id = NULL,                                                                                                             
				started_at = NULL,                                                                                                            
				completed_at = NULL,                                                                                                          
				scheduled_at = $3,
				updated_at = now()                                                                                                            
			WHERE id = $1 AND status = 'running'                                                                                              
		`
		if _, err := s.pool.Exec(ctx, query, jobID, jobErr, retryAt); err != nil {
			return false, fmt.Errorf("store.FailJobWithBackoff retry: %w", err)
		}
		return true, nil
	} else {
		query := `
		UPDATE jobs                                                                                                                               
		SET status = 'dead',
			error = $2,                                                                                                                           
			completed_at = now(),                                                                                                                 
			updated_at = now()                                                                                                                    
		WHERE id = $1 AND status = 'running'                                                                                                      
		`
		if _, err := s.pool.Exec(ctx, query, jobID, jobErr); err != nil {
			return false, fmt.Errorf("store.FailJobWithBackoff dead: %w", err)
		}
		return false, nil
	}
}

func (s *PostgresStore) ListDeadJobs(ctx context.Context, limit int, offset int) ([]*domain.Job, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}

	query := `                                                                                                                                        
		SELECT id, type, priority, status, payload, constraints, result, 
				COALESCE(error, '') as error,
				max_retries, attempt, timeout_sec, 
				COALESCE(worker_id, '') as worker_id,                                                                                      
				scheduled_at, started_at, completed_at, created_at, updated_at                                                                     
		FROM jobs                                                                                                                                 
		WHERE status = 'dead'                                                                                                                     
		ORDER BY completed_at DESC                                                                                                                
		LIMIT $1 OFFSET $2
    `
	rows, err := s.pool.Query(ctx, query, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("store.ListDeadJobs: %w", err)
	}
	defer rows.Close()

	var jobs []*domain.Job
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
			return nil, fmt.Errorf("store.ListDeadJobs scan: %w", err)
		}
		jobs = append(jobs, job)
	}

	return jobs, nil
}

type JobStatus struct {
	Pending   int64
	Scheduled int64
	Running   int64
	Completed int64
	Failed    int64
	Cancelled int64
	Dead      int64
}

func (s *PostgresStore) GetJobStats(ctx context.Context) (*api.JobStats, error) {
	query := `
		SELECT
			COUNT(*) FILTER (WHERE status = 'pending') AS pending,
			COUNT(*) FILTER (WHERE status = 'scheduled') AS scheduled,
			COUNT(*) FILTER (WHERE status = 'running') AS running,
			COUNT(*) FILTER (WHERE status = 'completed') AS completed,
			COUNT(*) FILTER (WHERE status = 'failed') AS failed,
			COUNT(*) FILTER (WHERE status = 'cancelled') AS cancelled,
			COUNT(*) FILTER (WHERE status = 'dead') AS dead
		FROM jobs
	`
	stats := &api.JobStats{}
	err := s.pool.QueryRow(ctx, query).Scan(
		&stats.Pending,
		&stats.Scheduled,
		&stats.Running,
		&stats.Completed,
		&stats.Failed,
		&stats.Cancelled,
		&stats.Dead,
	)
	if err != nil {
		return nil, fmt.Errorf("store.GetJobStats: %w", err)
	}
	return stats, nil
}
