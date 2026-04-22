package api

import (
	"context"

	"github.com/tradaokamsa/go-taskqueue/internal/domain"
)

type JobStore interface {
	CreateJob(ctx context.Context, job *domain.Job) error
	GetJob(ctx context.Context, id string) (*domain.Job, error)
	UpdateJob(ctx context.Context, job *domain.Job) error
	ListJobs(ctx context.Context, opts ListOptions) ([]*domain.Job, string, error)
	ListDeadJobs(ctx context.Context, limit int, offset int) ([]*domain.Job, error)
	CancelJob(ctx context.Context, id string) error
	RetryJob(ctx context.Context, id string) (*domain.Job, error)
	GetJobStats(ctx context.Context) (*JobStats, error)
	Ping(ctx context.Context) error
}

type Queue interface {
	Enqueue(ctx context.Context, job *domain.Job) error
	Dequeue(ctx context.Context, workerID string) (*domain.Job, error)
}

type ListOptions struct {
	Limit  int
	Cursor string
	Status string
	Type   string
}

type JobStats struct {
	Pending   int64 `json:"pending"`
	Scheduled int64 `json:"scheduled"`
	Running   int64 `json:"running"`
	Completed int64 `json:"completed"`
	Failed    int64 `json:"failed"`
	Cancelled int64 `json:"cancelled"`
	Dead      int64 `json:"dead"`
}
