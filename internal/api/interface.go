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
	CancelJob(ctx context.Context, id string) error
	RetryJob(ctx context.Context, id string) (*domain.Job, error)
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
