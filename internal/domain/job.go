package domain

import "time"

type JobStatus string

const (
	StatusPending   JobStatus = "pending"
	StatusScheduled JobStatus = "scheduled"
	StatusRunning   JobStatus = "running"
	StatusCompleted JobStatus = "completed"
	StatusFailed    JobStatus = "failed"
	StatusCancelled JobStatus = "cancelled"
	StatusDead      JobStatus = "dead"
)

type Job struct {
	ID       string
	Type     string
	Priority int
	Status   JobStatus

	Payload     []byte
	Constraints []byte
	Result      []byte
	Error       *string

	MaxRetries int
	Attempt    int
	TimeoutSec int

	WorkerID *string

	CreatedAt   time.Time
	UpdatedAt   time.Time
	ScheduledAt *time.Time
	StartedAt   *time.Time
	CompletedAt *time.Time
}
