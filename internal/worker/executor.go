package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/tradaokamsa/go-taskqueue/internal/domain"
)

type Executor interface {
	Execute(ctx context.Context, job *domain.Job) (result []byte, err error)
}

type FakeExecutor struct{}

func NewFakeExecutor() *FakeExecutor {
	return &FakeExecutor{}
}

func (e *FakeExecutor) Execute(ctx context.Context, job *domain.Job) ([]byte, error) {
	slog.Info("executor.start",
		"job_id", job.ID,
		"type", job.Type,
		"attempt", job.Attempt,
	)

	sleepDuration := time.Duration(job.TimeoutSec/10) * time.Second
	if sleepDuration < time.Second {
		sleepDuration = time.Second
	}
	if sleepDuration > 10*time.Second {
		sleepDuration = 10 * time.Second
	}

	select {
	case <-time.After(sleepDuration):
		// Complete successfully
	case <-ctx.Done():
		// Context cancelled
		return nil, ctx.Err()
	}

	slog.Info("executor.complete",
		"job_id", job.ID,
		"duration", sleepDuration,
	)

	result, _ := json.Marshal(map[string]any{
		"status":       "processed",
		"processed_at": time.Now().Format(time.RFC3339),
	})

	return result, nil
}
