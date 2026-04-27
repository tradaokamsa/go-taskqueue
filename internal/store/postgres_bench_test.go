package store

// Benchmarks use testcontainers which adds startup overhead.
// For CI, consider using a persistent database or skipping these tests.

import (
	"context"
	"testing"
	"time"

	"github.com/tradaokamsa/go-taskqueue/internal/api"
	"github.com/tradaokamsa/go-taskqueue/internal/domain"
)

func BenchmarkCreateJob(b *testing.B) {
	store := setupTestDB(&testing.T{})
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		job := &domain.Job{
			Type:       "benchmark",
			Priority:   5,
			Status:     domain.StatusPending,
			MaxRetries: 3,
			TimeoutSec: 3600,
		}
		_ = store.CreateJob(ctx, job)
	}
}

func BenchmarkGetJob(b *testing.B) {
	store := setupTestDB(&testing.T{})
	ctx := context.Background()

	job := &domain.Job{
		Type:       "benchmark",
		Priority:   5,
		Status:     domain.StatusPending,
		MaxRetries: 3,
		TimeoutSec: 3600,
	}
	_ = store.CreateJob(ctx, job)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.GetJob(ctx, job.ID)
	}
}

func BenchmarkListJobs(b *testing.B) {
	store := setupTestDB(&testing.T{})
	ctx := context.Background()

	for i := 0; i < 1000; i++ {
		job := &domain.Job{
			Type:       "benchmark",
			Priority:   i % 10,
			Status:     domain.StatusPending,
			MaxRetries: 3,
			TimeoutSec: 3600,
			CreatedAt:  time.Now(),
		}
		_ = store.CreateJob(ctx, job)
	}

	opts := api.ListOptions{Limit: 20}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = store.ListJobs(ctx, opts)
	}
}
