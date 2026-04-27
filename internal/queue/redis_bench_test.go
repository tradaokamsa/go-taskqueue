package queue

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/tradaokamsa/go-taskqueue/internal/domain"
)

func BenchmarkEnqueue(b *testing.B) {
	queue := setupTestRedis(&testing.T{})
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		job := &domain.Job{
			ID:        fmt.Sprintf("bench-job-%d", i),
			Priority:  5,
			CreatedAt: time.Now(),
		}
		_ = queue.Enqueue(ctx, job)
	}
}

func BenchmarkDequeue(b *testing.B) {
	queue := setupTestRedis(&testing.T{})
	ctx := context.Background()

	// Pre-fill the queue with jobs
	for i := 0; i < b.N; i++ {
		job := &domain.Job{
			ID:        fmt.Sprintf("bench-job-%d", i),
			Priority:  5,
			CreatedAt: time.Now(),
		}
		_ = queue.Enqueue(ctx, job)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = queue.Dequeue(ctx, "bench-worker")
	}
}

func BenchmarkEnqueueDequeue(b *testing.B) {
	queue := setupTestRedis(&testing.T{})
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		job := &domain.Job{
			ID:        fmt.Sprintf("bench-job-%d", i),
			Priority:  5,
			CreatedAt: time.Now(),
		}
		_ = queue.Enqueue(ctx, job)
		_, _ = queue.Dequeue(ctx, "bench-worker")
	}
}
