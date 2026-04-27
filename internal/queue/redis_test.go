package queue

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/tradaokamsa/go-taskqueue/internal/domain"
)

func setupTestRedis(t *testing.T) *RedisQueue {
	t.Helper()
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "redis:7-alpine",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start redis container: %v", err)
	}

	t.Cleanup(func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("failed to terminate redis container: %v", err)
		}
	})

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get container host: %v", err)
	}
	port, err := container.MappedPort(ctx, "6379")
	if err != nil {
		t.Fatalf("failed to get container port: %v", err)
	}

	redisURL := "redis://" + host + ":" + port.Port()

	queue, err := NewRedisQueue(ctx, redisURL)
	if err != nil {
		t.Fatalf("failed to create RedisQueue: %v", err)
	}

	t.Cleanup(func() { _ = queue.Close() })

	return queue
}

func TestEnqueueDequeue(t *testing.T) {
	queue := setupTestRedis(t)
	ctx := context.Background()

	job := &domain.Job{
		ID:        "job-1",
		Priority:  5,
		CreatedAt: time.Now(),
	}

	err := queue.Enqueue(ctx, job)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	length, err := queue.Len(ctx)
	if err != nil {
		t.Fatalf("Len failed: %v", err)
	}
	if length != 1 {
		t.Errorf("expected queue length 1, got %d", length)
	}

	dequeued, err := queue.Dequeue(ctx, "worker-1")
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}
	if dequeued == nil {
		t.Fatal("expected a job, got nil")
	}
	if dequeued.ID != job.ID {
		t.Errorf("expected job ID %s, got %s", job.ID, dequeued.ID)
	}

	length, _ = queue.Len(ctx)
	if length != 0 {
		t.Errorf("expected queue length 0 after dequeue, got %d", length)
	}

	processing, _ := queue.ProcessingCount(ctx)
	if processing != 1 {
		t.Errorf("expected processing count 1, got %d", processing)
	}
}

func TestPriorityOrdering(t *testing.T) {
	queue := setupTestRedis(t)
	ctx := context.Background()

	now := time.Now()

	jobs := []*domain.Job{
		{ID: "low", Priority: 1, CreatedAt: now},
		{ID: "high", Priority: 10, CreatedAt: now},
		{ID: "medium", Priority: 5, CreatedAt: now},
	}

	for _, job := range jobs {
		if err := queue.Enqueue(ctx, job); err != nil {
			t.Fatalf("Enqueue failed: %v", err)
		}
	}

	expected := []string{"high", "medium", "low"}
	for _, exexpectedID := range expected {
		job, err := queue.Dequeue(ctx, "worker-1")
		if err != nil {
			t.Fatalf("Dequeue failed: %v", err)
		}
		if job.ID != exexpectedID {
			t.Errorf("expected job ID %s, got %s", exexpectedID, job.ID)
		}
	}
}

func TestAck(t *testing.T) {
	queue := setupTestRedis(t)
	ctx := context.Background()

	job := &domain.Job{
		ID:        "job-ack",
		Priority:  5,
		CreatedAt: time.Now(),
	}

	if err := queue.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}
	if _, err := queue.Dequeue(ctx, "worker-1"); err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}

	err := queue.Ack(ctx, job.ID)
	if err != nil {
		t.Fatalf("Ack failed: %v", err)
	}

	processing, _ := queue.ProcessingCount(ctx)
	if processing != 0 {
		t.Errorf("expected processing count 0 after ack, got %d", processing)
	}
}

func TestDequeueEmpty(t *testing.T) {
	queue := setupTestRedis(t)
	ctx := context.Background()

	job, err := queue.Dequeue(ctx, "worker-1")
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}
	if job != nil {
		t.Errorf("expected nil when dequeuing from empty queue, got %v", job)
	}
}
