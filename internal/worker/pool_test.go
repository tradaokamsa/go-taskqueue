package worker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tradaokamsa/go-taskqueue/internal/domain"
	"github.com/tradaokamsa/go-taskqueue/internal/queue"
)

type mockStore struct {
	jobs map[string]*domain.Job
	mu   sync.RWMutex
}

func newMockStore() *mockStore {
	return &mockStore{jobs: make(map[string]*domain.Job)}
}

func (s *mockStore) GetJob(ctx context.Context, id string) (*domain.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, exists := s.jobs[id]
	if !exists {
		return nil, domain.ErrNotFound
	}
	copy := *job
	return &copy, nil
}

func (s *mockStore) UpdateJob(ctx context.Context, job *domain.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[job.ID] = job
	return nil
}

func (s *mockStore) ClaimJob(ctx context.Context, jobID string, workerID string) (*domain.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, exists := s.jobs[jobID]
	if !exists {
		return nil, domain.ErrNotFound
	}
	if job.Status != domain.StatusPending {
		return nil, domain.ErrAlreadyClaimed
	}
	job.Status = domain.StatusRunning
	job.WorkerID = workerID
	job.Attempt++
	now := time.Now()
	job.StartedAt = &now
	copy := *job
	return &copy, nil
}

func (s *mockStore) CompleteJob(ctx context.Context, jobID string, result []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[jobID]
	if !ok {
		return domain.ErrNotFound
	}
	job.Status = domain.StatusCompleted
	job.Result = result
	return nil
}

func (s *mockStore) FailJob(ctx context.Context, jobID string, jobErr string, attempt int, maxRetries int) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[jobID]
	if !ok {
		return false, domain.ErrNotFound
	}
	if attempt < maxRetries {
		job.Status = domain.StatusPending
		job.Error = jobErr
		return true, nil
	}
	job.Status = domain.StatusDead
	job.Error = jobErr
	return false, nil
}

func (s *mockStore) FailJobWithBackoff(ctx context.Context, jobID string, jobErr string, attempt int, maxRetries int) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[jobID]
	if !ok {
		return false, domain.ErrNotFound
	}
	if attempt < maxRetries {
		job.Status = domain.StatusPending
		job.Error = jobErr
		return true, nil
	}
	job.Status = domain.StatusDead
	job.Error = jobErr
	return false, nil
}

func (s *mockStore) AddJob(job *domain.Job) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[job.ID] = job
}

func (s *mockStore) CountByStatus(status domain.JobStatus) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	count := 0
	for _, job := range s.jobs {
		if job.Status == status {
			count++
		}
	}
	return count
}

type mockQueue struct {
	pending    []string
	processing map[string]string
	mu         sync.Mutex
}

func newMockQueue() *mockQueue {
	return &mockQueue{
		pending:    make([]string, 0),
		processing: make(map[string]string),
	}
}

func (q *mockQueue) Enqueue(ctx context.Context, job *domain.Job) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.pending = append(q.pending, job.ID)
	return nil
}

func (q *mockQueue) Dequeue(ctx context.Context, workerID string) (*domain.Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.pending) == 0 {
		return nil, nil
	}
	jobID := q.pending[0]
	q.pending = q.pending[1:]
	q.processing[jobID] = workerID
	return &domain.Job{ID: jobID}, nil
}

func (q *mockQueue) Ack(ctx context.Context, jobID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.processing, jobID)
	return nil
}

func (q *mockQueue) Nack(ctx context.Context, jobID string, requeue bool, priority int) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.processing, jobID)
	if requeue {
		q.pending = append(q.pending, jobID)
	}
	return nil
}

func (q *mockQueue) Heartbeat(ctx context.Context, jobID string, workerID string) error {
	return nil
}

func (q *mockQueue) GetStuckJobs(ctx context.Context, timeout time.Duration) ([]queue.StuckJob, error) {
	return nil, nil
}

func (q *mockQueue) RemoveFromProcessing(ctx context.Context, jobID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.processing, jobID)
	return nil
}

type fastExecutor struct {
	execCount atomic.Int64
}

func (e *fastExecutor) Execute(ctx context.Context, job *domain.Job) ([]byte, error) {
	e.execCount.Add(1)
	return []byte(`{"ok":true}`), nil
}

func TestWorkerPoolStress(t *testing.T) {
	store := newMockStore()
	q := newMockQueue()
	executor := &fastExecutor{}

	numJobs := 100
	numWorkers := 10

	for i := 0; i < numJobs; i++ {
		job := &domain.Job{
			ID:         fmt.Sprintf("job-%d", i),
			Type:       "stress-test",
			Priority:   i % 10,
			Status:     domain.StatusPending,
			MaxRetries: 3,
			TimeoutSec: 60,
			CreatedAt:  time.Now(),
		}
		store.AddJob(job)
		q.Enqueue(context.Background(), job)
	}

	pool := NewWorkerPool(numWorkers, q, store, executor)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool.Start(ctx)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		completed := store.CountByStatus(domain.StatusCompleted)
		if completed == numJobs {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	pool.Shutdown(5 * time.Second)

	completed := store.CountByStatus(domain.StatusCompleted)
	if completed != numJobs {
		t.Errorf("expected %d completed jobs, got %d", numJobs, completed)
	}

	executions := executor.execCount.Load()
	if executions != int64(numJobs) {
		t.Errorf("expected %d executions, got %d (double-processing)", numJobs, executions)
	}

	metrics := pool.Metrics()
	if metrics.Processed() != int64(numJobs) {
		t.Errorf("metrics: expected %d processed, got %d", numJobs, metrics.Processed())
	}

	t.Logf("Stress test: %d jobs, %d workers, %+v", numJobs, numWorkers, metrics.Snapshot())
}
