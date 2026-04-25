package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tradaokamsa/go-taskqueue/internal/domain"
)

func TestNewRouter(t *testing.T) {
	store := newMockJobStore()
	store.jobs["job-1"] = &domain.Job{ID: "job-1", Type: "test", Status: domain.StatusPending}
	handler := NewHandler(store, nil, nil)
	router := NewRouter(handler)

	tests := []struct {
		method string
		path   string
		status int
	}{
		{http.MethodGet, "/health", http.StatusOK},
		{http.MethodGet, "/ready", http.StatusOK},
		{http.MethodGet, "/api/v1/jobs", http.StatusOK},
		{http.MethodGet, "/api/v1/jobs/dead", http.StatusOK},
		{http.MethodGet, "/api/v1/stats", http.StatusOK},
		{http.MethodGet, "/metrics", http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.method+" "+tt.path, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			if rec.Code != tt.status {
				t.Errorf("expected status %d, got %d", tt.status, rec.Code)
			}
		})
	}
}

func TestNewRouter_JobEndpoints(t *testing.T) {
	store := newMockJobStore()
	store.jobs["job-123"] = &domain.Job{
		ID:     "job-123",
		Type:   "test",
		Status: domain.StatusPending,
	}
	handler := NewHandler(store, nil, nil)
	router := NewRouter(handler)

	// GET job
	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/job-123", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("GET job: expected 200, got %d", rec.Code)
	}

	// DELETE (cancel) job
	req = httptest.NewRequest(http.MethodDelete, "/api/v1/jobs/job-123", nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("DELETE job: expected 200, got %d", rec.Code)
	}
}

// mockJobStore for router tests - reuse from handler_test.go
type routerMockStore struct {
	jobs map[string]*domain.Job
}

func (m *routerMockStore) CreateJob(ctx context.Context, job *domain.Job) error {
	job.ID = "new-job-id"
	m.jobs[job.ID] = job
	return nil
}
func (m *routerMockStore) GetJob(ctx context.Context, id string) (*domain.Job, error) {
	job, ok := m.jobs[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return job, nil
}
func (m *routerMockStore) UpdateJob(ctx context.Context, job *domain.Job) error {
	m.jobs[job.ID] = job
	return nil
}
func (m *routerMockStore) ListJobs(ctx context.Context, opts ListOptions) ([]*domain.Job, string, error) {
	jobs := make([]*domain.Job, 0)
	for _, j := range m.jobs {
		jobs = append(jobs, j)
	}
	return jobs, "", nil
}
func (m *routerMockStore) ListDeadJobs(ctx context.Context, limit int, offset int) ([]*domain.Job, error) {
	return []*domain.Job{}, nil
}
func (m *routerMockStore) CancelJob(ctx context.Context, id string) error {
	if _, ok := m.jobs[id]; !ok {
		return domain.ErrNotFound
	}
	return nil
}
func (m *routerMockStore) RetryJob(ctx context.Context, id string) (*domain.Job, error) {
	job, ok := m.jobs[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return job, nil
}
func (m *routerMockStore) GetJobStats(ctx context.Context) (*JobStats, error) {
	return &JobStats{}, nil
}
func (m *routerMockStore) Ping(ctx context.Context) error {
	return nil
}
