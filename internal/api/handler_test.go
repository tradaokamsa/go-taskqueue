package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/tradaokamsa/go-taskqueue/internal/domain"
)

// --- Mock Store ---
type mockJobStore struct {
	jobs      map[string]*domain.Job
	createErr error
	getErr    error
	cancelErr error
	retryErr  error
	listErr   error
	statsErr  error
	pingErr   error
	deadErr   error
}

func newMockJobStore() *mockJobStore {
	return &mockJobStore{
		jobs: make(map[string]*domain.Job),
	}
}

func (m *mockJobStore) CreateJob(ctx context.Context, job *domain.Job) error {
	if m.createErr != nil {
		return m.createErr
	}
	job.ID = "test-job-id-123"
	m.jobs[job.ID] = job
	return nil
}

func (m *mockJobStore) GetJob(ctx context.Context, id string) (*domain.Job, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	job, ok := m.jobs[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return job, nil
}

func (m *mockJobStore) UpdateJob(ctx context.Context, job *domain.Job) error {
	m.jobs[job.ID] = job
	return nil
}

func (m *mockJobStore) ListJobs(ctx context.Context, opts ListOptions) ([]*domain.Job, string, error) {
	if m.listErr != nil {
		return nil, "", m.listErr
	}
	jobs := make([]*domain.Job, 0, len(m.jobs))
	for _, job := range m.jobs {
		jobs = append(jobs, job)
	}
	return jobs, "", nil
}

func (m *mockJobStore) ListDeadJobs(ctx context.Context, limit int, offset int) ([]*domain.Job, error) {
	if m.deadErr != nil {
		return nil, m.deadErr
	}
	return []*domain.Job{}, nil
}

func (m *mockJobStore) CancelJob(ctx context.Context, id string) error {
	if m.cancelErr != nil {
		return m.cancelErr
	}
	job, ok := m.jobs[id]
	if !ok {
		return domain.ErrNotFound
	}
	job.Status = domain.StatusCancelled
	return nil
}

func (m *mockJobStore) RetryJob(ctx context.Context, id string) (*domain.Job, error) {
	if m.retryErr != nil {
		return nil, m.retryErr
	}
	job, ok := m.jobs[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	job.Status = domain.StatusPending
	job.Attempt++
	return job, nil
}

func (m *mockJobStore) GetJobStats(ctx context.Context) (*JobStats, error) {
	if m.statsErr != nil {
		return nil, m.statsErr
	}
	return &JobStats{Pending: 5, Completed: 10}, nil
}

func (m *mockJobStore) Ping(ctx context.Context) error {
	if m.pingErr != nil {
		return m.pingErr
	}
	return nil
}

// --- Mock Queue ---
type mockQueue struct {
	jobs []string
}

func newMockQueue() *mockQueue {
	return &mockQueue{jobs: make([]string, 0)}
}

func (m *mockQueue) Enqueue(ctx context.Context, job *domain.Job) error {
	m.jobs = append(m.jobs, job.ID)
	return nil
}

func (m *mockQueue) Dequeue(ctx context.Context, workerID string) (*domain.Job, error) {
	return nil, nil
}

// --- Tests ---
func TestSubmitJob_Success(t *testing.T) {
	store := newMockJobStore()
	queue := newMockQueue()
	handler := NewHandler(store, queue, nil)

	body := `{"type":"email","priority":5,"payload":{"to":"test@example.com"}}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.SubmitJob(rec, req)

	if rec.Code != http.StatusCreated {
		t.Errorf("expected status 201, got %d", rec.Code)
	}

	var resp map[string]interface{}
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)

	if resp["id"] == nil {
		t.Error("expected job id in response")
	}
	if resp["type"] != "email" {
		t.Errorf("expected type 'email', got %v", resp["type"])
	}
	if resp["status"] != "pending" {
		t.Errorf("expected status 'pending', got %v", resp["status"])
	}
}

func TestSubmitJob_InvalidJSON(t *testing.T) {
	store := newMockJobStore()
	handler := NewHandler(store, nil, nil)

	body := `{invalid json}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.SubmitJob(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", rec.Code)
	}
}

func TestSubmitJob_MissingType(t *testing.T) {
	store := newMockJobStore()
	handler := NewHandler(store, nil, nil)

	body := `{"priority":5,"payload":{}}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.SubmitJob(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", rec.Code)
	}
}

func TestGetJob_Success(t *testing.T) {
	store := newMockJobStore()
	store.jobs["job-123"] = &domain.Job{
		ID:       "job-123",
		Type:     "email",
		Priority: 5,
		Status:   domain.StatusPending,
	}
	handler := NewHandler(store, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/job-123", nil)
	rec := httptest.NewRecorder()

	// Add chi URL param
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", "job-123")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	handler.GetJob(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var resp map[string]interface{}
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)

	if resp["id"] != "job-123" {
		t.Errorf("expected id 'job-123', got %v", resp["id"])
	}
}

func TestGetJob_NotFound(t *testing.T) {
	store := newMockJobStore()
	handler := NewHandler(store, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/nonexistent", nil)
	rec := httptest.NewRecorder()

	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", "nonexistent")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	handler.GetJob(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", rec.Code)
	}
}

func TestCancelJob_Success(t *testing.T) {
	store := newMockJobStore()
	store.jobs["job-123"] = &domain.Job{
		ID:     "job-123",
		Status: domain.StatusPending,
	}
	handler := NewHandler(store, nil, nil)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/jobs/job-123", nil)
	rec := httptest.NewRecorder()

	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", "job-123")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	handler.CancelJob(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
}

func TestCancelJob_NotFound(t *testing.T) {
	store := newMockJobStore()
	handler := NewHandler(store, nil, nil)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/jobs/nonexistent", nil)
	rec := httptest.NewRecorder()

	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", "nonexistent")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	handler.CancelJob(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", rec.Code)
	}
}

func TestCancelJob_InvalidTransition(t *testing.T) {
	store := newMockJobStore()
	store.cancelErr = domain.ErrInvalidTransition
	store.jobs["job-123"] = &domain.Job{
		ID:     "job-123",
		Status: domain.StatusCompleted,
	}
	handler := NewHandler(store, nil, nil)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/jobs/job-123", nil)
	rec := httptest.NewRecorder()

	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", "job-123")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	handler.CancelJob(rec, req)

	if rec.Code != http.StatusConflict {
		t.Errorf("expected status 409, got %d", rec.Code)
	}
}

func TestHealth(t *testing.T) {
	store := newMockJobStore()
	handler := NewHandler(store, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	handler.Health(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
}

func TestReady(t *testing.T) {
	store := newMockJobStore()
	handler := NewHandler(store, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	rec := httptest.NewRecorder()

	handler.Ready(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
}

func TestStats(t *testing.T) {
	store := newMockJobStore()
	handler := NewHandler(store, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/stats", nil)
	rec := httptest.NewRecorder()

	handler.Stats(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var resp JobStats
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)

	if resp.Pending != 5 {
		t.Errorf("expected pending 5, got %d", resp.Pending)
	}
}

func TestListJobs_Success(t *testing.T) {
	store := newMockJobStore()
	store.jobs["job-1"] = &domain.Job{ID: "job-1", Type: "email", Status: domain.StatusPending}
	store.jobs["job-2"] = &domain.Job{ID: "job-2", Type: "sms", Status: domain.StatusCompleted}
	handler := NewHandler(store, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
	rec := httptest.NewRecorder()

	handler.ListJobs(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var resp map[string]interface{}
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)

	jobs := resp["jobs"].([]interface{})
	if len(jobs) != 2 {
		t.Errorf("expected 2 jobs, got %d", len(jobs))
	}
}

func TestListJobs_WithFilters(t *testing.T) {
	store := newMockJobStore()
	store.jobs["job-1"] = &domain.Job{ID: "job-1", Type: "email", Status: domain.StatusPending}
	handler := NewHandler(store, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs?status=pending&type=email&limit=10", nil)
	rec := httptest.NewRecorder()

	handler.ListJobs(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
}

func TestListDeadJobs_Success(t *testing.T) {
	store := newMockJobStore()
	handler := NewHandler(store, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/dead?limit=10&offset=0", nil)
	rec := httptest.NewRecorder()

	handler.ListDeadJobs(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
}

func TestRetryJob_Success(t *testing.T) {
	store := newMockJobStore()
	store.jobs["job-123"] = &domain.Job{
		ID:     "job-123",
		Type:   "email",
		Status: domain.StatusFailed,
	}
	queue := newMockQueue()
	handler := NewHandler(store, queue, nil)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/job-123/retry", nil)
	rec := httptest.NewRecorder()

	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", "job-123")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	handler.RetryJob(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
}

func TestRetryJob_NotFound(t *testing.T) {
	store := newMockJobStore()
	handler := NewHandler(store, nil, nil)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/nonexistent/retry", nil)
	rec := httptest.NewRecorder()

	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", "nonexistent")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	handler.RetryJob(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", rec.Code)
	}
}

func TestRetryJob_InvalidTransition(t *testing.T) {
	store := newMockJobStore()
	store.retryErr = domain.ErrInvalidTransition
	store.jobs["job-123"] = &domain.Job{
		ID:     "job-123",
		Status: domain.StatusPending,
	}
	handler := NewHandler(store, nil, nil)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/job-123/retry", nil)
	rec := httptest.NewRecorder()

	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", "job-123")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	handler.RetryJob(rec, req)

	if rec.Code != http.StatusConflict {
		t.Errorf("expected status 409, got %d", rec.Code)
	}
}

func TestRetryJob_MissingID(t *testing.T) {
	store := newMockJobStore()
	handler := NewHandler(store, nil, nil)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs//retry", nil)
	rec := httptest.NewRecorder()

	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", "")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	handler.RetryJob(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", rec.Code)
	}
}

func TestCancelJob_MissingID(t *testing.T) {
	store := newMockJobStore()
	handler := NewHandler(store, nil, nil)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/jobs/", nil)
	rec := httptest.NewRecorder()

	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", "")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	handler.CancelJob(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", rec.Code)
	}
}

func TestGetJob_MissingID(t *testing.T) {
	store := newMockJobStore()
	handler := NewHandler(store, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/", nil)
	rec := httptest.NewRecorder()

	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", "")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	handler.GetJob(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", rec.Code)
	}
}

func TestSubmitJob_WithScheduledAt(t *testing.T) {
	store := newMockJobStore()
	handler := NewHandler(store, nil, nil)

	body := `{"type":"email","priority":5,"payload":{},"scheduled_at":"2026-04-30T10:00:00Z"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.SubmitJob(rec, req)

	if rec.Code != http.StatusCreated {
		t.Errorf("expected status 201, got %d", rec.Code)
	}

	var resp map[string]interface{}
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)

	if resp["status"] != "scheduled" {
		t.Errorf("expected status 'scheduled', got %v", resp["status"])
	}
}

func TestSubmitJob_InvalidScheduledAt(t *testing.T) {
	store := newMockJobStore()
	handler := NewHandler(store, nil, nil)

	body := `{"type":"email","payload":{},"scheduled_at":"invalid-date"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.SubmitJob(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", rec.Code)
	}
}

func TestSubmitJob_CreateError(t *testing.T) {
	store := newMockJobStore()
	store.createErr = domain.ErrNotFound
	handler := NewHandler(store, nil, nil)

	body := `{"type":"email","payload":{}}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.SubmitJob(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", rec.Code)
	}
}

func TestGetJob_StoreError(t *testing.T) {
	store := newMockJobStore()
	store.getErr = domain.ErrNotFound
	handler := NewHandler(store, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/job-123", nil)
	rec := httptest.NewRecorder()

	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", "job-123")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	handler.GetJob(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", rec.Code)
	}
}

func TestListJobs_Error(t *testing.T) {
	store := newMockJobStore()
	store.listErr = domain.ErrNotFound
	handler := NewHandler(store, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
	rec := httptest.NewRecorder()

	handler.ListJobs(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", rec.Code)
	}
}

func TestListDeadJobs_Error(t *testing.T) {
	store := newMockJobStore()
	store.deadErr = domain.ErrNotFound
	handler := NewHandler(store, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/dead", nil)
	rec := httptest.NewRecorder()

	handler.ListDeadJobs(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", rec.Code)
	}
}

func TestStats_Error(t *testing.T) {
	store := newMockJobStore()
	store.statsErr = domain.ErrNotFound
	handler := NewHandler(store, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/stats", nil)
	rec := httptest.NewRecorder()

	handler.Stats(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", rec.Code)
	}
}

func TestReady_Unhealthy(t *testing.T) {
	store := newMockJobStore()
	store.pingErr = domain.ErrNotFound
	handler := NewHandler(store, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	rec := httptest.NewRecorder()

	handler.Ready(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected status 503, got %d", rec.Code)
	}
}

func TestCancelJob_InternalError(t *testing.T) {
	store := newMockJobStore()
	store.cancelErr = errors.New("db error")
	store.jobs["job-123"] = &domain.Job{ID: "job-123"}
	handler := NewHandler(store, nil, nil)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/jobs/job-123", nil)
	rec := httptest.NewRecorder()

	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", "job-123")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	handler.CancelJob(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", rec.Code)
	}
}

func TestRetryJob_InternalError(t *testing.T) {
	store := newMockJobStore()
	store.retryErr = errors.New("db error")
	store.jobs["job-123"] = &domain.Job{ID: "job-123"}
	handler := NewHandler(store, nil, nil)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/job-123/retry", nil)
	rec := httptest.NewRecorder()

	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", "job-123")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	handler.RetryJob(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", rec.Code)
	}
}

func TestGetJob_InternalError(t *testing.T) {
	store := newMockJobStore()
	store.getErr = errors.New("db error")
	handler := NewHandler(store, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/job-123", nil)
	rec := httptest.NewRecorder()

	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", "job-123")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	handler.GetJob(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", rec.Code)
	}
}
