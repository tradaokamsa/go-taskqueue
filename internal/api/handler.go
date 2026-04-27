package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/tradaokamsa/go-taskqueue/internal/domain"
	"github.com/tradaokamsa/go-taskqueue/internal/metrics"
)

type Handler struct {
	store       JobStore
	queue       Queue
	redisClient *redis.Client
}

func NewHandler(store JobStore, queue Queue, redisClient *redis.Client) *Handler {
	return &Handler{
		store:       store,
		queue:       queue,
		redisClient: redisClient,
	}
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if data != nil {
		if err := json.NewEncoder(w).Encode(data); err != nil {
			slog.Error("failed to encode JSON response", "error", err)
		}
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

type SubmitJobRequest struct {
	Type        string          `json:"type"`
	Priority    int             `json:"priority"`
	Payload     json.RawMessage `json:"payload"`
	Constraints json.RawMessage `json:"constraints,omitempty"`
	MaxRetries  int             `json:"max_retries,omitempty"`
	TimeoutSec  int             `json:"timeout_sec,omitempty"`
	ScheduledAt *string         `json:"scheduled_at,omitempty"`
}

type JobResponse struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Priority    int             `json:"priority"`
	Status      string          `json:"status"`
	Payload     json.RawMessage `json:"payload"`
	Constraints json.RawMessage `json:"constraints,omitempty"`
	Result      json.RawMessage `json:"result,omitempty"`
	Error       string          `json:"error,omitempty"`
	MaxRetries  int             `json:"max_retries,omitempty"`
	Attempt     int             `json:"attempt"`
	CreatedAt   string          `json:"created_at"`
	UpdatedAt   string          `json:"updated_at"`
}

func toJobResponse(job *domain.Job) JobResponse {
	return JobResponse{
		ID:          job.ID,
		Type:        job.Type,
		Priority:    job.Priority,
		Status:      string(job.Status),
		Payload:     job.Payload,
		Constraints: job.Constraints,
		Result:      job.Result,
		Error:       job.Error,
		MaxRetries:  job.MaxRetries,
		Attempt:     job.Attempt,
		CreatedAt:   job.CreatedAt.Format(time.RFC3339),
		UpdatedAt:   job.UpdatedAt.Format(time.RFC3339),
	}
}

func (h *Handler) SubmitJob(w http.ResponseWriter, r *http.Request) {
	ctx, span := otel.Tracer("api").Start(r.Context(), "SubmitJob")
	defer span.End()

	var req SubmitJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Type == "" {
		writeError(w, http.StatusBadRequest, "type is required")
		return
	}

	span.SetAttributes(
		attribute.String("job.type", req.Type),
		attribute.Int("job.priority", req.Priority),
	)
	if req.MaxRetries == 0 {
		req.MaxRetries = 3
	}
	if req.TimeoutSec == 0 {
		req.TimeoutSec = 2700
	}

	var scheduledAt *time.Time
	if req.ScheduledAt != nil {
		t, err := time.Parse(time.RFC3339, *req.ScheduledAt)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid scheduled_at format, use RFC3339")
			return
		}
		scheduledAt = &t
	}

	status := domain.StatusPending
	if scheduledAt != nil {
		status = domain.StatusScheduled
	}

	job := &domain.Job{
		Type:        req.Type,
		Priority:    req.Priority,
		Status:      status,
		Payload:     req.Payload,
		Constraints: req.Constraints,
		MaxRetries:  req.MaxRetries,
		TimeoutSec:  req.TimeoutSec,
		ScheduledAt: scheduledAt,
	}

	if err := h.store.CreateJob(ctx, job); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create job")
		return
	}

	span.SetAttributes(attribute.String("job.id", job.ID))

	metrics.JobsSubmitted.WithLabelValues(job.Type).Inc()

	slog.Info("job.submitted",
		"job_id", job.ID,
		"type", job.Type,
		"priority", job.Priority,
	)

	if h.queue != nil {
		if err := h.queue.Enqueue(ctx, job); err != nil {
			slog.Error("failed to enqueue job", "job_id", job.ID, "error", err) // job is in store but failed to enqueue, can be retried later
		}
	}

	writeJSON(w, http.StatusCreated, toJobResponse(job))
}

func (h *Handler) GetJob(w http.ResponseWriter, r *http.Request) {
	ctx, span := otel.Tracer("api").Start(r.Context(), "GetJob")
	defer span.End()

	id := chi.URLParam(r, "id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "job id is required")
		return
	}

	span.SetAttributes(attribute.String("job.id", id))

	job, err := h.store.GetJob(ctx, id)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			writeError(w, http.StatusNotFound, "job not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to get job")
		return
	}
	writeJSON(w, http.StatusOK, toJobResponse(job))
}

func (h *Handler) ListJobs(w http.ResponseWriter, r *http.Request) {
	opts := ListOptions{
		Cursor: r.URL.Query().Get("cursor"),
		Status: r.URL.Query().Get("status"),
		Type:   r.URL.Query().Get("type"),
	}

	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if limit, err := strconv.Atoi(limitStr); err == nil {
			opts.Limit = limit
		}
	}

	jobs, nextCursor, err := h.store.ListJobs(r.Context(), opts)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list jobs")
		return
	}

	jobResponses := make([]JobResponse, len(jobs))
	for i, job := range jobs {
		jobResponses[i] = toJobResponse(job)
	}

	response := map[string]any{
		"jobs": jobResponses,
	}
	if nextCursor != "" {
		response["next_cursor"] = nextCursor
	}

	writeJSON(w, http.StatusOK, response)
}

func (h *Handler) CancelJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "job id is required")
		return
	}

	if err := h.store.CancelJob(r.Context(), id); err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			writeError(w, http.StatusNotFound, "job not found")
			return
		}
		if errors.Is(err, domain.ErrInvalidTransition) {
			writeError(w, http.StatusConflict, "job cannot be cancelled in its current state")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to cancel job")
		return
	}

	slog.Info("job.cancelled", "job_id", id)
	writeJSON(w, http.StatusOK, map[string]string{"status": "cancelled"})
}

func (h *Handler) RetryJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "job id is required")
		return
	}

	job, err := h.store.RetryJob(r.Context(), id)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			writeError(w, http.StatusNotFound, "job not found")
			return
		}
		if errors.Is(err, domain.ErrInvalidTransition) {
			writeError(w, http.StatusConflict, "job cannot be retried in its current state")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to retry job")
		return
	}

	// Re-enqueue if queue is available
	if h.queue != nil {
		if err := h.queue.Enqueue(r.Context(), job); err != nil {
			slog.Error("failed to re-enqueue job", "job_id", job.ID, "error", err) // job is retried in store but failed to enqueue, can be retried later
		}
	}

	slog.Info("job.retried", "job_id", job.ID, "attempt", job.Attempt)
	writeJSON(w, http.StatusOK, toJobResponse(job))
}

func (h *Handler) Stats(w http.ResponseWriter, r *http.Request) {
	stats, err := h.store.GetJobStats(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get stats")
		return
	}

	writeJSON(w, http.StatusOK, stats)
}

func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) Ready(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if err := h.store.Ping(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "unhealthy",
			"error":  "database connection failed",
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "healthy"})
}

func (h *Handler) ListDeadJobs(w http.ResponseWriter, r *http.Request) {
	limit := 20
	offset := 0

	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}
	if offsetStr := r.URL.Query().Get("offset"); offsetStr != "" {
		if o, err := strconv.Atoi(offsetStr); err == nil && o >= 0 {
			offset = o
		}
	}

	jobs, err := h.store.ListDeadJobs(r.Context(), limit, offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list dead jobs")
		return
	}

	JobResponses := make([]JobResponse, len(jobs))
	for i, job := range jobs {
		JobResponses[i] = toJobResponse(job)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"jobs":   JobResponses,
		"limit":  limit,
		"offset": offset,
	})
}
