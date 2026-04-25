package api

import (
	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func NewRouter(h *Handler) *chi.Mux {
	r := chi.NewRouter()

	r.Use(RequestID)
	r.Use(Logger)
	r.Use(Recoverer)

	r.Get("/health", h.Health)
	r.Get("/ready", h.Ready)

	r.Handle("/metrics", promhttp.Handler())

	r.Route("/api/v1", func(r chi.Router) {
		if h.redisClient != nil {
			limiter := NewRateLimiter(h.redisClient, 10, 20, "api-rate-limiter") // 10 rps, burst 20
			r.Use(limiter.Middleware)
		}
		r.Post("/jobs", h.SubmitJob)
		r.Get("/jobs", h.ListJobs)
		r.Get("/jobs/dead", h.ListDeadJobs)
		r.Get("/jobs/{id}", h.GetJob)
		r.Delete("/jobs/{id}", h.CancelJob)
		r.Post("/jobs/{id}/retry", h.RetryJob)
		r.Get("/stats", h.Stats)
	})
	return r
}
