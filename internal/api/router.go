package api

import (
	"github.com/go-chi/chi/v5"
)

func NewRouter(h *Handler) *chi.Mux {
	r := chi.NewRouter()

	r.Use(RequestID)
	r.Use(Logger)
	r.Use(Recoverer)

	r.Get("/health", h.Health)
	r.Get("/ready", h.Ready)

	r.Route("/api/v1", func(r chi.Router) {
		r.Post("/jobs", h.SubmitJob)
		r.Get("/jobs", h.ListJobs)
		r.Get("/jobs/{id}", h.GetJob)
		r.Delete("/jobs/{id}", h.CancelJob)
		r.Post("/jobs/{id}/retry", h.RetryJob)
		r.Get("/stats", h.Stats)
	})
	return r
}
