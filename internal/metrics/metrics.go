package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// API Metrics
	JobsSubmitted = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "taskqueue_jobs_submitted_total",
			Help: "Total number of jobs submitted",
		},
		[]string{"type"},
	)

	// Worker Metrics
	JobDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "taskqueue_job_duration_seconds",
			Help:    "Job execution duration in seconds",
			Buckets: prometheus.ExponentialBuckets(0.1, 2, 15),
		},
		[]string{"type", "status"},
	)

	JobsProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "taskqueue_jobs_processed_total",
			Help: "Total number of jobs processed",
		},
		[]string{"type", "status"},
	)

	// Queue Metrics
	QueueDepth = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "taskqueue_queue_depth",
			Help: "Current number of jobs in the queue",
		},
	)

	ProcessingCount = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "taskqueue_processing_count",
			Help: "Current number of jobs being processed",
		},
	)

	ActiveWorkers = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "taskqueue_active_workers",
			Help: "Current number of active workers",
		},
	)
)
