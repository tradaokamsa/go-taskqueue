# go-taskqueue

A production-grade distributed task queue built in Go. It handles asynchronous job scheduling, priority-based execution, and fault-tolerant delivery across concurrent workers.

Think of it as a self-hosted alternative to Sidekiq, Celery, or AWS SQS + Lambda — built from scratch with observability and reliability as first-class concerns.

## Architecture

```
┌──────────────┐       ┌──────────────┐       ┌──────────────────┐
│   Client     │──────▶│   API Server │──────▶│   PostgreSQL     │
│  (HTTP)      │       │   (Chi)      │       │   (Job Store)    │
└──────────────┘       └──────┬───────┘       └────────┬─────────┘
                              │                        │
                              ▼                        │
                       ┌──────────────┐                │
                       │    Redis     │                │
                       │ (Priority Q) │                │
                       └──────┬───────┘                │
                              │                        │
                              ▼                        ▼
                       ┌──────────────────────────────────────┐
                       │          Worker Pool                  │
                       │  ┌────────┐ ┌────────┐ ┌────────┐   │
                       │  │Worker 1│ │Worker 2│ │Worker N│   │
                       │  └────────┘ └────────┘ └────────┘   │
                       │         + Reaper (stuck job recovery) │
                       └──────────────────────────────────────┘
                              │
                              ▼
                       ┌──────────────────┐
                       │  Observability   │
                       │  Prometheus      │
                       │  Grafana         │
                       │  Jaeger (Traces) │
                       └──────────────────┘
```

## Features

**Job Management**
- Submit, cancel, retry, and query jobs via REST API
- Priority-based scheduling (higher priority jobs execute first)
- Scheduled jobs (submit now, execute later)
- Dead letter queue for permanently failed jobs
- Cursor-based pagination for job listing

**Reliability**
- Atomic dequeue using Redis Lua scripts (no race conditions)
- Atomic job claiming via PostgreSQL `UPDATE...RETURNING` (exactly-once processing)
- Exponential backoff retry (10s, 20s, 40s... capped at 10 min)
- Worker heartbeat with stuck job detection and automatic recovery
- Distributed lock on reaper to prevent duplicate execution across instances
- Graceful shutdown with configurable timeout

**Observability**
- OpenTelemetry distributed tracing (exported to Jaeger)
- Prometheus metrics (job duration, queue depth, processing count, active workers, rate limits)
- Pre-built Grafana dashboard
- Structured JSON logging (slog) with request ID correlation

**API Protection**
- Redis-backed token bucket rate limiting (Lua script, per-IP)
- Panic recovery middleware
- Health and readiness endpoints

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Go 1.25 |
| HTTP Router | chi/v5 |
| Database | PostgreSQL 16 (pgxpool connection pool) |
| Queue | Redis 7 (Sorted Sets + Lua scripts) |
| Tracing | OpenTelemetry + Jaeger |
| Metrics | Prometheus + Grafana |
| Container | Multi-stage Docker (distroless, ~12MB) |
| CI/CD | GitHub Actions (lint, test, build, Docker) |
| Testing | Testcontainers, race detector, benchmarks |

## Getting Started

### Prerequisites

- Go 1.25+
- Docker & Docker Compose

### Run Locally

```bash
# Start infrastructure (Postgres, Redis, Prometheus, Grafana, Jaeger)
docker compose up -d

# Run database migrations
psql "postgres://taskqueue:taskqueue@localhost:5432/taskqueue?sslmode=disable" \
  -f ./migrations/001_create_jobs.up.sql

# Start the API server
go run ./cmd/server

# Start the worker (in another terminal)
go run ./cmd/worker
```

### Run with Docker

```bash
# Build the image
docker build -t taskqueue .

# Run API server
docker run -e DATABASE_URL=... -e REDIS_URL=... -p 8081:8081 taskqueue

# Run worker
docker run -e DATABASE_URL=... -e REDIS_URL=... --entrypoint /worker taskqueue
```

## API Reference

Base URL: `http://localhost:8081/api/v1`

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/jobs` | Submit a new job |
| `GET` | `/jobs` | List jobs (supports `?status=`, `?type=`, `?cursor=`, `?limit=`) |
| `GET` | `/jobs/{id}` | Get job details |
| `DELETE` | `/jobs/{id}` | Cancel a job |
| `POST` | `/jobs/{id}/retry` | Retry a failed/dead job |
| `GET` | `/jobs/dead` | List dead letter queue |
| `GET` | `/stats` | Get job statistics by status |

### Submit a Job

```bash
curl -X POST http://localhost:8081/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "type": "email.send",
    "priority": 5,
    "payload": {"to": "user@example.com", "subject": "Hello"},
    "max_retries": 3,
    "timeout_sec": 60
  }'
```

Response:
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "type": "email.send",
  "priority": 5,
  "status": "pending",
  "payload": {"to": "user@example.com", "subject": "Hello"},
  "max_retries": 3,
  "attempt": 0,
  "created_at": "2026-05-16T10:00:00Z",
  "updated_at": "2026-05-16T10:00:00Z"
}
```

### Schedule a Job for Later

```bash
curl -X POST http://localhost:8081/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "type": "report.generate",
    "priority": 1,
    "payload": {"report_id": "monthly-2026-05"},
    "scheduled_at": "2026-05-17T08:00:00Z"
  }'
```

### Get Job Statistics

```bash
curl http://localhost:8081/api/v1/stats
```

```json
{
  "pending": 12,
  "scheduled": 3,
  "running": 2,
  "completed": 847,
  "failed": 0,
  "cancelled": 5,
  "dead": 1
}
```

## Job Lifecycle

```
                  ┌───────────┐
                  │  pending  │◀──────────────────────┐
                  └─────┬─────┘                       │
                        │                             │ (retry)
                        ▼                             │
                  ┌───────────┐                 ┌─────┴─────┐
  scheduled_at───▶│ scheduled │                 │  failed   │
                  └─────┬─────┘                 └───────────┘
                        │                             ▲
                        ▼                             │ (attempt < max)
                  ┌───────────┐                       │
                  │  running  │───────────────────────┘
                  └─────┬─────┘
                        │
              ┌─────────┼─────────┐
              ▼         ▼         ▼
        ┌──────────┐ ┌──────┐ ┌──────────┐
        │completed │ │ dead │ │cancelled │
        └──────────┘ └──────┘ └──────────┘
                    (max retries
                     exceeded)
```

## Configuration

All configuration is via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgres://taskqueue:taskqueue@localhost:5432/taskqueue?sslmode=disable` | PostgreSQL connection string |
| `REDIS_URL` | `redis://localhost:6379` | Redis connection string |
| `PORT` | `8081` | API server port |
| `ENV` | `development` | Environment (`production` enables JSON logging) |
| `WORKER_COUNT` | `3` | Number of concurrent workers |
| `WORKER_METRICS_PORT` | `9090` | Worker Prometheus metrics port |
| `OTLP_ENDPOINT` | `localhost:4318` | OpenTelemetry collector endpoint |

## Observability

| Service | URL | Purpose |
|---------|-----|---------|
| Grafana | http://localhost:3001 | Dashboards (admin/admin) |
| Prometheus | http://localhost:9091 | Metrics queries |
| Jaeger | http://localhost:16686 | Distributed traces |
| API Metrics | http://localhost:8081/metrics | Raw Prometheus metrics |
| Worker Metrics | http://localhost:9090/metrics | Worker Prometheus metrics |

### Key Metrics

- `taskqueue_jobs_submitted_total` — Jobs submitted by type
- `taskqueue_jobs_processed_total` — Jobs processed by type and status
- `taskqueue_job_duration_seconds` — Execution duration histogram
- `taskqueue_queue_depth` — Current pending queue size
- `taskqueue_processing_count` — Jobs currently being processed
- `taskqueue_active_workers` — Number of active worker goroutines
- `taskqueue_ratelimit_hits_total` — Rate limit rejections by client IP

## Testing

```bash
# Run all tests (requires Docker for Testcontainers)
go test -race ./...

# Run benchmarks
go test -bench=. ./internal/queue/ ./internal/store/

# Run with coverage
go test -race -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

## Project Structure

```
.
├── cmd/
│   ├── server/          # API server entrypoint
│   └── worker/          # Worker pool entrypoint
├── internal/
│   ├── api/             # HTTP handlers, router, middleware, rate limiting
│   ├── config/          # Environment-based configuration
│   ├── domain/          # Core types (Job, errors)
│   ├── lock/            # Redis distributed lock
│   ├── metrics/         # Prometheus metric definitions
│   ├── queue/           # Redis priority queue (Lua scripts)
│   ├── store/           # PostgreSQL persistence layer
│   ├── telemetry/       # OpenTelemetry tracer setup
│   └── worker/          # Worker pool, executor, reaper
├── migrations/          # SQL schema migrations
├── grafana/dashboards/  # Pre-built Grafana dashboard JSON
├── docker-compose.yml   # Full local development stack
├── Dockerfile           # Multi-stage production build
├── prometheus.yml       # Prometheus scrape config
└── .github/workflows/   # CI pipeline
```

## Design Decisions

- **Redis Sorted Sets for priority queue** — O(log N) insert/dequeue with atomic Lua scripts eliminates race conditions between workers competing for the same job.
- **PostgreSQL as source of truth** — Jobs are persisted before being enqueued. If Redis loses data, jobs can be re-enqueued from Postgres.
- **Atomic claim via `UPDATE...RETURNING WHERE status = 'pending'`** — Guarantees exactly-once delivery even if multiple workers dequeue the same job ID.
- **Heartbeat + Reaper pattern** — Workers send heartbeats while processing. A reaper goroutine (protected by distributed lock) detects stuck jobs and requeues them.
- **Exponential backoff stored in DB** — Retry delay is calculated server-side and persisted as `scheduled_at`, making it visible and debuggable.

## License

MIT
