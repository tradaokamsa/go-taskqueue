package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/tradaokamsa/go-taskqueue/internal/domain"
	"github.com/tradaokamsa/go-taskqueue/internal/queue"
)

type Queue interface {
	Enqueue(ctx context.Context, job *domain.Job) error
	Dequeue(ctx context.Context, workerID string) (*domain.Job, error)
	Ack(ctx context.Context, jobID string) error
	Nack(ctx context.Context, jobID string, requeue bool, priority int) error
	Heartbeat(ctx context.Context, jobID string, workerID string) error
	GetStuckJobs(ctx context.Context, timeout time.Duration) ([]queue.StuckJob, error)
	RemoveFromProcessing(ctx context.Context, jobID string) error
}

type Store interface {
	GetJob(ctx context.Context, id string) (*domain.Job, error)
	UpdateJob(ctx context.Context, job *domain.Job) error
}

type WorkerPool struct {
	numWorkers     int
	queue          Queue
	store          Store
	executor       Executor
	hostname       string
	stuckTimeout   time.Duration
	reaperInterval time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewWorkerPool(numWorkers int, queue Queue, store Store, executor Executor) *WorkerPool {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "unknown"
	}

	return &WorkerPool{
		numWorkers:     numWorkers,
		queue:          queue,
		store:          store,
		executor:       executor,
		hostname:       hostname,
		stuckTimeout:   60 * time.Second,
		reaperInterval: 30 * time.Second,
	}
}

func (p *WorkerPool) Start(ctx context.Context) {
	p.ctx, p.cancel = context.WithCancel(ctx)

	slog.Info("worker_pool.starting", "workers", p.numWorkers)

	for i := 0; i < p.numWorkers; i++ {
		p.wg.Add(1)
		go p.runWorker(i)
	}

	p.wg.Add(1)
	go p.runReaper()
	slog.Info("worker_pool.started", "workers", p.numWorkers)
}

// runReaper periodically checks for stuck jobs and requeues them
func (p *WorkerPool) runReaper() {
	defer p.wg.Done()

	slog.Info("reaper.started",
		"interval", p.reaperInterval,
		"timeout", p.stuckTimeout,
)
	ticker := time.NewTicker(p.reaperInterval)
	defer ticker.Stop()
	
	for {
		select{
		case <-p.ctx.Done():
				slog.Info("reaper.stopped")
				return
		case <-ticker.C:
			p.reapStuckJobs()
		}
	}
}

// reapStuckJobs finds and requeues stuck jobs
func (p *WorkerPool) reapStuckJobs() {
	stuckJobs, err := p.queue.GetStuckJobs(p.ctx, p.stuckTimeout)
	if err != nil {
		slog.Error("reaper.get_stuck_error", "error", err)
		return
	}

	if len(stuckJobs) == 0{
		return
	}

	slog.Info("reaper.found_stuck_jobs", "count", len(stuckJobs))

	for _, stuck := range stuckJobs {
		p.requeueStuckJob(stuck)
	}
}

// requeueStuckJob resets a stuck job and puts it back in the queue
func (p *WorkerPool) requeueStuckJob(stuck queue.StuckJob) {
	job, err := p.store.GetJob(p.ctx, stuck.JobID)
	if err != nil {
		slog.Error("reaper.get_job_error", 
		"job_id", stuck.JobID, 
		"error", err)
		p.queue.RemoveFromProcessing(p.ctx, stuck.JobID)
		return
	}

	if job.Status != domain.StatusRunning {
		p.queue.RemoveFromProcessing(p.ctx, stuck.JobID)
		return
	}

	slog.Warn("reaper.requeueing_stuck_job",
		"job_id", job.ID,
		"worker_id", stuck.WorkerID,
		"last_seen", stuck.LastSeen,
		"stuck_for", time.Since(stuck.LastSeen),
)

	if job.Attempt >= job.MaxRetries{
		job.Status = domain.StatusDead
		job.Error = "job stuck and max retries exceeded"
		now:= time.Now()
		job.CompletedAt = &now

		if err:= p.store.UpdateJob(p.ctx, job); err != nil {
			slog.Error("reaper.update_dead_error", 
			"job_id", job.ID, 
			"error", err)
		}
		p.queue.RemoveFromProcessing(p.ctx, job.ID)
		return
	}

	job.Status = domain.StatusPending
	job.WorkerID = nil
	job.StartedAt = nil
	job.CompletedAt = nil
	job.Error = "job stuck and requeued by reaper"

	if err := p.store.UpdateJob(p.ctx, job); err != nil {
		slog.Error("reaper.update_pending_error", 
		"job_id", job.ID, 
		"error", err)
		return
	}

	p.queue.RemoveFromProcessing(p.ctx, job.ID)
	if err := p.queue.Enqueue(p.ctx, job); err != nil {
		slog.Error("reaper.enqueue_error", 
		"job_id", job.ID, 
		"error", err)
		return
	}
}

func (p *WorkerPool) runWorker(id int) {
	defer p.wg.Done()

	workerID := fmt.Sprintf("worker-%d-%s", id, p.hostname)
	slog.Info("worker.started", "worker_id", workerID)

	for {
		select {
		case <-p.ctx.Done():
			slog.Info("worker.stopped", "worker_id", workerID)
			return
		default:
			p.processOne(workerID)
		}
	}
}

func (p *WorkerPool) processOne(workerID string) {
	// try to get a job from the queue
	queuedJob, err := p.queue.Dequeue(p.ctx, workerID)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			slog.Error("worker.dequeue_error", "worker_id", workerID, "error", err)
		}
		time.Sleep(100 * time.Millisecond)
		return
	}

	// wait before polling again if no job is available
	if queuedJob == nil {
		time.Sleep(100 * time.Millisecond)
		return
	}

	// fetch full job details from the store
	job, err := p.store.GetJob(p.ctx, queuedJob.ID)
	if err != nil {
		slog.Error("worker.get_job_error",
			"worker_id", workerID,
			"job_id", queuedJob.ID,
			"error", err)
		p.queue.Nack(p.ctx, queuedJob.ID, true, job.Priority)
		return
	}

	// execute the job
	p.executeJob(workerID, job)
}

func (p *WorkerPool) executeJob(workerID string, job *domain.Job) {
	slog.Info("worker.executing_job",
		"worker_id", workerID,
		"job_id", job.ID,
		"type", job.Type,
	)

	now := time.Now()
	job.Status = domain.StatusRunning
	job.WorkerID = &workerID
	job.StartedAt = &now
	job.Attempt++

	if err := p.store.UpdateJob(p.ctx, job); err != nil {
		slog.Error("worker.update_running_error",
			"worker_id", workerID,
			"job_id", job.ID,
			"error", err)
		p.queue.Nack(p.ctx, job.ID, true, job.Priority)
		return
	}

	// creat timeout context
	execCtx, cancel := context.WithTimeout(p.ctx, time.Duration(job.TimeoutSec)*time.Second)
	defer cancel()

	// start heartbeat goroutine
	heartbeatDone := make(chan struct{})
	go p.runHeartbeat(execCtx, job.ID, workerID, heartbeatDone)

	startTime := time.Now()
	result, execErr := p.executor.Execute(execCtx, job)
	duration := time.Since(startTime)

	// stop heartbeat
	close(heartbeatDone)

	completedAt := time.Now()
	job.CompletedAt = &completedAt

	if execErr != nil {
		p.handleJobFailure(workerID, job, execErr, duration)
	} else {
		p.handleJobSuccess(workerID, job, result, duration)
	}
}

func (p *WorkerPool) runHeartbeat(ctx context.Context, jobID string, workerID string, done <-chan struct{}) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.queue.Heartbeat(ctx, jobID, workerID); err != nil {
				slog.Warn("worker.heartbeat_error",
					"worker_id", workerID,
					"job_id", jobID,
					"error", err)
			}
		}
	}
}

func (p *WorkerPool) handleJobSuccess(workerID string, job *domain.Job, result []byte, duration time.Duration) {
	job.Status = domain.StatusCompleted
	job.Result = result
	job.Error = ""

	if err := p.store.UpdateJob(p.ctx, job); err != nil {
		slog.Error("worker.update_completed_error",
			"worker_id", workerID,
			"job_id", job.ID,
			"error", err)
		return
	}

	p.queue.Ack(p.ctx, job.ID)

	slog.Info("worker.job_completed",
		"worker_id", workerID,
		"job_id", job.ID,
		"duration_ms", duration.Milliseconds(),
	)
}

func (p *WorkerPool) handleJobFailure(workerID string, job *domain.Job, execErr error, duration time.Duration) {
	job.Error = execErr.Error()

	if job.Attempt < job.MaxRetries {
		job.Status = domain.StatusPending
		job.WorkerID = nil
		job.StartedAt = nil
		job.CompletedAt = nil

		if err := p.store.UpdateJob(p.ctx, job); err != nil {
			slog.Error("worker.update_retry_error",
				"worker_id", workerID,
				"job_id", job.ID,
				"error", err)
		}

		p.queue.Nack(p.ctx, job.ID, true, job.Priority)

		slog.Warn("worker.job_retry",
			"worker_id", workerID,
			"job_id", job.ID,
			"attempt", job.Attempt,
			"max_retries", job.MaxRetries,
			"error", execErr,
		)
	} else {
		job.Status = domain.StatusDead

		if err := p.store.UpdateJob(p.ctx, job); err != nil {
			slog.Error("worker.update_dead_error",
				"worker_id", workerID,
				"job_id", job.ID,
				"error", err)
		}

		// don't requeue
		p.queue.Nack(p.ctx, job.ID, false, 0)

		slog.Error("worker.job_dead",
			"worker_id", workerID,
			"job_id", job.ID,
			"attempt", job.Attempt,
			"duration_ms", duration.Milliseconds(),
			"error", execErr,
		)
	}
}

func (p *WorkerPool) Shutdown(timeout time.Duration) error {
	slog.Info("worker_pool.shutting_down")
	p.cancel()

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		slog.Info("worker_pool.shutdown_complete")
		return nil
	case <-time.After(timeout):
		slog.Warn("worker_pool.shutdown_timeout", "timeout", timeout)
		return errors.New("shutdown timedout: some workers may still be running")
	}
}
