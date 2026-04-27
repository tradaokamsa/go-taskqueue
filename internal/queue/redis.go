package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/tradaokamsa/go-taskqueue/internal/api"
	"github.com/tradaokamsa/go-taskqueue/internal/domain"
)

var _ api.Queue = (*RedisQueue)(nil)

const (
	pendingKey    = "taskqueue:pending"
	processingKey = "taskqueue:processing"
)

type RedisQueue struct {
	client *redis.Client
}

func NewRedisQueue(ctx context.Context, redisURL string) (*RedisQueue, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("queue.NewRedisQueue: invalid URL: %w", err)
	}

	client := redis.NewClient(opts)

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("queue.NewRedisQueue: ping failed: %w", err)
	}

	return &RedisQueue{client: client}, nil
}

func (q *RedisQueue) Client() *redis.Client {
	return q.client
}

func (q *RedisQueue) Close() error {
	return q.client.Close()
}

func (q *RedisQueue) Ping(ctx context.Context) error {
	return q.client.Ping(ctx).Err()
}

// Formula: priority * 1e12 + (maxTimestamp - createdAt)
func calculateScore(priority int, createdAt time.Time) float64 {
	// Max timestamp far in the future (year 2100) to ensure newer jobs have lower scores
	maxTimestamp := time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	timeScore := float64(maxTimestamp - createdAt.UnixNano())
	return float64(priority)*1e12 + timeScore
}

func (q *RedisQueue) Enqueue(ctx context.Context, job *domain.Job) error {
	score := calculateScore(job.Priority, job.CreatedAt)

	err := q.client.ZAdd(ctx, pendingKey, redis.Z{
		Score:  score,
		Member: job.ID,
	}).Err()

	if err != nil {
		return fmt.Errorf("queue.Enqueue: %w", err)
	}
	return nil
}

// Lua script for atomic dequeue
// KEYS[1] = pending queue (ZSET)
// KEYS[2] = processing hash (HASH)
// ARGV[1] = worker ID
// ARGV[2] = current timestamp
var dequeueScript = redis.NewScript(`
	local result = redis.call('ZPOPMAX', KEYS[1], 1)
	if #result == 0 then
			return nil
	end
	local job_id = result[1]
	redis.call('HSET', KEYS[2], job_id, ARGV[1] .. ':' .. ARGV[2])
	return job_id
`)

func (q *RedisQueue) Dequeue(ctx context.Context, workerID string) (*domain.Job, error) {
	timestamp := fmt.Sprintf("%d", time.Now().Unix())

	result, err := dequeueScript.Run(ctx, q.client, []string{pendingKey, processingKey}, workerID, timestamp).Result()

	if err == redis.Nil || result == nil {
		return nil, nil // No job available
	}
	if err != nil {
		return nil, fmt.Errorf("queue.Dequeue: %w", err)
	}

	jobID, ok := result.(string)
	if !ok {
		return nil, fmt.Errorf("queue.Dequeue: unexpected result type")
	}

	return &domain.Job{ID: jobID}, nil
}

// Ack removes the job from the processing hash after successful completion
func (q *RedisQueue) Ack(ctx context.Context, jobID string) error {
	err := q.client.HDel(ctx, processingKey, jobID).Err()
	if err != nil {
		return fmt.Errorf("queue.Ack: %w", err)
	}
	return nil
}

// Nack optionally moves the job back to the pending queue if it fails
func (q *RedisQueue) Nack(ctx context.Context, jobID string, requeue bool, priority int) error {
	if err := q.client.HDel(ctx, processingKey, jobID).Err(); err != nil {
		return fmt.Errorf("queue.Nack: %w", err)
	}

	if requeue {
		score := calculateScore(priority, time.Now())
		if err := q.client.ZAdd(ctx, pendingKey, redis.Z{
			Score:  score,
			Member: jobID,
		}).Err(); err != nil {
			return fmt.Errorf("queue.Nack requeue: %w", err)
		}
	}

	return nil
}

func (q *RedisQueue) Len(ctx context.Context) (int64, error) {
	return q.client.ZCard(ctx, pendingKey).Result()
}

func (q *RedisQueue) ProcessingCount(ctx context.Context) (int64, error) {
	return q.client.HLen(ctx, processingKey).Result()
}

func (q *RedisQueue) Heartbeat(ctx context.Context, jobID string, workerID string) error {
	timestamp := fmt.Sprintf("%d", time.Now().Unix())
	value := workerID + ":" + timestamp

	err := q.client.HSet(ctx, processingKey, jobID, value).Err()
	if err != nil {
		return fmt.Errorf("queue.Heartbeat: %w", err)
	}
	return nil
}

type StuckJob struct {
	JobID    string
	WorkerID string
	LastSeen time.Time
}

func (q *RedisQueue) GetStuckJobs(ctx context.Context, timeout time.Duration) ([]StuckJob, error) {
	result, err := q.client.HGetAll(ctx, processingKey).Result()
	if err != nil {
		return nil, fmt.Errorf("queue.GetStuckJobs: %w", err)
	}

	cutoff := time.Now().Add(-timeout).Unix()
	var stuck []StuckJob

	for jobID, value := range result {
		var workerID string
		var timestamp int64
		_, err := fmt.Sscanf(value, "%s", &workerID)
		if err != nil {
			continue // Skip malformed entries
		}

		for i := len(value) - 1; i >= 0; i-- {
			if value[i] == ':' {
				workerID = value[:i]
				_, _ = fmt.Sscanf(value[i+1:], "%d", &timestamp)
				break
			}
		}

		if timestamp < cutoff {
			stuck = append(stuck, StuckJob{
				JobID:    jobID,
				WorkerID: workerID,
				LastSeen: time.Unix(timestamp, 0),
			})
		}
	}
	return stuck, nil
}

func (q *RedisQueue) RemoveFromProcessing(ctx context.Context, jobID string) error {
	return q.client.HDel(ctx, processingKey, jobID).Err()
}
