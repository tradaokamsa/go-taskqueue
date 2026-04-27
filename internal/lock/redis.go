package lock

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type DistributedLock struct {
	client *redis.Client
	key    string
	value  string
	ttl    time.Duration
}

func NewDistributedLock(client *redis.Client, key string, value string, ttl time.Duration) *DistributedLock {
	return &DistributedLock{
		client: client,
		key:    key,
		value:  value,
		ttl:    ttl,
	}
}

func (l *DistributedLock) Acquire(ctx context.Context) (bool, error) {
	result, err := l.client.SetArgs(ctx, l.key, l.value, redis.SetArgs{
		Mode: "NX",
		TTL:  l.ttl,
	}).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return result == "OK", nil
}

var releaseScript = redis.NewScript(`
	if redis.call("get", KEYS[1]) == ARGV[1] then
		return redis.call("del", KEYS[1])
	else
		return 0
	end
`)

func (l *DistributedLock) Release(ctx context.Context) error {
	_, err := releaseScript.Run(ctx, l.client, []string{l.key}, l.value).Result()
	return err
}

var extendScript = redis.NewScript(`
	if redis.call("get", KEYS[1]) == ARGV[1] then
		return redis.call("pexpire", KEYS[1], ARGV[2])
	else
		return 0
	end
`)

func (l *DistributedLock) Extend(ctx context.Context, ttl time.Duration) (bool, error) {
	result, err := extendScript.Run(ctx, l.client, []string{l.key}, l.value, ttl.Milliseconds()).Result()
	if err != nil {
		return false, err
	}
	return result.(int64) == 1, nil //nolint:errcheck // Lua script returns int64
}
