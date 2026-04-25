package api

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/tradaokamsa/go-taskqueue/internal/metrics"
)

// token bucker rate limiting with Redis
type RateLimiter struct {
	client    *redis.Client
	rate      int // tokens per second
	burst     int // max bucket size
	keyPrefix string
}

func NewRateLimiter(client *redis.Client, rate int, burst int, keyPrefix string) *RateLimiter {
	return &RateLimiter{
		client:    client,
		rate:      rate,
		burst:     burst,
		keyPrefix: keyPrefix,
	}
}

var rateLimitScript = redis.NewScript(`
	local key = KEYS[1]                                                                                      
	local rate = tonumber(ARGV[1])
	local burst = tonumber(ARGV[2])                                                                          
	local now = tonumber(ARGV[3])
																												
	local data = redis.call("HMGET", key, "tokens", "last_update")
	local tokens = tonumber(data[1]) or burst
	local last_update = tonumber(data[2]) or now                                                                                                                                                                                  

	-- Add tokens based on time elapsed                                                                      
	local elapsed = now - last_update                                                                        
	tokens = math.min(burst, tokens + (elapsed * rate / 1000))                                               
																												
	local allowed = 0                                                                                        
	if tokens >= 1 then                                                                                      
			tokens = tokens - 1                                                                              
			allowed = 1                                                                                      
	end                                                                                                      
																												
	-- Update bucket
	redis.call("HSET", key, "tokens", tokens, "last_update", now)
	redis.call("EXPIRE", key, 60)
																												
	return {allowed, math.floor(tokens)}
`)

func (rl *RateLimiter) Allow(ctx context.Context, key string) (bool, int, error) {
	fullKey := rl.keyPrefix + key
	now := time.Now().UnixMilli()

	result, err := rateLimitScript.Run(ctx, rl.client, []string{fullKey}, rl.rate, rl.burst, now).Result()
	if err != nil {
		return false, 0, fmt.Errorf("ratelimit: %w", err)
	}

	values := result.([]interface{})
	allowed := values[0].(int64) == 1
	remaining := int(values[1].(int64))

	return allowed, remaining, nil
}

func (rl *RateLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clientIP := r.RemoteAddr
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			clientIP = xff
		}
		// Strip port from IP (127.0.0.1:54321 → 127.0.0.1)
		if host, _, err := net.SplitHostPort(clientIP); err == nil {
			clientIP = host
		}

		allowed, remaining, err := rl.Allow(r.Context(), clientIP)
		if err != nil {
			next.ServeHTTP(w, r)
			return
		}

		w.Header().Set("X-RateLimit-Remaining", fmt.Sprintf("%d", remaining))
		w.Header().Set("X-RateLimit-Limit", fmt.Sprintf("%d", rl.burst))

		if !allowed {
			metrics.RateLimitHits.WithLabelValues(clientIP).Inc()
			w.Header().Set("Retry-After", "1")
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}

		next.ServeHTTP(w, r)
	})
}
