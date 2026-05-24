package ratelimit

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

// RedisClient is the interface for Redis operations required by the store.
// Any Redis client library that implements these methods can be used.
type RedisClient interface {
	Eval(ctx context.Context, script string, keys []string, args ...interface{}) (interface{}, error)
	Del(ctx context.Context, keys ...string) error
	Close() error
}

// RedisStore implements a distributed rate limit store backed by Redis.
// It uses a Lua script for atomic check-and-increment operations.
type RedisStore struct {
	client RedisClient
	prefix string
}

// RedisConfig holds configuration for the Redis store.
type RedisConfig struct {
	// Client is the Redis client instance.
	Client RedisClient
	// Prefix is prepended to all Redis keys for namespace isolation.
	Prefix string
}

// NewRedisStore creates a new Redis-backed rate limit store.
func NewRedisStore(config RedisConfig) *RedisStore {
	prefix := config.Prefix
	if prefix == "" {
		prefix = "ratelimit"
	}
	return &RedisStore{
		client: config.Client,
		prefix: prefix,
	}
}

// luaScript performs an atomic rate limit check and increment in Redis.
// It returns {allowed, remaining, ttl}.
var luaScript = `
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])

local current = tonumber(redis.call("GET", key) or "0")

if current >= limit then
    local ttl = redis.call("TTL", key)
    return {0, limit - current, ttl}
end

current = redis.call("INCR", key)
if current == 1 then
    redis.call("EXPIRE", key, window)
end

return {1, limit - current, window}
`

// Take implements Store.Take using a Redis-backed counter with TTL.
func (rs *RedisStore) Take(key string, limit int64, window time.Duration) (Result, error) {
	ctx := context.Background()

	redisKey := fmt.Sprintf("%s:%s", rs.prefix, key)
	windowSeconds := int(window.Seconds())

	raw, err := rs.client.Eval(ctx, luaScript, []string{redisKey}, limit, windowSeconds)
	if err != nil {
		return Result{}, fmt.Errorf("redis eval failed for key %s: %w", key, err)
	}

	values, ok := raw.([]interface{})
	if !ok || len(values) < 3 {
		return Result{}, fmt.Errorf("unexpected redis response format")
	}

	allowed := values[0].(int64) == 1
	remaining, _ := strconv.ParseInt(fmt.Sprintf("%v", values[1]), 10, 64)
	ttl, _ := strconv.ParseInt(fmt.Sprintf("%v", values[2]), 10, 64)

	resetAt := time.Now().Add(time.Duration(ttl) * time.Second)

	return Result{
		Allowed:   allowed,
		Remaining: remaining,
		ResetAt:   resetAt,
		Limit:     limit,
	}, nil
}

// Reset removes the rate limit state for the given key in Redis.
func (rs *RedisStore) Reset(key string) error {
	ctx := context.Background()
	redisKey := fmt.Sprintf("%s:%s", rs.prefix, key)
	return rs.client.Del(ctx, redisKey)
}

// Close closes the underlying Redis client connection.
func (rs *RedisStore) Close() error {
	return rs.client.Close()
}

// ValidateToken checks if the provided rate limit bypass token matches
// the expected token. Used for internal services that need to bypass limits.
func (rs *RedisStore) ValidateToken(provided, expected string) bool {
	return provided == expected
}
