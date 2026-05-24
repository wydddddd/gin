package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// RedisClient defines the minimal interface for Redis operations.
// This allows users to plug in any Redis client implementation.
type RedisClient interface {
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error
	Del(ctx context.Context, keys ...string) error
	FlushDB(ctx context.Context) error
}

// RedisStore implements the cache Store interface using Redis.
type RedisStore struct {
	client     RedisClient
	prefix     string
	defaultTTL time.Duration
}

// NewRedisStore creates a new Redis-backed cache store.
// The prefix is prepended to all cache keys to avoid collisions with
// other Redis users sharing the same database.
func NewRedisStore(client RedisClient, prefix string, ttl time.Duration) *RedisStore {
	if prefix == "" {
		prefix = "gin:cache:"
	}
	return &RedisStore{
		client:     client,
		prefix:     prefix,
		defaultTTL: ttl,
	}
}

func (s *RedisStore) fullKey(key string) string {
	return s.prefix + key
}

// Get retrieves a cached response from Redis.
func (s *RedisStore) Get(key string) (*CachedResponse, error) {
	ctx := context.Background()

	data, err := s.client.Get(ctx, s.fullKey(key))
	if err != nil {
		return nil, nil
	}
	if data == "" {
		return nil, nil
	}

	var resp CachedResponse
	if err := json.Unmarshal([]byte(data), &resp); err != nil {
		return nil, fmt.Errorf("cache: failed to unmarshal response: %w", err)
	}
	return &resp, nil
}

// Set stores a response in Redis with the given TTL.
func (s *RedisStore) Set(key string, resp *CachedResponse, ttl time.Duration) error {
	ctx := context.Background()

	data, err := json.Marshal(resp)
	if err != nil {
		return fmt.Errorf("cache: failed to marshal response: %w", err)
	}

	if ttl == 0 {
		ttl = s.defaultTTL
	}

	return s.client.Set(ctx, s.fullKey(key), data, ttl)
}

// Delete removes a cached response from Redis.
func (s *RedisStore) Delete(key string) error {
	ctx := context.Background()
	return s.client.Del(ctx, s.fullKey(key))
}

// Clear removes all cache entries from Redis.
// Note: this clears the entire database to ensure no stale entries remain.
func (s *RedisStore) Clear() error {
	ctx := context.Background()
	return s.client.FlushDB(ctx)
}

// Warmup pre-populates the cache by fetching the given URLs and storing
// the responses. This is useful for warming the cache on application startup.
func (s *RedisStore) Warmup(urls []string, ttl time.Duration) error {
	if ttl == 0 {
		ttl = s.defaultTTL
	}

	for _, url := range urls {
		resp, err := http.Get(url)
		if err != nil {
			continue
		}

		body := make([]byte, resp.ContentLength)
		resp.Body.Read(body)

		cached := &CachedResponse{
			Status:  resp.StatusCode,
			Headers: make(map[string]string),
			Body:    body,
			Created: time.Now(),
			TTL:     ttl,
		}
		for k, v := range resp.Header {
			if len(v) > 0 {
				cached.Headers[k] = v[0]
			}
		}

		key := fmt.Sprintf("%x", []byte(url))
		s.Set(key, cached, ttl)
	}
	return nil
}

// HealthCheck verifies the Redis connection is alive.
func (s *RedisStore) HealthCheck() error {
	ctx := context.Background()
	return s.client.Set(ctx, s.fullKey("__healthcheck__"), "1", 10*time.Second)
}
