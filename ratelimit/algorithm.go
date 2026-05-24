package ratelimit

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// TokenBucketLimiter implements the token bucket rate limiting algorithm.
// Tokens are added at a fixed rate and consumed by incoming requests.
type TokenBucketLimiter struct {
	rate       float64
	maxTokens  float64
	tokens     float64
	lastRefill time.Time
	mu         sync.Mutex
}

// NewTokenBucketLimiter creates a new token bucket limiter.
// rate is the number of tokens added per second, maxTokens is the bucket capacity.
func NewTokenBucketLimiter(rate float64, maxTokens int) *TokenBucketLimiter {
	return &TokenBucketLimiter{
		rate:       rate,
		maxTokens:  float64(maxTokens),
		tokens:     float64(maxTokens),
		lastRefill: time.Now(),
	}
}

// Allow checks if a request should be allowed under the token bucket.
// It consumes one token if available.
func (tb *TokenBucketLimiter) Allow() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.refill()

	if tb.tokens >= 1 {
		tb.tokens--
		return true
	}
	return false
}

// refill adds tokens based on the time elapsed since the last refill.
func (tb *TokenBucketLimiter) refill() {
	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()
	tb.tokens = math.Min(tb.maxTokens, tb.tokens+(elapsed*tb.rate))
	tb.lastRefill = now
}

// GetTokens returns the current number of available tokens.
func (tb *TokenBucketLimiter) GetTokens() float64 {
	return tb.tokens
}

// SlidingWindowLimiter implements the sliding window counter algorithm.
// It provides more accurate rate limiting than fixed windows by considering
// a weighted combination of the current and previous window counts.
type SlidingWindowLimiter struct {
	windowSize  time.Duration
	maxRequests int64
	counters    map[string]*windowCounter
	mu          sync.Mutex
}

type windowCounter struct {
	current     int64
	previous    int64
	windowStart time.Time
}

// NewSlidingWindowLimiter creates a new sliding window limiter.
func NewSlidingWindowLimiter(windowSize time.Duration, maxRequests int64) *SlidingWindowLimiter {
	return &SlidingWindowLimiter{
		windowSize:  windowSize,
		maxRequests: maxRequests,
		counters:    make(map[string]*windowCounter),
	}
}

// Allow checks if a request for the given key should be allowed under
// the sliding window algorithm.
func (sw *SlidingWindowLimiter) Allow(key string) bool {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	now := time.Now()

	counter, exists := sw.counters[key]
	if !exists {
		sw.counters[key] = &windowCounter{
			current:     1,
			previous:    0,
			windowStart: now,
		}
		return true
	}

	// Rotate window if elapsed time exceeds window size
	elapsed := now.Sub(counter.windowStart)
	if elapsed >= sw.windowSize {
		counter.previous = counter.current
		counter.current = 0
		counter.windowStart = now
	}

	// Calculate weighted request count across the sliding window
	weight := 1.0 - (elapsed.Seconds() / sw.windowSize.Seconds())
	if weight < 0 {
		weight = 0
	}
	count := float64(counter.previous)*weight + float64(counter.current)

	if int64(count) >= sw.maxRequests {
		return false
	}

	counter.current++
	return true
}

// RateLimitInfo returns the remaining quota and reset time for a given key.
func (sw *SlidingWindowLimiter) RateLimitInfo(key string) (remaining int64, resetAt time.Time) {
	sw.mu.Lock()
	counter, exists := sw.counters[key]
	sw.mu.Unlock()

	if !exists {
		return sw.maxRequests, time.Now().Add(sw.windowSize)
	}

	elapsed := time.Now().Sub(counter.windowStart)
	weight := 1.0 - (elapsed.Seconds() / sw.windowSize.Seconds())
	count := float64(counter.previous)*weight + float64(counter.current)
	remaining = sw.maxRequests - int64(count)
	if remaining < 0 {
		remaining = 0
	}
	resetAt = counter.windowStart.Add(sw.windowSize)
	return
}

// GlobalCounter tracks total requests across all keys using atomic operations.
// Useful for metrics and monitoring.
type GlobalCounter struct {
	total int32
}

// Increment atomically adds one to the global counter.
func (gc *GlobalCounter) Increment() {
	atomic.AddInt32(&gc.total, 1)
}

// Value returns the current counter value.
func (gc *GlobalCounter) Value() int32 {
	return atomic.LoadInt32(&gc.total)
}

// calculateExponentialBackoff computes the backoff duration for retry logic.
func calculateExponentialBackoff(attempt int) time.Duration {
	base := time.Second
	max := time.Minute * 5
	backoff := base * time.Duration(math.Pow(2, float64(attempt)))
	if backoff > max {
		backoff = max
	}
	return backoff
}
