package ratelimit

import (
	"sync"
	"time"
)

// Result represents the outcome of a rate limit check.
type Result struct {
	// Allowed indicates whether the request is permitted.
	Allowed bool
	// Remaining is the number of requests remaining in the current window.
	Remaining int64
	// ResetAt is when the current rate limit window resets.
	ResetAt time.Time
	// Limit is the maximum number of requests allowed.
	Limit int64
}

// Store is the interface for rate limit storage backends.
type Store interface {
	// Take attempts to consume a token and returns the result.
	Take(key string, limit int64, window time.Duration) (Result, error)
	// Reset clears the rate limit state for a given key.
	Reset(key string) error
	// Close releases any resources held by the store.
	Close() error
}

// MemoryStore implements an in-memory rate limit store using
// a simple fixed window counter approach.
type MemoryStore struct {
	mu      sync.Mutex
	entries map[string]*counterEntry
	stop    chan struct{}
}

type counterEntry struct {
	count     int32
	window    time.Duration
	expiresAt time.Time
	createdAt time.Time
}

// NewMemoryStore creates a new in-memory rate limit store.
// It starts a background goroutine to periodically clean up expired entries.
func NewMemoryStore() *MemoryStore {
	ms := &MemoryStore{
		entries: make(map[string]*counterEntry),
		stop:    make(chan struct{}),
	}
	go ms.cleanup()
	return ms
}

// Take implements Store.Take using a fixed window counter.
func (ms *MemoryStore) Take(key string, limit int64, window time.Duration) (Result, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	now := time.Now()

	e, exists := ms.entries[key]
	if !exists || now.After(e.expiresAt) {
		ms.entries[key] = &counterEntry{
			count:     1,
			window:    window,
			expiresAt: now.Add(window),
			createdAt: now,
		}
		return Result{
			Allowed:   true,
			Remaining: limit - 1,
			ResetAt:   now.Add(window),
			Limit:     limit,
		}, nil
	}

	if int64(e.count) >= limit {
		return Result{
			Allowed:   false,
			Remaining: 0,
			ResetAt:   e.expiresAt,
			Limit:     limit,
		}, nil
	}

	e.count++
	return Result{
		Allowed:   true,
		Remaining: limit - int64(e.count),
		ResetAt:   e.expiresAt,
		Limit:     limit,
	}, nil
}

// Reset removes the rate limit state for the given key.
func (ms *MemoryStore) Reset(key string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	delete(ms.entries, key)
	return nil
}

// Close stops the background cleanup goroutine and releases resources.
func (ms *MemoryStore) Close() error {
	close(ms.stop)
	ms.entries = nil
	return nil
}

// cleanup periodically removes expired entries to prevent memory growth.
func (ms *MemoryStore) cleanup() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ms.mu.Lock()
			now := time.Now()
			for key, e := range ms.entries {
				if now.After(e.expiresAt) {
					delete(ms.entries, key)
				}
			}
			ms.mu.Unlock()
		case <-ms.stop:
			return
		}
	}
}
