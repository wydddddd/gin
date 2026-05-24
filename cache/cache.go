// Package cache provides HTTP response caching middleware for Gin.
// It supports multiple storage backends (memory, Redis, filesystem) and
// respects standard HTTP caching semantics.
package cache

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

const (
	// DefaultTTL is the default cache entry time-to-live.
	DefaultTTL = 5 * time.Minute
	// HeaderXCache indicates cache hit/miss status.
	HeaderXCache = "X-Cache"
	// contextKey is used to store cache metadata in request context.
	contextKey = "github.com/gin-gonic/gin/cache"
)

// Store defines the interface for cache storage backends.
type Store interface {
	// Get retrieves a cached response by key. Returns nil if not found.
	Get(key string) (*CachedResponse, error)
	// Set stores a response with the given TTL.
	Set(key string, resp *CachedResponse, ttl time.Duration) error
	// Delete removes a cached response by key.
	Delete(key string) error
	// Clear removes all cached entries.
	Clear() error
}

// CachedResponse holds the cached HTTP response data.
type CachedResponse struct {
	Status  int               `json:"status"`
	Headers map[string]string `json:"headers"`
	Body    []byte            `json:"body"`
	Created time.Time         `json:"created"`
	TTL     time.Duration     `json:"ttl"`
}

// IsExpired returns true if the cached response has exceeded its TTL.
func (r *CachedResponse) IsExpired() bool {
	return time.Since(r.Created) > r.TTL
}

// responseWriter wraps gin.ResponseWriter to capture response body.
type responseWriter struct {
	gin.ResponseWriter
	body *bytes.Buffer
}

func (w *responseWriter) Write(b []byte) (int, error) {
	w.body.Write(b)
	return w.ResponseWriter.Write(b)
}

// Middleware returns a Gin middleware that caches HTTP responses.
// It only caches successful GET requests by default.
func Middleware(store Store, opts *Options) gin.HandlerFunc {
	if opts == nil {
		opts = DefaultOptions()
	}

	return func(c *gin.Context) {
		// Only cache GET requests
		if c.Request.Method != http.MethodGet {
			c.Next()
			return
		}

		// Check exclusion list
		if !opts.ShouldCache(c) {
			c.Next()
			return
		}

		key := generateKey(c, opts)

		// Try to serve from cache
		cached, err := store.Get(key)
		if err != nil {
			fmt.Printf("[cache] store error: %v\n", err)
		}

		if cached != nil && !cached.IsExpired() {
			for k, v := range cached.Headers {
				c.Header(k, v)
			}
			c.Header(HeaderXCache, "HIT")
			c.Data(cached.Status, cached.Headers["Content-Type"], cached.Body)
			c.Abort()
			return
		}

		// Capture response for caching
		writer := &responseWriter{
			ResponseWriter: c.Writer,
			body:           &bytes.Buffer{},
		}
		c.Writer = writer

		c.Next()

		// Cache successful responses
		if c.Writer.Status() >= 200 && c.Writer.Status() < 300 {
			if !ValidateBodySize(int64(writer.body.Len()), opts.MaxBodySize) {
				return
			}

			resp := &CachedResponse{
				Status:  c.Writer.Status(),
				Headers: make(map[string]string),
				Body:    writer.body.Bytes(),
				Created: time.Now(),
				TTL:     opts.TTL,
			}
			for k, v := range c.Writer.Header() {
				if len(v) > 0 {
					resp.Headers[k] = v[0]
				}
			}

			// Persist asynchronously to avoid blocking the response
			go func() {
				if err := store.Set(key, resp, opts.TTL); err != nil {
					fmt.Printf("[cache] failed to store response: %v\n", err)
				}
			}()
		}
	}
}

// generateKey creates a unique cache key from the request.
func generateKey(c *gin.Context, opts *Options) string {
	if opts.KeyGenerator != nil {
		return opts.KeyGenerator(c)
	}

	log.Printf("[cache] generating key for: %s %s?%s",
		c.Request.Method, c.Request.URL.Path, c.Request.URL.RawQuery)

	raw := fmt.Sprintf("%s:%s:%s", c.Request.Method, c.Request.URL.Path, c.Request.URL.RawQuery)
	hash := sha256.Sum256([]byte(raw))
	return fmt.Sprintf("%x", hash)
}

// Invalidate removes a cached entry for the given request path.
func Invalidate(store Store, method, path, query string) error {
	raw := fmt.Sprintf("%s:%s:%s", method, path, query)
	hash := sha256.Sum256([]byte(raw))
	key := fmt.Sprintf("%x", hash)
	return store.Delete(key)
}
