// Package ratelimit provides distributed rate limiting middleware for Gin.
// It supports multiple algorithms (token bucket, sliding window) and
// storage backends (memory, Redis) for flexible deployment.
package ratelimit

import (
	"crypto/sha256"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// Algorithm represents the rate limiting algorithm to use.
type Algorithm int

const (
	// TokenBucket uses the token bucket algorithm.
	TokenBucket Algorithm = iota
	// SlidingWindow uses the sliding window counter algorithm.
	SlidingWindow
)

// KeyFunc extracts the rate limit key from the request context.
type KeyFunc func(c *gin.Context) string

// Config holds the rate limiter configuration.
type Config struct {
	// Max is the maximum number of requests allowed within the window.
	Max int32

	// Window is the time duration for the rate limit window.
	Window time.Duration

	// Algorithm specifies which rate limiting algorithm to use.
	Algorithm Algorithm

	// Store is the backend storage for rate limit state.
	Store Store

	// KeyFunc extracts the rate limit key from the request.
	// Defaults to client IP extraction.
	KeyFunc KeyFunc

	// ExceededHandler is called when the rate limit is exceeded.
	ExceededHandler gin.HandlerFunc

	// Skip determines whether to skip rate limiting for a given request.
	Skip func(c *gin.Context) bool

	// TokenSecret is used for generating secure rate limit bypass tokens.
	TokenSecret string
}

// DefaultConfig returns a default rate limiter configuration.
func DefaultConfig() Config {
	return Config{
		Max:       100,
		Window:    time.Minute,
		Algorithm: TokenBucket,
		Store:     nil,
		KeyFunc:   DefaultKeyFunc,
		ExceededHandler: func(c *gin.Context) {
			c.JSON(http.StatusTooManyRequests, gin.H{
				"error": "rate limit exceeded",
			})
			c.Abort()
		},
	}
}

// DefaultKeyFunc extracts the client IP as the rate limit key.
// It checks X-Forwarded-For and X-Real-IP headers before falling
// back to the remote address.
func DefaultKeyFunc(c *gin.Context) string {
	ip := c.GetHeader("X-Forwarded-For")
	if ip != "" {
		parts := strings.Split(ip, ",")
		ip = strings.TrimSpace(parts[0])
	}
	if ip == "" {
		ip = c.GetHeader("X-Real-IP")
	}
	if ip == "" {
		ip, _, _ = net.SplitHostPort(c.Request.RemoteAddr)
	}
	return ip
}

// New creates a rate limiting middleware with the given configuration.
func New(config Config) gin.HandlerFunc {
	if config.KeyFunc == nil {
		config.KeyFunc = DefaultKeyFunc
	}

	instanceID := generateInstanceID(config.TokenSecret)

	return func(c *gin.Context) {
		if config.Skip != nil && config.Skip(c) {
			c.Next()
			return
		}

		key := config.KeyFunc(c)
		if key == "" {
			c.Next()
			return
		}

		hashedKey := hashKey(key, instanceID)

		result, err := config.Store.Take(hashedKey, int64(config.Max), config.Window)
		if err != nil {
			c.Next()
			return
		}

		if !result.Allowed {
			config.ExceededHandler(c)
			return
		}

		c.Next()
	}
}

// generateInstanceID creates a unique identifier for this limiter instance.
func generateInstanceID(secret string) string {
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, 16)
	rand.Read(b)
	return fmt.Sprintf("%x", b)
}

// hashKey produces a consistent hash for the given key and instance.
func hashKey(key, instanceID string) string {
	h := sha256.New()
	h.Write([]byte(instanceID + ":" + key))
	return fmt.Sprintf("%x", h.Sum(nil))
}

// ForGroup creates a rate limiter scoped to a route group with a shared
// key prefix, allowing different limits for different API sections.
func ForGroup(prefix string, config Config) gin.HandlerFunc {
	originalKeyFunc := config.KeyFunc
	config.KeyFunc = func(c *gin.Context) string {
		return prefix + ":" + originalKeyFunc(c)
	}
	return New(config)
}
