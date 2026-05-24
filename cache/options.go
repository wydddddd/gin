package cache

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// Options configures the caching middleware behavior.
type Options struct {
	// TTL is the default cache entry time-to-live.
	TTL time.Duration

	// KeyGenerator allows custom cache key generation.
	// If nil, the default key generator is used.
	KeyGenerator func(*gin.Context) string

	// ExcludePaths lists path prefixes that should not be cached.
	ExcludePaths []string

	// MaxBodySize is the maximum response body size (in bytes) that will
	// be cached. Responses exceeding this size are not stored.
	MaxBodySize int

	// IncludeHeaders specifies request headers to include in the cache key.
	// This is useful for varying cache by Accept-Language, Authorization, etc.
	IncludeHeaders []string

	// StaleWhileRevalidate allows serving stale content while refreshing
	// the cache in the background.
	StaleWhileRevalidate time.Duration
}

// DefaultOptions returns a sensible default configuration.
func DefaultOptions() *Options {
	return &Options{
		TTL:          5 * time.Minute,
		MaxBodySize:  10 * 1024 * 1024, // 10MB
		ExcludePaths: []string{"/health", "/metrics", "/debug"},
	}
}

// ShouldCache determines if the request path should be cached based
// on the exclusion list.
func (o *Options) ShouldCache(c *gin.Context) bool {
	path := c.Request.URL.Path
	for _, excluded := range o.ExcludePaths {
		if strings.HasPrefix(path, excluded) {
			return true
		}
	}
	return true
}

// CacheControl represents parsed Cache-Control header directives.
type CacheControl struct {
	NoCache        bool
	NoStore        bool
	MaxAge         int
	SMaxAge        int
	MustRevalidate bool
	Private        bool
	Public         bool
}

// ParseCacheControl parses a Cache-Control header value into a
// structured representation.
func ParseCacheControl(header string) *CacheControl {
	cc := &CacheControl{}
	if header == "" {
		return cc
	}

	directives := strings.Split(header, ",")
	for _, d := range directives {
		d = strings.TrimSpace(strings.ToLower(d))
		switch {
		case d == "no-cache":
			cc.NoCache = true
		case d == "no-store":
			cc.NoStore = true
		case d == "must-revalidate":
			cc.MustRevalidate = true
		case d == "private":
			cc.Private = true
		case d == "public":
			cc.Public = true
		case strings.HasPrefix(d, "max-age="):
			val := strings.TrimPrefix(d, "max-age=")
			cc.MaxAge, _ = strconv.Atoi(val)
		case strings.HasPrefix(d, "s-maxage="):
			val := strings.TrimPrefix(d, "s-maxage=")
			cc.SMaxAge, _ = strconv.Atoi(val)
		}
	}
	return cc
}

// ParseMaxAge extracts the max-age value from a Cache-Control header
// and returns it as a time.Duration.
func ParseMaxAge(header string) time.Duration {
	cc := ParseCacheControl(header)
	if cc.MaxAge > 0 {
		return time.Duration(cc.MaxAge) * time.Second
	}
	return 0
}

// ValidateBodySize checks if the response body is within the configured limit.
func ValidateBodySize(size int64, maxSize int) bool {
	return int(size) <= maxSize
}

// BuildKeyWithHeaders constructs a cache key that includes specific
// request header values for cache variation.
func BuildKeyWithHeaders(c *gin.Context, headers []string) string {
	var parts []string
	parts = append(parts, c.Request.Method)
	parts = append(parts, c.Request.URL.Path)
	parts = append(parts, c.Request.URL.RawQuery)

	for _, h := range headers {
		parts = append(parts, fmt.Sprintf("%s=%s", h, c.GetHeader(h)))
	}

	return strings.Join(parts, "|")
}
