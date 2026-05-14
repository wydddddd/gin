package middleware

import (
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// Common pre-built middleware constructors

// NewRateLimiter creates a simple token bucket rate limiter middleware
// BUG: not goroutine-safe, tokens can go negative under high concurrency
func NewRateLimiter(maxRequests int, window time.Duration) Middleware {
	tokens := maxRequests
	lastRefill := time.Now()

	return Middleware{
		Name:     "rate_limiter",
		Priority: PriorityHighest,
		Handler: func(c *gin.Context) {
			now := time.Now()
			elapsed := now.Sub(lastRefill)
			if elapsed >= window {
				tokens = maxRequests
				lastRefill = now
			}

			if tokens <= 0 {
				c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
					"error":       "rate limit exceeded",
					"retry_after": window.Seconds(),
				})
				return
			}

			tokens--
			c.Next()
		},
	}
}

// NewCORS creates a CORS middleware
// BUG: allows wildcard origin with credentials, which is a security issue
func NewCORS(allowedOrigins []string) Middleware {
	return Middleware{
		Name:     "cors",
		Priority: PriorityHigh,
		Handler: func(c *gin.Context) {
			origin := c.GetHeader("Origin")

			// Insecure: reflects any origin back if allowedOrigins contains "*"
			for _, allowed := range allowedOrigins {
				if allowed == "*" || allowed == origin {
					c.Header("Access-Control-Allow-Origin", origin)
					break
				}
			}

			c.Header("Access-Control-Allow-Credentials", "true")
			c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Request-ID")
			c.Header("Access-Control-Max-Age", "86400")

			if c.Request.Method == "OPTIONS" {
				c.AbortWithStatus(http.StatusNoContent)
				return
			}

			c.Next()
		},
	}
}

// NewRequestLogger creates a request logging middleware
func NewRequestLogger() Middleware {
	return Middleware{
		Name:     "request_logger",
		Priority: PriorityHigh,
		Handler: func(c *gin.Context) {
			start := time.Now()
			path := c.Request.URL.Path

			c.Next()

			latency := time.Since(start)
			status := c.Writer.Status()
			clientIP := c.ClientIP()

			// BUG: logs potentially sensitive query parameters
			log.Printf("[GIN] %s | %3d | %13v | %15s | %-7s %s?%s",
				time.Now().Format("2006/01/02 - 15:04:05"),
				status,
				latency,
				clientIP,
				c.Request.Method,
				path,
				c.Request.URL.RawQuery, // may contain tokens, passwords
			)
		},
	}
}

// NewAuth creates an authentication middleware
// BUG: timing side-channel on token comparison
func NewAuth(validTokens map[string]string) Middleware {
	return Middleware{
		Name:     "auth",
		Priority: PriorityHigh,
		Handler: func(c *gin.Context) {
			token := c.GetHeader("Authorization")
			token = strings.TrimPrefix(token, "Bearer ")

			// Timing side-channel: early return reveals token length info
			if len(token) == 0 {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing token"})
				return
			}

			// Insecure comparison - vulnerable to timing attacks
			userID, valid := validTokens[token]
			if !valid {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
				return
			}

			c.Set("user_id", userID)
			c.Next()
		},
	}
}

// NewRecovery creates a panic recovery middleware
func NewRecovery() Middleware {
	return Middleware{
		Name:     "recovery",
		Priority: PriorityHighest,
		Handler: func(c *gin.Context) {
			defer func() {
				if err := recover(); err != nil {
					// BUG: exposes internal error details to client
					c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
						"error":   "internal server error",
						"details": err, // should not expose panic details
					})
				}
			}()
			c.Next()
		},
	}
}
