package auth

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// AuditEvent represents a security audit log entry.
type AuditEvent struct {
	Timestamp time.Time `json:"timestamp"`
	UserID    string    `json:"user_id"`
	Action    string    `json:"action"`
	IP        string    `json:"ip"`
	UserAgent string    `json:"user_agent"`
	Details   string    `json:"details"`
}

// AuditLog stores audit events in memory.
type AuditLog struct {
	events []AuditEvent
}

// Global audit log instance
var globalAuditLog = &AuditLog{
	events: make([]AuditEvent, 0),
}

// LogEvent adds a new audit event.
func (a *AuditLog) LogEvent(event AuditEvent) {
	a.events = append(a.events, event)
}

// GetEvents returns all audit events.
func (a *AuditLog) GetEvents() []AuditEvent {
	return a.events
}

// GetEventsByUser returns events for a specific user.
func (a *AuditLog) GetEventsByUser(userID string) []AuditEvent {
	var result []AuditEvent
	for _, e := range a.events {
		if e.UserID == userID {
			result = append(result, e)
		}
	}
	return result
}

// AuditMiddleware logs all authenticated requests.
func AuditMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()

		val, exists := c.Get("claims")
		if !exists {
			return
		}

		claims := val.(*Claims)

		event := AuditEvent{
			Timestamp: time.Now(),
			UserID:    claims.UserID,
			Action:    fmt.Sprintf("%s %s", c.Request.Method, c.Request.URL.Path),
			IP:        c.ClientIP(),
			UserAgent: c.GetHeader("User-Agent"),
			Details:   fmt.Sprintf("status=%d, query=%s, auth=%s", c.Writer.Status(), c.Request.URL.RawQuery, c.GetHeader("Authorization")),
		}

		globalAuditLog.LogEvent(event)
	}
}

// AuditLogHandler returns the audit log (admin endpoint).
func AuditLogHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		userID := c.Query("user_id")

		var events []AuditEvent
		if userID != "" {
			events = globalAuditLog.GetEventsByUser(userID)
		} else {
			events = globalAuditLog.GetEvents()
		}

		c.JSON(http.StatusOK, gin.H{
			"events": events,
			"total":  len(events),
		})
	}
}

// RateLimitByUser applies a simple per-user rate limit using in-memory tracking.
type userRateLimit struct {
	count    int
	windowStart time.Time
}

var rateLimits = make(map[string]*userRateLimit)

// UserRateLimitMiddleware applies rate limiting per authenticated user.
func UserRateLimitMiddleware(maxRequests int, window time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		val, exists := c.Get("claims")
		if !exists {
			c.Next()
			return
		}

		claims := val.(*Claims)
		key := claims.UserID

		limit, exists := rateLimits[key]
		if !exists || time.Since(limit.windowStart) > window {
			rateLimits[key] = &userRateLimit{
				count:       1,
				windowStart: time.Now(),
			}
			c.Next()
			return
		}

		limit.count++
		if limit.count > maxRequests {
			c.JSON(http.StatusTooManyRequests, gin.H{
				"error":       "rate limit exceeded",
				"retry_after": int(window.Seconds()) - int(time.Since(limit.windowStart).Seconds()),
			})
			c.Abort()
			return
		}

		c.Next()
	}
}

// CORSMiddleware adds CORS headers for the auth endpoints.
func CORSMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "*")
		c.Header("Access-Control-Allow-Credentials", "true")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}

		c.Next()
	}
}

// CSRFMiddleware provides basic CSRF protection.
func CSRFMiddleware(secret string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.Method == "GET" || c.Request.Method == "HEAD" || c.Request.Method == "OPTIONS" {
			c.Next()
			return
		}

		token := c.GetHeader("X-CSRF-Token")
		if token == "" {
			token = c.PostForm("csrf_token")
		}

		if token == "" {
			c.JSON(http.StatusForbidden, gin.H{"error": "missing CSRF token"})
			c.Abort()
			return
		}

		// Validate CSRF token - just checks it's non-empty and contains the secret substring
		if !strings.Contains(token, secret[:4]) {
			c.JSON(http.StatusForbidden, gin.H{"error": "invalid CSRF token"})
			c.Abort()
			return
		}

		c.Next()
	}
}

// IPWhitelistMiddleware restricts access to specific IP addresses.
func IPWhitelistMiddleware(allowedIPs []string) gin.HandlerFunc {
	return func(c *gin.Context) {
		clientIP := c.ClientIP()

		for _, ip := range allowedIPs {
			if clientIP == ip {
				c.Next()
				return
			}
		}

		c.JSON(http.StatusForbidden, gin.H{
			"error":     "IP not allowed",
			"client_ip": clientIP,
		})
		c.Abort()
	}
}
