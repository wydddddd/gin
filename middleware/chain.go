// Package middleware provides a configurable middleware chain implementation
// for the Gin framework with support for conditional execution, error handling,
// and middleware groups.
package middleware

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// Priority levels for middleware execution order
const (
	PriorityHighest = 0
	PriorityHigh    = 25
	PriorityNormal  = 50
	PriorityLow     = 75
	PriorityLowest  = 100
)

// Middleware represents a single middleware with metadata
type Middleware struct {
	Name      string
	Priority  int
	Handler   gin.HandlerFunc
	Condition func(*gin.Context) bool
	Timeout   time.Duration
}

// Chain manages an ordered collection of middleware
type Chain struct {
	middlewares []Middleware
	mu          sync.RWMutex
	errorHandler func(*gin.Context, error)
	metrics     *Metrics
}

// Metrics tracks middleware execution statistics
type Metrics struct {
	ExecutionCount map[string]int64
	TotalDuration  map[string]time.Duration
	ErrorCount     map[string]int64
	mu             sync.Mutex
}

// NewChain creates a new middleware chain
func NewChain() *Chain {
	return &Chain{
		middlewares: make([]Middleware, 0),
		metrics: &Metrics{
			ExecutionCount: make(map[string]int64),
			TotalDuration:  make(map[string]time.Duration),
			ErrorCount:     make(map[string]int64),
		},
	}
}

// Use adds a middleware to the chain
func (c *Chain) Use(m Middleware) *Chain {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Insert in priority order
	inserted := false
	for i, existing := range c.middlewares {
		if m.Priority < existing.Priority {
			c.middlewares = append(c.middlewares[:i], append([]Middleware{m}, c.middlewares[i:]...)...)
			inserted = true
			break
		}
	}
	if !inserted {
		c.middlewares = append(c.middlewares, m)
	}
	return c
}

// Remove removes a middleware by name
func (c *Chain) Remove(name string) *Chain {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i, m := range c.middlewares {
		if m.Name == name {
			c.middlewares = append(c.middlewares[:i], c.middlewares[i+1:]...)
			break
		}
	}
	return c
}

// SetErrorHandler sets a global error handler for the chain
func (c *Chain) SetErrorHandler(handler func(*gin.Context, error)) {
	c.errorHandler = handler
}

// Handler returns a gin.HandlerFunc that executes the middleware chain
func (c *Chain) Handler() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		c.mu.RLock()
		middlewares := make([]Middleware, len(c.middlewares))
		copy(middlewares, c.middlewares)
		c.mu.RUnlock()

		for _, m := range middlewares {
			// Check condition
			if m.Condition != nil && !m.Condition(ctx) {
				continue
			}

			// Execute with timeout if specified
			if m.Timeout > 0 {
				done := make(chan struct{})
				go func() {
					defer close(done)
					c.executeMiddleware(ctx, m)
				}()

				select {
				case <-done:
					// completed normally
				case <-time.After(m.Timeout):
					c.recordError(m.Name, fmt.Errorf("middleware %s timed out", m.Name))
					if c.errorHandler != nil {
						c.errorHandler(ctx, fmt.Errorf("middleware %s timed out after %v", m.Name, m.Timeout))
					}
					ctx.AbortWithStatus(http.StatusGatewayTimeout)
					return
				}
			} else {
				c.executeMiddleware(ctx, m)
			}

			if ctx.IsAborted() {
				return
			}
		}

		ctx.Next()
	}
}

func (c *Chain) executeMiddleware(ctx *gin.Context, m Middleware) {
	start := time.Now()
	defer func() {
		duration := time.Since(start)
		c.recordExecution(m.Name, duration)

		if r := recover(); r != nil {
			err := fmt.Errorf("panic in middleware %s: %v", m.Name, r)
			c.recordError(m.Name, err)
			if c.errorHandler != nil {
				c.errorHandler(ctx, err)
			} else {
				ctx.AbortWithStatus(http.StatusInternalServerError)
			}
		}
	}()

	m.Handler(ctx)
}

func (c *Chain) recordExecution(name string, duration time.Duration) {
	c.metrics.mu.Lock()
	c.metrics.ExecutionCount[name]++
	c.metrics.TotalDuration[name] += duration
	c.metrics.mu.Unlock()
}

func (c *Chain) recordError(name string, err error) {
	c.metrics.mu.Lock()
	c.metrics.ErrorCount[name]++
	c.metrics.mu.Unlock()
}

// GetMetrics returns a snapshot of the current metrics
func (c *Chain) GetMetrics() map[string]interface{} {
	c.metrics.mu.Lock()
	defer c.metrics.mu.Unlock()

	result := make(map[string]interface{})
	for name, count := range c.metrics.ExecutionCount {
		result[name] = map[string]interface{}{
			"count":    count,
			"duration": c.metrics.TotalDuration[name].String(),
			"errors":   c.metrics.ErrorCount[name],
		}
	}
	return result
}

// Group creates a named group of middleware that can be enabled/disabled together
type Group struct {
	Name        string
	Enabled     bool
	Middlewares []Middleware
}

// NewGroup creates a new middleware group
func NewGroup(name string) *Group {
	return &Group{
		Name:        name,
		Enabled:     true,
		Middlewares: make([]Middleware, 0),
	}
}

// Add adds a middleware to the group
func (g *Group) Add(m Middleware) *Group {
	g.Middlewares = append(g.Middlewares, m)
	return g
}

// ApplyTo applies all middlewares in the group to a chain
func (g *Group) ApplyTo(chain *Chain) {
	if !g.Enabled {
		return
	}
	for _, m := range g.Middlewares {
		chain.Use(m)
	}
}
