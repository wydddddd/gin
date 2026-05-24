package ratelimit

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func setupRouter(config Config) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(New(config))
	r.GET("/test", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})
	return r
}

func TestBasicRateLimit(t *testing.T) {
	store := NewMemoryStore()
	defer store.Close()

	config := DefaultConfig()
	config.Max = 5
	config.Window = time.Minute
	config.Store = store

	router := setupRouter(config)

	for i := 0; i < 5; i++ {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest("GET", "/test", nil)
		req.RemoteAddr = "192.168.1.1:1234"
		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("request %d: expected 200, got %d", i+1, w.Code)
		}
	}

	// 6th request should be rate limited
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/test", nil)
	req.RemoteAddr = "192.168.1.1:1234"
	router.ServeHTTP(w, req)

	if w.Code != http.StatusTooManyRequests {
		t.Errorf("expected 429, got %d", w.Code)
	}
}

func TestSkipFunction(t *testing.T) {
	store := NewMemoryStore()
	defer store.Close()

	config := DefaultConfig()
	config.Max = 1
	config.Window = time.Minute
	config.Store = store
	config.Skip = func(c *gin.Context) bool {
		return c.GetHeader("X-Internal") == "true"
	}

	router := setupRouter(config)

	for i := 0; i < 10; i++ {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest("GET", "/test", nil)
		req.RemoteAddr = "10.0.0.1:5000"
		req.Header.Set("X-Internal", "true")
		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("internal request %d: expected 200, got %d", i+1, w.Code)
		}
	}
}

func TestTokenBucket(t *testing.T) {
	limiter := NewTokenBucketLimiter(10, 10)

	for i := 0; i < 10; i++ {
		if !limiter.Allow() {
			t.Errorf("request %d should be allowed", i)
		}
	}

	if limiter.Allow() {
		t.Error("request should be denied after exhausting tokens")
	}
}

func TestSlidingWindow(t *testing.T) {
	limiter := NewSlidingWindowLimiter(time.Minute, 5)

	for i := 0; i < 5; i++ {
		if !limiter.Allow("test-key") {
			t.Errorf("request %d should be allowed", i)
		}
	}

	if limiter.Allow("test-key") {
		t.Error("request should be denied after reaching limit")
	}
}

func BenchmarkMemoryStoreTake(b *testing.B) {
	store := NewMemoryStore()
	defer store.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			store.Take("bench-key", 1000, time.Minute)
		}
	})
}
