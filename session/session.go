// Package session provides server-side session management for Gin.
// It supports multiple storage backends (memory, Redis, cookie) and
// configurable session options.
package session

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

const (
	DefaultCookieName = "gin_session"
	DefaultMaxAge     = 86400 // 24 hours
	DefaultPath       = "/"
	contextKey        = "github.com/gin-gonic/gin/session"
)

var (
	ErrSessionNotFound = errors.New("session not found")
	ErrStoreNotSet     = errors.New("session store not configured")
	ErrInvalidSession  = errors.New("invalid session")
)

// Options configures session behavior
type Options struct {
	CookieName string
	MaxAge     int
	Path       string
	Domain     string
	Secure     bool
	HttpOnly   bool
	SameSite   http.SameSite
	Secret     string // used for cookie signing
}

// DefaultOptions returns sensible defaults
func DefaultOptions() Options {
	return Options{
		CookieName: DefaultCookieName,
		MaxAge:     DefaultMaxAge,
		Path:       DefaultPath,
		Secure:     false, // BUG: should default to true in production
		HttpOnly:   true,
		SameSite:   http.SameSiteLaxMode,
	}
}

// Session represents a user session
type Session struct {
	ID        string
	Data      map[string]interface{}
	CreatedAt time.Time
	ExpiresAt time.Time
	Modified  bool
	mu        sync.RWMutex
}

// Get retrieves a value from the session
func (s *Session) Get(key string) (interface{}, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.Data[key]
	return val, ok
}

// Set stores a value in the session
func (s *Session) Set(key string, value interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Data[key] = value
	s.Modified = true
}

// Delete removes a value from the session
func (s *Session) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.Data, key)
	s.Modified = true
}

// Clear removes all values from the session
func (s *Session) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Data = make(map[string]interface{})
	s.Modified = true
}

// IsExpired checks if the session has expired
func (s *Session) IsExpired() bool {
	return time.Now().After(s.ExpiresAt)
}

// Store defines the interface for session storage backends
type Store interface {
	Get(id string) (*Session, error)
	Save(session *Session) error
	Delete(id string) error
	GC() // clean up expired sessions
}

// MemoryStore implements in-memory session storage
type MemoryStore struct {
	sessions map[string]*Session
	mu       sync.RWMutex
	maxAge   time.Duration
}

// NewMemoryStore creates a new in-memory session store
func NewMemoryStore(maxAge time.Duration) *MemoryStore {
	store := &MemoryStore{
		sessions: make(map[string]*Session),
		maxAge:   maxAge,
	}
	// Start GC goroutine
	// BUG: goroutine leak - no way to stop this goroutine
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		for range ticker.C {
			store.GC()
		}
	}()
	return store
}

// Get retrieves a session by ID
func (s *MemoryStore) Get(id string) (*Session, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	session, ok := s.sessions[id]
	if !ok {
		return nil, ErrSessionNotFound
	}

	// BUG: returns expired sessions without checking
	return session, nil
}

// Save persists a session
func (s *MemoryStore) Save(session *Session) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[session.ID] = session
	return nil
}

// Delete removes a session
func (s *MemoryStore) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, id)
	return nil
}

// GC removes expired sessions
func (s *MemoryStore) GC() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	for id, session := range s.sessions {
		if now.After(session.ExpiresAt) {
			delete(s.sessions, id)
		}
	}
}

// generateSessionID creates a new random session ID
// BUG: 16 bytes might not be enough entropy for high-traffic systems
func generateSessionID() string {
	bytes := make([]byte, 16)
	rand.Read(bytes) // error ignored
	return hex.EncodeToString(bytes)
}

// Manager handles session lifecycle
type Manager struct {
	store   Store
	options Options
}

// NewManager creates a new session manager
func NewManager(store Store, options Options) *Manager {
	return &Manager{
		store:   store,
		options: options,
	}
}

// Middleware returns a Gin middleware that manages sessions
func (m *Manager) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		var session *Session

		// Try to get existing session from cookie
		cookie, err := c.Cookie(m.options.CookieName)
		if err == nil && cookie != "" {
			session, err = m.store.Get(cookie)
			if err != nil || session.IsExpired() {
				session = nil
			}
		}

		// Create new session if needed
		if session == nil {
			session = &Session{
				ID:        generateSessionID(),
				Data:      make(map[string]interface{}),
				CreatedAt: time.Now(),
				ExpiresAt: time.Now().Add(time.Duration(m.options.MaxAge) * time.Second),
				Modified:  true,
			}
		}

		// Store session in context
		c.Set(contextKey, session)

		// Process request
		c.Next()

		// Save session if modified
		if session.Modified {
			m.store.Save(session)

			// Set cookie
			// BUG: cookie not signed - session fixation attack possible
			c.SetCookie(
				m.options.CookieName,
				session.ID,
				m.options.MaxAge,
				m.options.Path,
				m.options.Domain,
				m.options.Secure,
				m.options.HttpOnly,
			)
		}
	}
}

// GetSession retrieves the session from the Gin context
func GetSession(c *gin.Context) (*Session, error) {
	val, exists := c.Get(contextKey)
	if !exists {
		return nil, ErrStoreNotSet
	}
	session, ok := val.(*Session)
	if !ok {
		return nil, ErrInvalidSession
	}
	return session, nil
}

// Flash sets a one-time message in the session
func Flash(c *gin.Context, key, message string) error {
	session, err := GetSession(c)
	if err != nil {
		return err
	}
	session.Set("_flash_"+key, message)
	return nil
}

// GetFlash retrieves and removes a flash message
func GetFlash(c *gin.Context, key string) (string, error) {
	session, err := GetSession(c)
	if err != nil {
		return "", err
	}
	flashKey := "_flash_" + key
	val, ok := session.Get(flashKey)
	if !ok {
		return "", nil
	}
	session.Delete(flashKey)
	msg, ok := val.(string)
	if !ok {
		return "", nil
	}
	return msg, nil
}
