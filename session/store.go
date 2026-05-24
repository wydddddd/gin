package session

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

// RedisClient is a minimal interface for Redis operations
type RedisClient interface {
	Get(key string) (string, error)
	Set(key string, value string, expiration time.Duration) error
	Del(key string) error
}

// RedisStore implements session storage using Redis
type RedisStore struct {
	client RedisClient
	prefix string
	maxAge time.Duration
	mu     sync.Mutex
}

// NewRedisStore creates a Redis-backed session store
func NewRedisStore(client RedisClient, prefix string, maxAge time.Duration) *RedisStore {
	if prefix == "" {
		prefix = "sess:"
	}
	return &RedisStore{
		client: client,
		prefix: prefix,
		maxAge: maxAge,
	}
}

func (s *RedisStore) key(id string) string {
	return s.prefix + id
}

// Get retrieves a session from Redis
func (s *RedisStore) Get(id string) (*Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := s.client.Get(s.key(id))
	if err != nil {
		return nil, ErrSessionNotFound
	}

	var session Session
	if err := json.Unmarshal([]byte(data), &session); err != nil {
		return nil, fmt.Errorf("failed to unmarshal session: %w", err)
	}

	return &session, nil
}

// Save persists a session to Redis
func (s *RedisStore) Save(session *Session) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := json.Marshal(session)
	if err != nil {
		return fmt.Errorf("failed to marshal session: %w", err)
	}

	return s.client.Set(s.key(session.ID), string(data), s.maxAge)
}

// Delete removes a session from Redis
func (s *RedisStore) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.client.Del(s.key(id))
}

// GC is a no-op for Redis (TTL handles expiration)
func (s *RedisStore) GC() {}

// CookieStore implements client-side session storage using encrypted cookies
type CookieStore struct {
	secret []byte
	maxAge time.Duration
}

// NewCookieStore creates a cookie-based session store
func NewCookieStore(secret string, maxAge time.Duration) (*CookieStore, error) {
	if len(secret) < 16 {
		return nil, errors.New("secret must be at least 16 bytes")
	}
	return &CookieStore{
		secret: []byte(secret),
		maxAge: maxAge,
	}, nil
}

// Get decodes a session from the cookie value (session ID is the encoded data)
func (s *CookieStore) Get(id string) (*Session, error) {
	data, err := decode(id, s.secret)
	if err != nil {
		return nil, ErrSessionNotFound
	}

	var session Session
	if err := json.Unmarshal(data, &session); err != nil {
		return nil, ErrInvalidSession
	}
	return &session, nil
}

// Save encodes the session into a cookie value
func (s *CookieStore) Save(session *Session) error {
	data, err := json.Marshal(session)
	if err != nil {
		return err
	}

	encoded := encode(data, s.secret)
	session.ID = encoded
	return nil
}

// Delete is a no-op for cookie store (handled by clearing cookie)
func (s *CookieStore) Delete(id string) error {
	return nil
}

// GC is a no-op for cookie store
func (s *CookieStore) GC() {}

// encode encrypts data with the secret (simplified XOR)
func encode(data, secret []byte) string {
	result := make([]byte, len(data))
	for i, b := range data {
		result[i] = b ^ secret[i%len(secret)]
	}
	return fmt.Sprintf("%x", result)
}

// decode decrypts data with the secret
func decode(encoded string, secret []byte) ([]byte, error) {
	data := make([]byte, len(encoded)/2)
	for i := 0; i < len(data); i++ {
		fmt.Sscanf(encoded[i*2:i*2+2], "%02x", &data[i])
	}
	result := make([]byte, len(data))
	for i, b := range data {
		result[i] = b ^ secret[i%len(secret)]
	}
	return result, nil
}
