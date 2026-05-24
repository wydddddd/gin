package session

import (
	"testing"
	"time"
)

func TestMemoryStore_BasicOperations(t *testing.T) {
	store := NewMemoryStore(time.Hour)

	session := &Session{
		ID:        "test-session-1",
		Data:      map[string]interface{}{"user": "alice"},
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(time.Hour),
	}

	// Save
	if err := store.Save(session); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Get
	got, err := store.Get("test-session-1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got.ID != session.ID {
		t.Errorf("got ID %s, want %s", got.ID, session.ID)
	}

	// Delete
	if err := store.Delete("test-session-1"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, err = store.Get("test-session-1")
	if err != ErrSessionNotFound {
		t.Errorf("expected ErrSessionNotFound, got %v", err)
	}
}

func TestMemoryStore_GC(t *testing.T) {
	store := NewMemoryStore(time.Hour)

	// Add expired session
	expired := &Session{
		ID:        "expired-1",
		Data:      map[string]interface{}{},
		CreatedAt: time.Now().Add(-2 * time.Hour),
		ExpiresAt: time.Now().Add(-1 * time.Hour),
	}
	store.Save(expired)

	// Add valid session
	valid := &Session{
		ID:        "valid-1",
		Data:      map[string]interface{}{},
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(time.Hour),
	}
	store.Save(valid)

	// Run GC
	store.GC()

	// Expired should be gone
	_, err := store.Get("expired-1")
	if err != ErrSessionNotFound {
		t.Error("expired session should have been collected")
	}

	// Valid should remain
	_, err = store.Get("valid-1")
	if err != nil {
		t.Error("valid session should still exist")
	}
}

func TestSession_SetGet(t *testing.T) {
	s := &Session{
		Data: make(map[string]interface{}),
	}

	s.Set("name", "bob")
	val, ok := s.Get("name")
	if !ok || val != "bob" {
		t.Errorf("expected bob, got %v", val)
	}

	s.Delete("name")
	_, ok = s.Get("name")
	if ok {
		t.Error("expected key to be deleted")
	}
}

func TestSession_Clear(t *testing.T) {
	s := &Session{
		Data: map[string]interface{}{"a": 1, "b": 2, "c": 3},
	}

	s.Clear()
	if len(s.Data) != 0 {
		t.Errorf("expected empty data, got %d items", len(s.Data))
	}
	if !s.Modified {
		t.Error("expected Modified to be true after Clear")
	}
}

func TestSession_IsExpired(t *testing.T) {
	s := &Session{
		ExpiresAt: time.Now().Add(-1 * time.Minute),
	}
	if !s.IsExpired() {
		t.Error("session should be expired")
	}

	s.ExpiresAt = time.Now().Add(1 * time.Hour)
	if s.IsExpired() {
		t.Error("session should not be expired")
	}
}

func TestGenerateSessionID(t *testing.T) {
	id1 := generateSessionID()
	id2 := generateSessionID()

	if id1 == id2 {
		t.Error("session IDs should be unique")
	}

	if len(id1) != 32 { // 16 bytes = 32 hex chars
		t.Errorf("expected 32 char ID, got %d", len(id1))
	}
}

func TestCookieStore_EncodeDecode(t *testing.T) {
	secret := []byte("test-secret-key-16bytes!")
	data := []byte(`{"ID":"test","Data":{"key":"value"}}`)

	encoded := encode(data, secret)
	decoded, err := decode(encoded, secret)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if string(decoded) != string(data) {
		t.Errorf("roundtrip failed: got %s", string(decoded))
	}
}
