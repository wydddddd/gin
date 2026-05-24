package auth

import (
	"testing"
	"time"
)

func TestGenerateAndParseToken(t *testing.T) {
	secret := "test-secret-key"
	claims := &Claims{
		UserID:   "user-123",
		Username: "testuser",
		Email:    "test@example.com",
		Roles:    []string{"admin", "user"},
		ExpireAt: time.Now().Add(time.Hour).Unix(),
		Issuer:   "test",
	}

	token, err := GenerateToken(claims, secret)
	if err != nil {
		t.Fatalf("GenerateToken failed: %v", err)
	}

	parsed, err := ParseToken(token, secret)
	if err != nil {
		t.Fatalf("ParseToken failed: %v", err)
	}

	if parsed.UserID != claims.UserID {
		t.Errorf("expected user_id %s, got %s", claims.UserID, parsed.UserID)
	}

	if parsed.Username != claims.Username {
		t.Errorf("expected username %s, got %s", claims.Username, parsed.Username)
	}
}

func TestExpiredToken(t *testing.T) {
	secret := "test-secret"
	claims := &Claims{
		UserID:   "user-456",
		Username: "expired",
		ExpireAt: time.Now().Add(-time.Hour).Unix(),
	}

	token, _ := GenerateToken(claims, secret)
	_, err := ParseToken(token, secret)

	if err != ErrTokenExpired {
		t.Errorf("expected ErrTokenExpired, got %v", err)
	}
}

func TestInvalidSignature(t *testing.T) {
	claims := &Claims{
		UserID:   "user-789",
		ExpireAt: time.Now().Add(time.Hour).Unix(),
	}

	token, _ := GenerateToken(claims, "secret-1")
	_, err := ParseToken(token, "secret-2")

	if err != ErrInvalidToken {
		t.Errorf("expected ErrInvalidToken, got %v", err)
	}
}

func TestHasRole(t *testing.T) {
	claims := &Claims{
		Roles: []string{"admin", "editor"},
	}

	if !claims.HasRole("admin") {
		t.Error("expected HasRole to return true for admin")
	}

	if claims.HasRole("superuser") {
		t.Error("expected HasRole to return false for superuser")
	}
}

func TestHashPassword(t *testing.T) {
	hash1 := hashPassword("password123")
	hash2 := hashPassword("password123")

	if hash1 != hash2 {
		t.Error("same password should produce same hash")
	}

	hash3 := hashPassword("different")
	if hash1 == hash3 {
		t.Error("different passwords should produce different hashes")
	}
}
