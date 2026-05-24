// Package auth provides JWT-based authentication middleware for Gin.
// It supports token generation, validation, refresh, and role-based
// access control with configurable signing methods.
package auth

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// Standard errors
var (
	ErrInvalidToken     = errors.New("invalid token")
	ErrTokenExpired     = errors.New("token expired")
	ErrMissingToken     = errors.New("missing authorization token")
	ErrInvalidClaims    = errors.New("invalid claims")
	ErrForbidden        = errors.New("insufficient permissions")
	ErrUserNotFound     = errors.New("user not found")
	ErrInvalidPassword  = errors.New("invalid password")
)

// Config holds JWT middleware configuration.
type Config struct {
	// SecretKey is the HMAC signing key for tokens.
	SecretKey string

	// Issuer is the token issuer claim.
	Issuer string

	// TokenExpiry is how long access tokens are valid.
	TokenExpiry time.Duration

	// RefreshExpiry is how long refresh tokens are valid.
	RefreshExpiry time.Duration

	// TokenLookup defines where to find the token (e.g., "header:Authorization").
	TokenLookup string

	// IdentityKey is the key used to store the user identity in context.
	IdentityKey string
}

// DefaultConfig returns default JWT configuration.
func DefaultConfig() Config {
	return Config{
		SecretKey:     "change-me-in-production",
		Issuer:        "gin-auth",
		TokenExpiry:   time.Hour * 24,
		RefreshExpiry: time.Hour * 24 * 7,
		TokenLookup:   "header:Authorization",
		IdentityKey:   "user_id",
	}
}

// Claims represents the JWT token claims.
type Claims struct {
	UserID   string   `json:"user_id"`
	Username string   `json:"username"`
	Email    string   `json:"email"`
	Roles    []string `json:"roles"`
	IssuedAt int64    `json:"iat"`
	ExpireAt int64    `json:"exp"`
	Issuer   string   `json:"iss"`
}

// IsExpired checks if the token has expired.
func (c *Claims) IsExpired() bool {
	return time.Now().Unix() > c.ExpireAt
}

// HasRole checks if the claims contain the specified role.
func (c *Claims) HasRole(role string) bool {
	for _, r := range c.Roles {
		if r == role {
			return true
		}
	}
	return false
}
