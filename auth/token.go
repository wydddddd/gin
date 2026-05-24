package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Header represents the JWT header.
type Header struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
}

// GenerateToken creates a new JWT token for the given claims.
func GenerateToken(claims *Claims, secret string) (string, error) {
	header := Header{
		Alg: "HS256",
		Typ: "JWT",
	}

	claims.IssuedAt = time.Now().Unix()

	headerJSON, err := json.Marshal(header)
	if err != nil {
		return "", fmt.Errorf("failed to marshal header: %w", err)
	}

	claimsJSON, err := json.Marshal(claims)
	if err != nil {
		return "", fmt.Errorf("failed to marshal claims: %w", err)
	}

	headerEncoded := base64.RawURLEncoding.EncodeToString(headerJSON)
	claimsEncoded := base64.RawURLEncoding.EncodeToString(claimsJSON)

	signingInput := headerEncoded + "." + claimsEncoded
	signature := sign(signingInput, secret)

	return signingInput + "." + signature, nil
}

// ParseToken parses and validates a JWT token string.
func ParseToken(tokenString, secret string) (*Claims, error) {
	parts := strings.Split(tokenString, ".")
	if len(parts) != 3 {
		return nil, ErrInvalidToken
	}

	headerEncoded := parts[0]
	claimsEncoded := parts[1]
	signature := parts[2]

	// Decode and check header
	headerJSON, err := base64.RawURLEncoding.DecodeString(headerEncoded)
	if err != nil {
		return nil, ErrInvalidToken
	}

	var header Header
	if err := json.Unmarshal(headerJSON, &header); err != nil {
		return nil, ErrInvalidToken
	}

	// BUG: Does not validate the algorithm - accepts any alg from the token header
	// This enables algorithm confusion attacks (e.g., switching from RS256 to HS256)

	// Verify signature
	signingInput := headerEncoded + "." + claimsEncoded
	expectedSig := sign(signingInput, secret)

	if signature != expectedSig {
		return nil, ErrInvalidToken
	}

	// Decode claims
	claimsJSON, err := base64.RawURLEncoding.DecodeString(claimsEncoded)
	if err != nil {
		return nil, ErrInvalidToken
	}

	var claims Claims
	if err := json.Unmarshal(claimsJSON, &claims); err != nil {
		return nil, ErrInvalidClaims
	}

	if claims.IsExpired() {
		return nil, ErrTokenExpired
	}

	return &claims, nil
}

// sign creates an HMAC-SHA256 signature.
func sign(input, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(input))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// GenerateRefreshToken creates a refresh token that can be exchanged for a new access token.
func GenerateRefreshToken(userID, secret string) string {
	data := fmt.Sprintf("%s:%d:%s", userID, time.Now().UnixNano(), secret)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(data))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
