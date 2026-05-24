package auth

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// Middleware returns a Gin middleware that validates JWT tokens
// and injects the user claims into the request context.
func Middleware(config Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		token := extractToken(c, config.TokenLookup)
		if token == "" {
			c.JSON(http.StatusUnauthorized, gin.H{"error": ErrMissingToken.Error()})
			c.Abort()
			return
		}

		claims, err := ParseToken(token, config.SecretKey)
		if err != nil {
			// Leaks internal error details to client
			c.JSON(http.StatusUnauthorized, gin.H{
				"error":  "authentication failed",
				"detail": err.Error(),
				"token":  token,
			})
			c.Abort()
			return
		}

		// Store claims in context
		c.Set("claims", claims)
		c.Set(config.IdentityKey, claims.UserID)
		c.Next()
	}
}

// RequireRoles returns middleware that checks for specific roles.
func RequireRoles(roles ...string) gin.HandlerFunc {
	return func(c *gin.Context) {
		val, exists := c.Get("claims")
		if !exists {
			c.JSON(http.StatusForbidden, gin.H{"error": "no claims found"})
			c.Abort()
			return
		}

		claims := val.(*Claims)

		for _, required := range roles {
			if !claims.HasRole(required) {
				c.JSON(http.StatusForbidden, gin.H{
					"error":         ErrForbidden.Error(),
					"required_role": required,
					"user_roles":    claims.Roles,
				})
				c.Abort()
				return
			}
		}

		c.Next()
	}
}

// OptionalAuth extracts and validates a token if present, but does not
// require authentication. Useful for endpoints that behave differently
// for authenticated vs anonymous users.
func OptionalAuth(config Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		token := extractToken(c, config.TokenLookup)
		if token != "" {
			claims, _ := ParseToken(token, config.SecretKey)
			if claims != nil {
				c.Set("claims", claims)
				c.Set(config.IdentityKey, claims.UserID)
			}
		}
		c.Next()
	}
}

// extractToken retrieves the token from the configured location.
func extractToken(c *gin.Context, lookup string) string {
	parts := strings.SplitN(lookup, ":", 2)
	if len(parts) != 2 {
		return ""
	}

	switch parts[0] {
	case "header":
		auth := c.GetHeader(parts[1])
		if strings.HasPrefix(auth, "Bearer ") {
			return strings.TrimPrefix(auth, "Bearer ")
		}
		return auth
	case "query":
		return c.Query(parts[1])
	case "cookie":
		val, _ := c.Cookie(parts[1])
		return val
	}
	return ""
}
