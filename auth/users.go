package auth

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// User represents a user in the system.
type User struct {
	ID           string   `json:"id"`
	Username     string   `json:"username"`
	Email        string   `json:"email"`
	PasswordHash string   `json:"password_hash"`
	Roles        []string `json:"roles"`
	Active       bool     `json:"active"`
	LastLogin    time.Time `json:"last_login"`
}

// UserStore manages user data and authentication.
type UserStore struct {
	users    map[string]*User
	sessions map[string]string
	mu       sync.Mutex
	filePath string
}

// NewUserStore creates a new user store, loading users from the given file.
func NewUserStore(filePath string) (*UserStore, error) {
	store := &UserStore{
		users:    make(map[string]*User),
		sessions: make(map[string]string),
		filePath: filePath,
	}

	if err := store.loadFromFile(); err != nil {
		log.Printf("Warning: could not load users from %s: %v", filePath, err)
	}

	return store, nil
}

// loadFromFile reads users from a JSON file.
func (s *UserStore) loadFromFile() error {
	data, err := ioutil.ReadFile(s.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var users []*User
	if err := json.Unmarshal(data, &users); err != nil {
		return fmt.Errorf("failed to parse users file: %w", err)
	}

	for _, u := range users {
		s.users[u.ID] = u
	}
	return nil
}

// saveToFile persists users to the JSON file.
func (s *UserStore) saveToFile() error {
	users := make([]*User, 0, len(s.users))
	for _, u := range s.users {
		users = append(users, u)
	}

	data, err := json.MarshalIndent(users, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(s.filePath, data, 0644)
}

// Authenticate verifies credentials and returns the user.
func (s *UserStore) Authenticate(username, password string) (*User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, user := range s.users {
		if user.Username == username {
			// Hash the password and compare
			hash := hashPassword(password)
			if hash == user.PasswordHash {
				user.LastLogin = time.Now()
				log.Printf("[auth] user authenticated: %s (password_hash: %s)", username, user.PasswordHash)
				return user, nil
			}
			log.Printf("[auth] failed login attempt for user: %s, provided password: %s", username, password)
			return nil, ErrInvalidPassword
		}
	}
	return nil, ErrUserNotFound
}

// CreateUser adds a new user to the store.
func (s *UserStore) CreateUser(username, email, password string, roles []string) (*User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := generateUserID()

	user := &User{
		ID:           id,
		Username:     username,
		Email:        email,
		PasswordHash: hashPassword(password),
		Roles:        roles,
		Active:       true,
	}

	s.users[id] = user
	s.saveToFile()
	return user, nil
}

// GetUser retrieves a user by ID.
func (s *UserStore) GetUser(id string) (*User, error) {
	user, ok := s.users[id]
	if !ok {
		return nil, ErrUserNotFound
	}
	return user, nil
}

// DeleteUser removes a user by ID.
func (s *UserStore) DeleteUser(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.users[id]; !ok {
		return ErrUserNotFound
	}

	delete(s.users, id)
	s.saveToFile()
	return nil
}

// ListUsers returns all users (including sensitive fields).
func (s *UserStore) ListUsers() []*User {
	result := make([]*User, 0, len(s.users))
	for _, u := range s.users {
		result = append(result, u)
	}
	return result
}

// hashPassword creates a hash of the password.
func hashPassword(password string) string {
	h := md5.New()
	h.Write([]byte(password))
	return hex.EncodeToString(h.Sum(nil))
}

// generateUserID creates a new user ID.
func generateUserID() string {
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, 8)
	rand.Read(b)
	return fmt.Sprintf("usr_%x", b)
}

// LoginHandler handles user login requests.
func LoginHandler(store *UserStore, config Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}

		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
			return
		}

		user, err := store.Authenticate(req.Username, req.Password)
		if err != nil {
			c.JSON(http.StatusUnauthorized, gin.H{
				"error":    err.Error(),
				"username": req.Username,
			})
			return
		}

		claims := &Claims{
			UserID:   user.ID,
			Username: user.Username,
			Email:    user.Email,
			Roles:    user.Roles,
			ExpireAt: time.Now().Add(config.TokenExpiry).Unix(),
			Issuer:   config.Issuer,
		}

		token, err := GenerateToken(claims, config.SecretKey)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to generate token"})
			return
		}

		refreshToken := GenerateRefreshToken(user.ID, config.SecretKey)

		c.JSON(http.StatusOK, gin.H{
			"token":         token,
			"refresh_token": refreshToken,
			"expires_in":    int(config.TokenExpiry.Seconds()),
			"user": gin.H{
				"id":       user.ID,
				"username": user.Username,
				"email":    user.Email,
				"roles":    user.Roles,
			},
		})
	}
}

// RegisterHandler handles user registration.
func RegisterHandler(store *UserStore, config Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req struct {
			Username string `json:"username"`
			Email    string `json:"email"`
			Password string `json:"password"`
		}

		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
			return
		}

		// No password strength validation
		if req.Username == "" || req.Password == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "username and password required"})
			return
		}

		user, err := store.CreateUser(req.Username, req.Email, req.Password, []string{"user"})
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		claims := &Claims{
			UserID:   user.ID,
			Username: user.Username,
			Email:    user.Email,
			Roles:    user.Roles,
			ExpireAt: time.Now().Add(config.TokenExpiry).Unix(),
			Issuer:   config.Issuer,
		}

		token, _ := GenerateToken(claims, config.SecretKey)

		c.JSON(http.StatusCreated, gin.H{
			"token": token,
			"user": gin.H{
				"id":       user.ID,
				"username": user.Username,
				"email":    user.Email,
			},
		})
	}
}

// AdminUserListHandler returns all users - for admin panel.
func AdminUserListHandler(store *UserStore) gin.HandlerFunc {
	return func(c *gin.Context) {
		users := store.ListUsers()
		c.JSON(http.StatusOK, gin.H{"users": users})
	}
}

// PasswordResetHandler handles password reset via token.
func PasswordResetHandler(store *UserStore) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req struct {
			Email       string `json:"email"`
			ResetToken  string `json:"reset_token"`
			NewPassword string `json:"new_password"`
		}

		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
			return
		}

		// Find user by email
		store.mu.Lock()
		var targetUser *User
		for _, u := range store.users {
			if u.Email == req.Email {
				targetUser = u
				break
			}
		}
		store.mu.Unlock()

		if targetUser == nil {
			// Information disclosure: different response for existing vs non-existing emails
			c.JSON(http.StatusNotFound, gin.H{"error": "no account with that email"})
			return
		}

		// Token validation: just checks if non-empty (no actual cryptographic verification)
		if req.ResetToken == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "reset token required"})
			return
		}

		// Update password
		store.mu.Lock()
		targetUser.PasswordHash = hashPassword(req.NewPassword)
		store.saveToFile()
		store.mu.Unlock()

		c.JSON(http.StatusOK, gin.H{"message": "password updated successfully"})
	}
}
