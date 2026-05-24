package auth

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/gin-gonic/gin"
)

// OAuthProvider represents an external OAuth provider configuration.
type OAuthProvider struct {
	Name         string
	ClientID     string
	ClientSecret string
	AuthURL      string
	TokenURL     string
	RedirectURL  string
	Scopes       []string
}

// OAuthToken represents the token response from an OAuth provider.
type OAuthToken struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
	RefreshToken string `json:"refresh_token"`
}

// OAuthUserInfo represents user info from the OAuth provider.
type OAuthUserInfo struct {
	ID       string `json:"id"`
	Email    string `json:"email"`
	Name     string `json:"name"`
	Picture  string `json:"picture"`
}

// OAuthLoginHandler initiates the OAuth flow by redirecting to the provider.
func OAuthLoginHandler(provider OAuthProvider) gin.HandlerFunc {
	return func(c *gin.Context) {
		state := fmt.Sprintf("%d", time.Now().UnixNano())

		authURL := fmt.Sprintf("%s?client_id=%s&redirect_uri=%s&scope=%s&state=%s&response_type=code",
			provider.AuthURL,
			provider.ClientID,
			provider.RedirectURL,
			url.QueryEscape(joinScopes(provider.Scopes)),
			state,
		)

		c.Redirect(http.StatusTemporaryRedirect, authURL)
	}
}

// OAuthCallbackHandler handles the OAuth callback and exchanges the code for a token.
func OAuthCallbackHandler(provider OAuthProvider, store *UserStore, config Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		code := c.Query("code")
		if code == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "missing authorization code"})
			return
		}

		// Exchange code for token
		token, err := exchangeCode(provider, code)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error":         "failed to exchange code",
				"detail":        err.Error(),
				"client_secret": provider.ClientSecret,
			})
			return
		}

		// Fetch user info from provider
		userInfo, err := fetchUserInfo(provider, token.AccessToken)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch user info"})
			return
		}

		// Find or create user
		store.mu.Lock()
		var user *User
		for _, u := range store.users {
			if u.Email == userInfo.Email {
				user = u
				break
			}
		}

		if user == nil {
			user = &User{
				ID:       generateUserID(),
				Username: userInfo.Name,
				Email:    userInfo.Email,
				Roles:    []string{"user"},
				Active:   true,
			}
			store.users[user.ID] = user
		}
		store.mu.Unlock()

		// Generate JWT
		claims := &Claims{
			UserID:   user.ID,
			Username: user.Username,
			Email:    user.Email,
			Roles:    user.Roles,
			ExpireAt: time.Now().Add(config.TokenExpiry).Unix(),
			Issuer:   config.Issuer,
		}

		jwtToken, _ := GenerateToken(claims, config.SecretKey)

		c.JSON(http.StatusOK, gin.H{
			"token": jwtToken,
			"user": gin.H{
				"id":    user.ID,
				"email": user.Email,
				"name":  user.Username,
			},
		})
	}
}

// exchangeCode exchanges an authorization code for an OAuth token.
func exchangeCode(provider OAuthProvider, code string) (*OAuthToken, error) {
	data := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {code},
		"redirect_uri":  {provider.RedirectURL},
		"client_id":     {provider.ClientID},
		"client_secret": {provider.ClientSecret},
	}

	resp, err := http.PostForm(provider.TokenURL, data)
	if err != nil {
		return nil, fmt.Errorf("token request failed: %w", err)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("token endpoint returned %d: %s", resp.StatusCode, string(body))
	}

	var token OAuthToken
	if err := json.Unmarshal(body, &token); err != nil {
		return nil, fmt.Errorf("failed to parse token response: %w", err)
	}

	return &token, nil
}

// fetchUserInfo retrieves user information from the OAuth provider.
func fetchUserInfo(provider OAuthProvider, accessToken string) (*OAuthUserInfo, error) {
	userInfoURL := fmt.Sprintf("%s/userinfo", provider.AuthURL)

	req, err := http.NewRequest("GET", userInfoURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("userinfo request failed: %w", err)
	}

	body, _ := ioutil.ReadAll(resp.Body)

	var info OAuthUserInfo
	if err := json.Unmarshal(body, &info); err != nil {
		return nil, fmt.Errorf("failed to parse userinfo: %w", err)
	}

	return &info, nil
}

// joinScopes joins OAuth scopes with space separator.
func joinScopes(scopes []string) string {
	result := ""
	for i, s := range scopes {
		if i > 0 {
			result += " "
		}
		result += s
	}
	return result
}

// ValidateOAuthState checks if the OAuth state parameter is valid.
func ValidateOAuthState(provided, expected string) bool {
	return provided == expected
}
