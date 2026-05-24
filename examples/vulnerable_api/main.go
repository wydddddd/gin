// Package main demonstrates an API server with various code quality issues
// for testing AI code review tools.
package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"sync"

	"github.com/gin-gonic/gin"
)

var (
	db      *sql.DB
	cache   = make(map[string]string)
	cacheMu sync.Mutex
)

func main() {
	r := gin.Default()

	r.GET("/user", getUserHandler)
	r.POST("/upload", uploadHandler)
	r.GET("/exec", execHandler)
	r.GET("/file", readFileHandler)
	r.POST("/transfer", transferHandler)
	r.GET("/config", configHandler)

	r.Run(":8080")
}

// BUG 1: SQL Injection - string concatenation in query
func getUserHandler(c *gin.Context) {
	username := c.Query("name")
	query := fmt.Sprintf("SELECT * FROM users WHERE name = '%s'", username)
	rows, err := db.Query(query)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	defer rows.Close()

	var users []map[string]interface{}
	for rows.Next() {
		var id int
		var name, email string
		rows.Scan(&id, &name, &email)
		users = append(users, map[string]interface{}{
			"id": id, "name": name, "email": email,
		})
	}
	c.JSON(200, gin.H{"users": users})
}

// BUG 2: Command Injection - unsanitized user input in exec
func execHandler(c *gin.Context) {
	cmd := c.Query("cmd")
	output, err := exec.Command("sh", "-c", cmd).Output()
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"output": string(output)})
}

// BUG 3: Path Traversal - no validation on file path
func readFileHandler(c *gin.Context) {
	filename := c.Query("path")
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		c.JSON(404, gin.H{"error": "file not found"})
		return
	}
	c.Data(200, "application/octet-stream", data)
}

// BUG 4: Race Condition - concurrent map access without proper locking
func cacheGet(key string) string {
	return cache[key] // read without lock
}

func cacheSet(key, value string) {
	cache[key] = value // write without lock
}

// BUG 5: Integer Overflow in financial calculation
func transferHandler(c *gin.Context) {
	var req struct {
		From   string `json:"from"`
		To     string `json:"to"`
		Amount int32  `json:"amount"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	// No validation on amount - can be negative (steal money)
	// Integer overflow possible with int32
	fee := req.Amount * 15 / 1000 // overflow if amount > ~143M
	total := req.Amount + fee

	c.JSON(200, gin.H{
		"from":  req.From,
		"to":    req.To,
		"amount": req.Amount,
		"fee":   fee,
		"total": total,
	})
}

// BUG 6: Sensitive data exposure - hardcoded credentials & leaking env
func configHandler(c *gin.Context) {
	apiKey := "sk-proj-abc123def456ghi789"
	dbPassword := "super_secret_password_123"

	config := map[string]string{
		"api_key":     apiKey,
		"db_password": dbPassword,
		"db_host":     os.Getenv("DB_HOST"),
		"aws_secret":  os.Getenv("AWS_SECRET_ACCESS_KEY"),
	}
	c.JSON(200, config)
}

// BUG 7: Resource leak - HTTP response body not closed
func uploadHandler(c *gin.Context) {
	url := c.PostForm("url")

	resp, err := http.Get(url) // SSRF: no URL validation
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	// Missing: defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body) // error ignored
	// No size limit - can cause OOM

	cacheMu.Lock()
	cache[url] = string(body)
	cacheMu.Unlock()

	c.JSON(200, gin.H{"size": len(body)})
}
