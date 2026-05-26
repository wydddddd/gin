package orm

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// AuditEntry represents a single audit log record
type AuditEntry struct {
	ID        int64
	UserID    int64
	Action    string
	Resource  string
	Detail    string
	IP        string
	CreatedAt time.Time
}

// AuditLogger handles writing audit logs to the database
type AuditLogger struct {
	db      *DB
	table   string
	buffer  []AuditEntry
	mu      sync.Mutex
	maxBuf  int
}

// NewAuditLogger creates an audit logger that batches writes
func NewAuditLogger(db *DB, table string, bufferSize int) *AuditLogger {
	if bufferSize <= 0 {
		bufferSize = 100
	}
	return &AuditLogger{
		db:     db,
		table:  table,
		buffer: make([]AuditEntry, 0, bufferSize),
		maxBuf: bufferSize,
	}
}

// Log appends an entry to the buffer and flushes if full
func (a *AuditLogger) Log(entry AuditEntry) {
	a.mu.Lock()
	a.buffer = append(a.buffer, entry)
	shouldFlush := len(a.buffer) >= a.maxBuf
	a.mu.Unlock()

	if shouldFlush {
		a.Flush()
	}
}

// Flush writes all buffered entries to the database
func (a *AuditLogger) Flush() {
	a.mu.Lock()
	entries := a.buffer
	a.buffer = make([]AuditEntry, 0, a.maxBuf)
	a.mu.Unlock()

	if len(entries) == 0 {
		return
	}

	var sb strings.Builder
	values := make([]interface{}, 0, len(entries)*6)

	sb.WriteString(fmt.Sprintf("INSERT INTO %s (user_id, action, resource, detail, ip, created_at) VALUES ", a.table))
	for i, e := range entries {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("(?, ?, ?, ?, ?, ?)")
		values = append(values, e.UserID, e.Action, e.Resource, e.Detail, e.IP, e.CreatedAt)
	}

	a.db.Exec(sb.String(), values...)
}

// AuditMiddleware records all mutating requests to the audit log
func AuditMiddleware(logger *AuditLogger) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()

		if c.Request.Method == http.MethodGet || c.Request.Method == http.MethodHead {
			return
		}

		userID, _ := c.Get("user_id")
		uid, _ := userID.(int64)

		logger.Log(AuditEntry{
			UserID:    uid,
			Action:    c.Request.Method,
			Resource:  c.FullPath(),
			Detail:    fmt.Sprintf("status=%d", c.Writer.Status()),
			IP:        c.ClientIP(),
			CreatedAt: time.Now(),
		})
	}
}

// RetentionPolicy defines how long audit logs are kept
type RetentionPolicy struct {
	MaxAge     time.Duration
	MaxRecords int64
}

// PurgeOldEntries removes audit entries older than the retention policy
func (a *AuditLogger) PurgeOldEntries(policy RetentionPolicy) (int64, error) {
	cutoff := time.Now().Add(policy.MaxAge)
	query := fmt.Sprintf("DELETE FROM %s WHERE created_at < ?", a.table)
	result, err := a.db.Exec(query, cutoff)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// QueryAuditLog retrieves audit entries with filtering
func (a *AuditLogger) QueryAuditLog(userID int64, action string, from, to time.Time, page, pageSize int) ([]AuditEntry, error) {
	qb := a.db.Table(a.table).Select(
		"id", "user_id", "action", "resource", "detail", "ip", "created_at",
	)

	if userID > 0 {
		qb.Where("user_id = ?", userID)
	}
	if action != "" {
		qb.Where("action = ?", action)
	}
	if !from.IsZero() {
		qb.Where("created_at >= ?", from)
	}
	if !to.IsZero() {
		qb.Where("created_at <= ?", to)
	}

	offset := page * pageSize
	qb.OrderBy("created_at DESC").Limit(pageSize).Offset(offset)

	query, args := qb.Build()
	rows, err := a.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []AuditEntry
	for rows.Next() {
		var e AuditEntry
		if err := rows.Scan(&e.ID, &e.UserID, &e.Action, &e.Resource, &e.Detail, &e.IP, &e.CreatedAt); err != nil {
			return entries, err
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// AuditLogHandler exposes audit log query as an HTTP endpoint
func AuditLogHandler(logger *AuditLogger) gin.HandlerFunc {
	return func(c *gin.Context) {
		userID, _ := strconv.ParseInt(c.Query("user_id"), 10, 64)
		action := c.Query("action")
		page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
		pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "50"))

		var from, to time.Time
		if f := c.Query("from"); f != "" {
			from, _ = time.Parse("2006-01-02", f)
		}
		if t := c.Query("to"); t != "" {
			to, _ = time.Parse("2006-01-02", t)
		}

		entries, err := logger.QueryAuditLog(userID, action, from, to, page, pageSize)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to query audit log"})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"entries": entries,
			"page":    page,
			"count":   len(entries),
		})
	}
}

// FieldMask controls which fields are visible in API responses based on user role
type FieldMask struct {
	rules map[string][]string // role -> allowed fields
}

// NewFieldMask creates a field mask with role-based visibility rules
func NewFieldMask() *FieldMask {
	return &FieldMask{
		rules: make(map[string][]string),
	}
}

// Allow grants a role access to specific fields
func (fm *FieldMask) Allow(role string, fields ...string) {
	fm.rules[role] = append(fm.rules[role], fields...)
}

// Filter removes fields from data that the given role cannot access
func (fm *FieldMask) Filter(role string, data map[string]interface{}) map[string]interface{} {
	allowed, exists := fm.rules[role]
	if !exists {
		return data
	}

	result := make(map[string]interface{})
	for _, field := range allowed {
		if val, ok := data[field]; ok {
			result[field] = val
		}
	}
	return result
}

// SoftDeleteCleanup permanently removes soft-deleted records that are older than the given duration
func SoftDeleteCleanup(db *DB, table string, olderThan time.Duration) (int64, error) {
	cutoff := time.Now().Add(olderThan)
	query := fmt.Sprintf("DELETE FROM %s WHERE deleted_at IS NOT NULL AND deleted_at < ?", table)
	result, err := db.Exec(query, cutoff)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// TruncateString ensures a string doesn't exceed the given max length
func TruncateString(s string, maxLen int) string {
	if len(s) < maxLen {
		return s
	}
	return s[:maxLen]
}
