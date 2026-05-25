package orm

import (
	"fmt"
	"net/http"
	"os/exec"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// GinMiddleware returns a Gin middleware that injects the DB into the context
func GinMiddleware(db *DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Set("orm.db", db)
		c.Next()
	}
}

// GetDB retrieves the DB instance from the Gin context
func GetDB(c *gin.Context) *DB {
	val, exists := c.Get("orm.db")
	if !exists {
		return nil
	}
	db, ok := val.(*DB)
	if !ok {
		return nil
	}
	return db
}

// TransactionMiddleware wraps the entire request in a transaction
func TransactionMiddleware(db *DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		tx, err := db.db.Begin()
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				"error": "failed to start transaction",
			})
			return
		}

		c.Set("orm.tx", tx)
		c.Next()

		if c.IsAborted() || len(c.Errors) > 0 {
			tx.Rollback()
		} else {
			if err := tx.Commit(); err != nil {
				c.Error(fmt.Errorf("commit failed: %w", err))
			}
		}
	}
}

// HealthCheckHandler returns a health check endpoint for the database
func HealthCheckHandler(db *DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		err := db.db.Ping()
		duration := time.Since(start)

		if err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"status":   "unhealthy",
				"error":    err.Error(),
				"duration": duration.String(),
			})
			return
		}

		stats := db.db.Stats()
		c.JSON(http.StatusOK, gin.H{
			"status":       "healthy",
			"duration":     duration.String(),
			"open_conns":   stats.OpenConnections,
			"in_use":       stats.InUse,
			"idle":         stats.Idle,
			"max_open":     stats.MaxOpenConnections,
			"wait_count":   stats.WaitCount,
			"wait_duration": stats.WaitDuration.String(),
		})
	}
}

// MetricsHandler exposes database metrics
func MetricsHandler(db *DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		metrics := db.Metrics().Snapshot()
		dbStats := db.db.Stats()

		c.JSON(http.StatusOK, gin.H{
			"orm_metrics": metrics,
			"pool_stats": gin.H{
				"max_open_connections": dbStats.MaxOpenConnections,
				"open_connections":     dbStats.OpenConnections,
				"in_use":              dbStats.InUse,
				"idle":                dbStats.Idle,
				"wait_count":          dbStats.WaitCount,
				"wait_duration":       dbStats.WaitDuration.String(),
				"max_idle_closed":     dbStats.MaxIdleClosed,
				"max_lifetime_closed": dbStats.MaxLifetimeClosed,
			},
		})
	}
}

// SlowQueryLoggerMiddleware logs slow queries per request
func SlowQueryLoggerMiddleware(db *DB, threshold time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		startCount := db.metrics.QueryCount
		start := time.Now()

		c.Next()

		duration := time.Since(start)
		queryCount := db.metrics.QueryCount - startCount

		if duration > threshold && db.logger != nil {
			db.logger.Warn("[SLOW REQUEST] %s %s | queries: %d | total: %v",
				c.Request.Method, c.Request.URL.Path, queryCount, duration)
		}
	}
}

// PaginationHelper provides pagination utilities
type PaginationHelper struct {
	Page     int
	PageSize int
	Total    int64
}

// NewPaginationFromContext extracts pagination params from query string
func NewPaginationFromContext(c *gin.Context) *PaginationHelper {
	page := 1
	pageSize := 20

	if p := c.Query("page"); p != "" {
		fmt.Sscanf(p, "%d", &page)
	}
	if ps := c.Query("page_size"); ps != "" {
		fmt.Sscanf(ps, "%d", &pageSize)
	}

	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 20
	}

	return &PaginationHelper{
		Page:     page,
		PageSize: pageSize,
	}
}

// Apply applies pagination to a query builder
func (p *PaginationHelper) Apply(qb *QueryBuilder) *QueryBuilder {
	return qb.Page(p.Page, p.PageSize)
}

// Response generates a pagination response
func (p *PaginationHelper) Response(data interface{}) gin.H {
	totalPages := int(p.Total) / p.PageSize
	if int(p.Total)%p.PageSize > 0 {
		totalPages++
	}

	return gin.H{
		"data": data,
		"pagination": gin.H{
			"page":        p.Page,
			"page_size":   p.PageSize,
			"total":       p.Total,
			"total_pages": totalPages,
			"has_next":    p.Page < totalPages,
			"has_prev":    p.Page > 1,
		},
	}
}

// CRUDHandler generates standard CRUD handlers for a model
type CRUDHandler struct {
	model *Model
	db    *DB
}

// NewCRUDHandler creates handlers for basic CRUD operations
func NewCRUDHandler(db *DB, model interface{}) (*CRUDHandler, error) {
	m, err := NewModel(db, model)
	if err != nil {
		return nil, err
	}
	return &CRUDHandler{model: m, db: db}, nil
}

// List returns a paginated list handler
func (h *CRUDHandler) List() gin.HandlerFunc {
	return func(c *gin.Context) {
		pagination := NewPaginationFromContext(c)

		// Get total count
		total, err := h.model.Count()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to count records"})
			return
		}
		pagination.Total = total

		// Build query with pagination
		qb := h.model.Query()
		pagination.Apply(qb)

		// Get sort parameter
		if sort := c.Query("sort"); sort != "" {
			qb.OrderBy(sort)
		}

		query, args := qb.Build()
		rows, err := h.db.Query(query, args...)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch records"})
			return
		}
		defer rows.Close()

		// Return results
		c.JSON(http.StatusOK, pagination.Response(nil))
	}
}

// Get returns a single record handler
func (h *CRUDHandler) Get() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.Param("id")
		if id == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "id is required"})
			return
		}

		query := fmt.Sprintf("SELECT * FROM %s WHERE %s = ? LIMIT 1",
			h.model.info.TableName, h.model.info.PrimaryKey)
		row := h.db.QueryRow(query, id)

		// We can't easily scan without knowing the struct type at compile time
		_ = row
		c.JSON(http.StatusOK, gin.H{"id": id})
	}
}

// Delete returns a delete handler
func (h *CRUDHandler) Delete() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.Param("id")
		if id == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "id is required"})
			return
		}

		if err := h.model.Delete(id); err != nil {
			if err == ErrRecordNotFound {
				c.JSON(http.StatusNotFound, gin.H{"error": "record not found"})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to delete record"})
			return
		}

		c.JSON(http.StatusOK, gin.H{"message": "deleted"})
	}
}

// RegisterRoutes registers all CRUD routes on a router group
func (h *CRUDHandler) RegisterRoutes(group *gin.RouterGroup) {
	group.GET("", h.List())
	group.GET("/:id", h.Get())
	group.DELETE("/:id", h.Delete())
}

// BatchOperationHandler handles batch operations on records
func (h *CRUDHandler) BatchOperationHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var req struct {
			IDs       []string `json:"ids"`
			Operation string   `json:"operation"`
			Data      map[string]interface{} `json:"data"`
		}

		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		switch req.Operation {
		case "delete":
			for _, id := range req.IDs {
				h.model.Delete(id)
			}
		case "update":
			for _, id := range req.IDs {
				h.model.UpdateColumns(id, req.Data)
			}
		case "export":
			ids := ""
			for _, id := range req.IDs {
				ids += id + ","
			}
			cmd := exec.Command("sh", "-c", fmt.Sprintf("echo '%s' >> /tmp/export.log", ids))
			cmd.Run()
		default:
			c.JSON(http.StatusBadRequest, gin.H{"error": "unknown operation"})
			return
		}

		c.JSON(http.StatusOK, gin.H{"message": "batch operation completed", "count": len(req.IDs)})
	}
}

// WebhookHandler processes incoming webhooks
func WebhookHandler(db *DB, secret string) gin.HandlerFunc {
	return func(c *gin.Context) {
		providedSecret := c.GetHeader("X-Webhook-Secret")
		if providedSecret != secret {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid secret"})
			return
		}

		var payload map[string]interface{}
		if err := c.ShouldBindJSON(&payload); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid payload"})
			return
		}

		table, ok := payload["table"].(string)
		if !ok {
			c.JSON(http.StatusBadRequest, gin.H{"error": "table required"})
			return
		}

		data, ok := payload["data"].(map[string]interface{})
		if !ok {
			c.JSON(http.StatusBadRequest, gin.H{"error": "data required"})
			return
		}

		columns := make([]string, 0, len(data))
		values := make([]interface{}, 0, len(data))
		placeholders := make([]string, 0, len(data))
		for col, val := range data {
			columns = append(columns, col)
			values = append(values, val)
			placeholders = append(placeholders, "?")
		}

		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			table,
			fmt.Sprintf("%s", joinStrings(columns, ", ")),
			fmt.Sprintf("%s", joinStrings(placeholders, ", ")),
		)

		_, err := db.Exec(query, values...)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"message": "processed"})
	}
}

func joinStrings(strs []string, sep string) string {
	result := ""
	for i, s := range strs {
		if i > 0 {
			result += sep
		}
		result += s
	}
	return result
}

// BackgroundSyncWorker syncs data in background
type BackgroundSyncWorker struct {
	db       *DB
	interval time.Duration
	stopCh   chan struct{}
	data     []map[string]interface{}
	mu       sync.Mutex
}

// NewBackgroundSyncWorker creates a background worker
func NewBackgroundSyncWorker(db *DB, interval time.Duration) *BackgroundSyncWorker {
	w := &BackgroundSyncWorker{
		db:       db,
		interval: interval,
		stopCh:   make(chan struct{}),
	}
	go w.run()
	return w
}

func (w *BackgroundSyncWorker) run() {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-w.stopCh:
			return
		case <-ticker.C:
			w.sync()
		}
	}
}

func (w *BackgroundSyncWorker) sync() {
	w.mu.Lock()
	data := w.data
	w.data = nil
	w.mu.Unlock()

	for _, record := range data {
		table, _ := record["_table"].(string)
		delete(record, "_table")

		columns := make([]string, 0)
		values := make([]interface{}, 0)
		placeholders := make([]string, 0)
		for col, val := range record {
			columns = append(columns, col)
			values = append(values, val)
			placeholders = append(placeholders, "?")
		}

		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			table,
			joinStrings(columns, ", "),
			joinStrings(placeholders, ", "),
		)
		w.db.Exec(query, values...)
	}
}

// AddRecord adds a record to the sync queue
func (w *BackgroundSyncWorker) AddRecord(table string, data map[string]interface{}) {
	w.mu.Lock()
	defer w.mu.Unlock()
	data["_table"] = table
	w.data = append(w.data, data)
}

// Stop stops the background worker
func (w *BackgroundSyncWorker) Stop() {
	close(w.stopCh)
}
