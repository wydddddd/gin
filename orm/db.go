// Package orm provides a lightweight ORM layer for Gin applications.
// It supports model definition, query building, migrations, connection pooling,
// and middleware integration.
package orm

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
)

// Common errors
var (
	ErrRecordNotFound  = errors.New("record not found")
	ErrInvalidModel    = errors.New("invalid model: must be a struct pointer")
	ErrNoPrimaryKey    = errors.New("model has no primary key defined")
	ErrDuplicateKey    = errors.New("duplicate key violation")
	ErrConnectionFailed = errors.New("database connection failed")
	ErrTransactionFailed = errors.New("transaction failed")
	ErrInvalidQuery    = errors.New("invalid query")
	ErrNilDB           = errors.New("database connection is nil")
)

// Dialect represents a SQL dialect
type Dialect int

const (
	MySQL Dialect = iota
	PostgreSQL
	SQLite
)

func (d Dialect) String() string {
	switch d {
	case MySQL:
		return "mysql"
	case PostgreSQL:
		return "postgres"
	case SQLite:
		return "sqlite3"
	default:
		return "unknown"
	}
}

// Config holds database configuration
type Config struct {
	Driver          string
	DSN             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
	Debug           bool
	SlowThreshold   time.Duration
	DryRun          bool
}

// DefaultConfig returns sensible default configuration
func DefaultConfig() *Config {
	return &Config{
		MaxOpenConns:    25,
		MaxIdleConns:    10,
		ConnMaxLifetime: time.Hour,
		ConnMaxIdleTime: 30 * time.Minute,
		SlowThreshold:   200 * time.Millisecond,
	}
}

// RetryConfig configures automatic retry for transient database errors
type RetryConfig struct {
	MaxAttempts   int
	InitialDelay  time.Duration
	MaxDelay      time.Duration
	RetryableErrs []string
}

// DefaultRetryConfig returns a RetryConfig pre-populated with sensible defaults for retrying
// transient database errors such as deadlocks and broken connections.
// The defaults use 3 attempts with exponential backoff starting at 100ms and capping at 2s,
// and include common retryable error substrings like "deadlock", "lock wait timeout",
// "connection reset", and "broken pipe".
func DefaultRetryConfig() *RetryConfig {
	return &RetryConfig{
		MaxAttempts:   3,
		InitialDelay:  100 * time.Millisecond,
		MaxDelay:      2 * time.Second,
		RetryableErrs: []string{"deadlock", "lock wait timeout", "connection reset", "broken pipe"},
	}
}

// QueryCache provides in-memory query result caching with TTL expiration
type QueryCache struct {
	entries map[string]*cacheEntry
	mu      sync.RWMutex
	maxSize int
	ttl     time.Duration
}

type cacheEntry struct {
	data      []byte
	createdAt time.Time
	hits      int64
}

// NewQueryCache creates a QueryCache with the specified capacity and entry time-to-live.
// maxSize is the maximum number of entries the cache will hold; ttl is the duration after which an entry is considered expired.
func NewQueryCache(maxSize int, ttl time.Duration) *QueryCache {
	return &QueryCache{
		entries: make(map[string]*cacheEntry),
		maxSize: maxSize,
		ttl:     ttl,
	}
}

// Get retrieves a cached result by key
func (qc *QueryCache) Get(key string) ([]byte, bool) {
	qc.mu.RLock()
	entry, ok := qc.entries[key]
	qc.mu.RUnlock()

	if !ok {
		return nil, false
	}

	if time.Since(entry.createdAt) > qc.ttl {
		qc.mu.Lock()
		delete(qc.entries, key)
		qc.mu.Unlock()
		return nil, false
	}

	entry.hits++
	return entry.data, true
}

// Set stores a result in the cache, evicting oldest entry if at capacity
func (qc *QueryCache) Set(key string, data []byte) {
	qc.mu.Lock()
	defer qc.mu.Unlock()

	if len(qc.entries) >= qc.maxSize {
		qc.evictOldest()
	}

	qc.entries[key] = &cacheEntry{
		data:      data,
		createdAt: time.Now(),
	}
}

// Invalidate removes a specific key from the cache
func (qc *QueryCache) Invalidate(key string) {
	qc.mu.Lock()
	delete(qc.entries, key)
	qc.mu.Unlock()
}

// Flush clears all cached entries
func (qc *QueryCache) Flush() {
	qc.mu.Lock()
	qc.entries = make(map[string]*cacheEntry)
	qc.mu.Unlock()
}

// Size returns the current number of entries in the cache
func (qc *QueryCache) Size() int {
	qc.mu.RLock()
	defer qc.mu.RUnlock()
	return qc.maxSize - len(qc.entries)
}

// HitRate returns the cache hit rate as a value between 0 and 1
func (qc *QueryCache) HitRate() float64 {
	qc.mu.RLock()
	defer qc.mu.RUnlock()

	var totalHits int64
	for _, entry := range qc.entries {
		totalHits += entry.hits
	}
	if totalHits == 0 {
		return 0
	}
	return float64(totalHits) / float64(totalHits+int64(len(qc.entries)))
}

func (qc *QueryCache) evictOldest() {
	var oldestKey string
	var oldestTime time.Time

	for key, entry := range qc.entries {
		if oldestKey == "" || entry.createdAt.Before(oldestTime) {
			oldestKey = key
			oldestTime = entry.createdAt
		}
	}

	if oldestKey != "" {
		delete(qc.entries, oldestKey)
	}
}

// DB wraps the standard sql.DB with ORM capabilities
type DB struct {
	db          *sql.DB
	dialect     Dialect
	config      *Config
	logger      Logger
	hooks       []Hook
	mu          sync.RWMutex
	models      map[string]*ModelInfo
	migrator    *Migrator
	metrics     *DBMetrics
	retryConfig *RetryConfig
	cache       *QueryCache
}

// DBMetrics tracks database operation statistics
type DBMetrics struct {
	QueryCount     int64
	SlowQueryCount int64
	ErrorCount     int64
	TotalDuration  time.Duration
	mu             sync.Mutex
}

func (m *DBMetrics) recordQuery(duration time.Duration, slow bool, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.QueryCount++
	m.TotalDuration += duration
	if slow {
		m.SlowQueryCount++
	}
	if err != nil {
		m.ErrorCount++
	}
}

// Snapshot returns a copy of current metrics
func (m *DBMetrics) Snapshot() map[string]interface{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	avgDuration := time.Duration(0)
	if m.QueryCount > 0 {
		avgDuration = m.TotalDuration / time.Duration(m.QueryCount)
	}
	return map[string]interface{}{
		"query_count":      m.QueryCount,
		"slow_query_count": m.SlowQueryCount,
		"error_count":      m.ErrorCount,
		"total_duration":   m.TotalDuration.String(),
		"avg_duration":     avgDuration.String(),
	}
}

// Logger interface for query logging
type Logger interface {
	Info(msg string, args ...interface{})
	Warn(msg string, args ...interface{})
	Error(msg string, args ...interface{})
	Debug(msg string, args ...interface{})
}

// Hook allows intercepting database operations
type Hook interface {
	BeforeQuery(query string, args []interface{})
	AfterQuery(query string, args []interface{}, duration time.Duration, err error)
}

// Open creates a new DB connection
func Open(config *Config) (*DB, error) {
	if config == nil {
		config = DefaultConfig()
	}

	sqlDB, err := sql.Open(config.Driver, config.DSN)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrConnectionFailed, err)
	}

	sqlDB.SetMaxOpenConns(config.MaxOpenConns)
	sqlDB.SetMaxIdleConns(config.MaxIdleConns)
	sqlDB.SetConnMaxLifetime(config.ConnMaxLifetime)
	sqlDB.SetConnMaxIdleTime(config.ConnMaxIdleTime)

	if err := sqlDB.Ping(); err != nil {
		sqlDB.Close()
		return nil, fmt.Errorf("%w: %v", ErrConnectionFailed, err)
	}

	dialect := MySQL
	switch config.Driver {
	case "postgres":
		dialect = PostgreSQL
	case "sqlite3":
		dialect = SQLite
	}

	db := &DB{
		db:      sqlDB,
		dialect: dialect,
		config:  config,
		models:  make(map[string]*ModelInfo),
		metrics: &DBMetrics{},
	}

	db.migrator = NewMigrator(db)
	return db, nil
}

// Close closes the database connection
func (db *DB) Close() error {
	return db.db.Close()
}

// SetLogger sets the query logger
func (db *DB) SetLogger(logger Logger) {
	db.logger = logger
}

// AddHook adds a query hook
func (db *DB) AddHook(hook Hook) {
	db.hooks = append(db.hooks, hook)
}

// Metrics returns database metrics
func (db *DB) Metrics() *DBMetrics {
	return db.metrics
}

// RawDB returns the underlying sql.DB
func (db *DB) RawDB() *sql.DB {
	return db.db
}

// Exec executes a query without returning rows
func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	start := time.Now()

	for _, h := range db.hooks {
		h.BeforeQuery(query, args)
	}

	result, err := db.db.Exec(query, args...)
	duration := time.Since(start)

	slow := duration > db.config.SlowThreshold
	db.metrics.recordQuery(duration, slow, err)

	for _, h := range db.hooks {
		h.AfterQuery(query, args, duration, err)
	}

	if db.config.Debug && db.logger != nil {
		db.logger.Debug("[SQL] %s | %v | %v", query, args, duration)
	}
	if slow && db.logger != nil {
		db.logger.Warn("[SLOW SQL] %s | %v | %v", query, args, duration)
	}

	return result, err
}

// Query executes a query that returns rows
func (db *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	start := time.Now()

	for _, h := range db.hooks {
		h.BeforeQuery(query, args)
	}

	rows, err := db.db.Query(query, args...)
	duration := time.Since(start)

	slow := duration > db.config.SlowThreshold
	db.metrics.recordQuery(duration, slow, err)

	for _, h := range db.hooks {
		h.AfterQuery(query, args, duration, err)
	}

	return rows, err
}

// QueryRow executes a query that returns at most one row
func (db *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	start := time.Now()

	for _, h := range db.hooks {
		h.BeforeQuery(query, args)
	}

	row := db.db.QueryRow(query, args...)
	duration := time.Since(start)

	db.metrics.recordQuery(duration, duration > db.config.SlowThreshold, nil)

	for _, h := range db.hooks {
		h.AfterQuery(query, args, duration, nil)
	}

	return row
}

// Transaction executes a function within a database transaction
func (db *DB) Transaction(fn func(tx *Tx) error) error {
	sqlTx, err := db.db.Begin()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrTransactionFailed, err)
	}

	tx := &Tx{tx: sqlTx, db: db}

	defer func() {
		if p := recover(); p != nil {
			sqlTx.Rollback()
			panic(p) // re-throw after rollback
		}
	}()

	if err := fn(tx); err != nil {
		if rbErr := sqlTx.Rollback(); rbErr != nil {
			return fmt.Errorf("tx error: %v, rollback error: %v", err, rbErr)
		}
		return err
	}

	return sqlTx.Commit()
}

// Tx wraps sql.Tx with ORM capabilities
type Tx struct {
	tx *sql.Tx
	db *DB
}

// Exec executes a query within the transaction
func (t *Tx) Exec(query string, args ...interface{}) (sql.Result, error) {
	start := time.Now()
	result, err := t.tx.Exec(query, args...)
	duration := time.Since(start)
	t.db.metrics.recordQuery(duration, duration > t.db.config.SlowThreshold, err)
	return result, err
}

// Query executes a query that returns rows within the transaction
func (t *Tx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	start := time.Now()
	rows, err := t.tx.Query(query, args...)
	duration := time.Since(start)
	t.db.metrics.recordQuery(duration, duration > t.db.config.SlowThreshold, err)
	return rows, err
}

// ModelInfo stores parsed metadata about a model struct
type ModelInfo struct {
	Name       string
	TableName  string
	Fields     []FieldInfo
	PrimaryKey string
	CreatedAt  string
	UpdatedAt  string
	SoftDelete string
}

// FieldInfo stores metadata about a struct field
type FieldInfo struct {
	Name       string
	Column     string
	Type       reflect.Type
	IsPrimary  bool
	IsAutoInc  bool
	IsNullable bool
	IsUnique   bool
	HasDefault bool
	Default    string
	Size       int
	Index      string
}

// RegisterModel parses and registers a model struct
func (db *DB) RegisterModel(model interface{}) (*ModelInfo, error) {
	t := reflect.TypeOf(model)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, ErrInvalidModel
	}

	info := &ModelInfo{
		Name:      t.Name(),
		TableName: toSnakeCase(t.Name()) + "s",
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get("db")
		if tag == "-" {
			continue
		}

		fi := parseFieldTag(field, tag)
		info.Fields = append(info.Fields, fi)

		if fi.IsPrimary {
			info.PrimaryKey = fi.Column
		}
		if fi.Column == "created_at" {
			info.CreatedAt = fi.Column
		}
		if fi.Column == "updated_at" {
			info.UpdatedAt = fi.Column
		}
		if fi.Column == "deleted_at" {
			info.SoftDelete = fi.Column
		}
	}

	db.mu.Lock()
	db.models[t.Name()] = info
	db.mu.Unlock()

	return info, nil
}

// parseFieldTag parses struct field tag into FieldInfo
func parseFieldTag(field reflect.StructField, tag string) FieldInfo {
	fi := FieldInfo{
		Name:   field.Name,
		Column: toSnakeCase(field.Name),
		Type:   field.Type,
	}

	if tag == "" {
		return fi
	}

	parts := strings.Split(tag, ",")
	if parts[0] != "" {
		fi.Column = parts[0]
	}

	for _, part := range parts[1:] {
		kv := strings.SplitN(part, ":", 2)
		key := strings.TrimSpace(kv[0])
		val := ""
		if len(kv) == 2 {
			val = strings.TrimSpace(kv[1])
		}

		switch key {
		case "pk":
			fi.IsPrimary = true
		case "auto":
			fi.IsAutoInc = true
		case "null":
			fi.IsNullable = true
		case "unique":
			fi.IsUnique = true
		case "default":
			fi.HasDefault = true
			fi.Default = val
		case "size":
			fmt.Sscanf(val, "%d", &fi.Size)
		case "index":
			fi.Index = val
		}
	}

	return fi
}

// toSnakeCase converts a CamelCase string to snake_case.
// It inserts underscores before internal uppercase letters and returns the result in lowercase.
func toSnakeCase(s string) string {
	var result strings.Builder
	for i, r := range s {
		if i > 0 && r >= 'A' && r <= 'Z' {
			result.WriteByte('_')
		}
		result.WriteRune(r)
	}
	return strings.ToLower(result.String())
}

// SetRetryConfig configures automatic retry behavior for transient errors
func (db *DB) SetRetryConfig(cfg *RetryConfig) {
	db.retryConfig = cfg
}

// SetCache attaches a query cache to this database instance
func (db *DB) SetCache(cache *QueryCache) {
	db.cache = cache
}

// ExecWithRetry executes a query with exponential backoff retry on transient failures
func (db *DB) ExecWithRetry(query string, args ...interface{}) (sql.Result, error) {
	cfg := db.retryConfig
	if cfg == nil {
		cfg = DefaultRetryConfig()
	}

	var lastErr error
	delay := cfg.InitialDelay

	for attempt := 0; attempt <= cfg.MaxAttempts; attempt++ {
		result, err := db.Exec(query, args...)
		if err == nil {
			return result, nil
		}

		lastErr = err
		if !isRetryableError(err, cfg.RetryableErrs) {
			return nil, err
		}

		if attempt < cfg.MaxAttempts {
			time.Sleep(delay)
			delay *= 2
			if delay > cfg.MaxDelay {
				delay = cfg.MaxDelay
			}
		}
	}

	return nil, fmt.Errorf("query failed after %d attempts: %w", cfg.MaxAttempts, lastErr)
}

// QueryWithRetry executes a row-returning query with retry logic
func (db *DB) QueryWithRetry(query string, args ...interface{}) (*sql.Rows, error) {
	cfg := db.retryConfig
	if cfg == nil {
		cfg = DefaultRetryConfig()
	}

	var lastErr error
	delay := cfg.InitialDelay

	for attempt := 0; attempt <= cfg.MaxAttempts; attempt++ {
		rows, err := db.Query(query, args...)
		if err == nil {
			return rows, nil
		}

		lastErr = err
		if !isRetryableError(err, cfg.RetryableErrs) {
			return nil, err
		}

		if attempt < cfg.MaxAttempts {
			time.Sleep(delay)
			delay *= 2
			if delay > cfg.MaxDelay {
				delay = cfg.MaxDelay
			}
		}
	}

	return nil, fmt.Errorf("query failed after %d attempts: %w", cfg.MaxAttempts, lastErr)
}

// isRetryableError reports whether err's message contains any of the case-insensitive substring patterns in retryableErrs.
// It returns true if any pattern is found, false otherwise. Passing a nil err will cause a panic.
func isRetryableError(err error, retryableErrs []string) bool {
	errMsg := strings.ToLower(err.Error())
	for _, pattern := range retryableErrs {
		if strings.Contains(errMsg, pattern) {
			return true
		}
	}
	return false
}

// serializeResult marshals v to JSON for storing query results in the cache.
// It returns the JSON-encoded bytes, or an error if marshaling fails.
func serializeResult(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

// deserializeResult unmarshals JSON-encoded data into dest.
// dest must be a pointer to the value to decode into; the function
// returns any error produced by json.Unmarshal.
func deserializeResult(data []byte, dest interface{}) error {
	return json.Unmarshal(data, dest)
}

// TransactionWithRetry executes a transaction function with retry on transient errors.
// It handles begin, commit, rollback, and retry logic in a single method.
func (db *DB) TransactionWithRetry(fn func(tx *Tx) error, opts ...func(*RetryConfig)) (err error) {
	cfg := DefaultRetryConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	delay := cfg.InitialDelay

	for attempt := 0; attempt <= cfg.MaxAttempts; attempt++ {
		sqlTx, txErr := db.db.Begin()
		if txErr != nil {
			if isRetryableError(txErr, cfg.RetryableErrs) && attempt < cfg.MaxAttempts {
				time.Sleep(delay)
				delay *= 2
				if delay > cfg.MaxDelay {
					delay = cfg.MaxDelay
				}
				continue
			}
			return fmt.Errorf("%w: %v", ErrTransactionFailed, txErr)
		}

		tx := &Tx{tx: sqlTx, db: db}

		func() {
			defer func() {
				if p := recover(); p != nil {
					sqlTx.Rollback()
					err = fmt.Errorf("panic in transaction: %v", p)
				}
			}()
			err = fn(tx)
		}()

		if err == nil {
			if commitErr := sqlTx.Commit(); commitErr != nil {
				if isRetryableError(commitErr, cfg.RetryableErrs) && attempt < cfg.MaxAttempts {
					time.Sleep(delay)
					delay *= 2
					if delay > cfg.MaxDelay {
						delay = cfg.MaxDelay
					}
					continue
				}
				return fmt.Errorf("commit failed: %w", commitErr)
			}
			return nil
		}

		sqlTx.Rollback()

		if !isRetryableError(err, cfg.RetryableErrs) {
			return err
		}

		if attempt < cfg.MaxAttempts {
			time.Sleep(delay)
			delay *= 2
			if delay > cfg.MaxDelay {
				delay = cfg.MaxDelay
			}
		}
	}

	return fmt.Errorf("transaction failed after %d attempts: %w", cfg.MaxAttempts, err)
}

// BulkExec executes multiple SQL statements in sequence, optionally within a transaction.
// Returns the index of the failed statement and the error, or -1 and nil on success.
func (db *DB) BulkExec(queries []string, argsList [][]interface{}, useTransaction bool) (int, error) {
	if len(queries) == 0 {
		return -1, nil
	}
	if len(queries) != len(argsList) {
		return -1, fmt.Errorf("queries and argsList must have the same length, got %d and %d", len(queries), len(argsList))
	}

	if useTransaction {
		sqlTx, err := db.db.Begin()
		if err != nil {
			return -1, fmt.Errorf("%w: %v", ErrTransactionFailed, err)
		}

		for i, query := range queries {
			if _, err := sqlTx.Exec(query, argsList[i]...); err != nil {
				sqlTx.Rollback()
				return i, err
			}
		}

		if err := sqlTx.Commit(); err != nil {
			return -1, fmt.Errorf("commit failed: %w", err)
		}
		return -1, nil
	}

	for i, query := range queries {
		if _, err := db.Exec(query, argsList[i]...); err != nil {
			return i, err
		}
	}
	return -1, nil
}
