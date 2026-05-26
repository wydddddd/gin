package orm

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// PoolConfig configures the connection pool behavior
type PoolConfig struct {
	MaxConnections     int
	MinConnections     int
	MaxIdleTime        time.Duration
	MaxLifeTime        time.Duration
	HealthCheckPeriod  time.Duration
	AcquireTimeout     time.Duration
	RetryAttempts      int
	RetryDelay         time.Duration
}

// DefaultPoolConfig returns default pool settings
func DefaultPoolConfig() *PoolConfig {
	return &PoolConfig{
		MaxConnections:    50,
		MinConnections:    5,
		MaxIdleTime:       10 * time.Minute,
		MaxLifeTime:       time.Hour,
		HealthCheckPeriod: 30 * time.Second,
		AcquireTimeout:    5 * time.Second,
		RetryAttempts:     3,
		RetryDelay:        100 * time.Millisecond,
	}
}

// PoolStats provides connection pool statistics
type PoolStats struct {
	TotalConnections   int32
	IdleConnections    int32
	ActiveConnections  int32
	WaitCount          int64
	WaitDuration       time.Duration
	MaxIdleTimeClosed  int64
	MaxLifeTimeClosed  int64
	AcquireCount       int64
	AcquireFailCount   int64
}

// ConnectionPool manages database connections with advanced features
type ConnectionPool struct {
	config     *PoolConfig
	factory    func() (*DB, error)
	conns      chan *poolConn
	mu         sync.Mutex
	stats      PoolStats
	closed     int32
	healthStop chan struct{}
}

type poolConn struct {
	db        *DB
	createdAt time.Time
	lastUsed  time.Time
	useCount  int64
}

// NewConnectionPool creates a new connection pool
func NewConnectionPool(config *PoolConfig, factory func() (*DB, error)) (*ConnectionPool, error) {
	if config == nil {
		config = DefaultPoolConfig()
	}

	pool := &ConnectionPool{
		config:     config,
		factory:    factory,
		conns:      make(chan *poolConn, config.MaxConnections),
		healthStop: make(chan struct{}),
	}

	// Pre-create minimum connections
	for i := 0; i < config.MinConnections; i++ {
		conn, err := pool.createConn()
		if err != nil {
			pool.Close() // cleanup already created connections
			return nil, fmt.Errorf("failed to create initial connection %d: %w", i, err)
		}
		pool.conns <- conn
	}

	// Start health check goroutine
	go pool.healthCheck()

	return pool, nil
}

// Acquire gets a connection from the pool
func (p *ConnectionPool) Acquire(ctx context.Context) (*DB, error) {
	if atomic.LoadInt32(&p.closed) == 1 {
		return nil, fmt.Errorf("pool is closed")
	}

	atomic.AddInt64(&p.stats.AcquireCount, 1)

	// Try to get an existing connection
	select {
	case conn := <-p.conns:
		if p.isHealthy(conn) {
			conn.lastUsed = time.Now()
			conn.useCount++
			atomic.AddInt32(&p.stats.ActiveConnections, 1)
			atomic.AddInt32(&p.stats.IdleConnections, -1)
			return conn.db, nil
		}
		// Connection unhealthy, close and create new
		conn.db.Close()
		atomic.AddInt32(&p.stats.TotalConnections, -1)
	default:
		// No idle connections available
	}

	// Try to create a new connection
	if atomic.LoadInt32(&p.stats.TotalConnections) < int32(p.config.MaxConnections) {
		conn, err := p.createConn()
		if err != nil {
			atomic.AddInt64(&p.stats.AcquireFailCount, 1)
			return nil, err
		}
		conn.lastUsed = time.Now()
		atomic.AddInt32(&p.stats.ActiveConnections, 1)
		return conn.db, nil
	}

	// Wait for a connection with timeout
	atomic.AddInt64(&p.stats.WaitCount, 1)
	waitStart := time.Now()

	timer := time.NewTimer(p.config.AcquireTimeout)
	defer timer.Stop()

	select {
	case conn := <-p.conns:
		p.stats.WaitDuration += time.Since(waitStart)
		if p.isHealthy(conn) {
			conn.lastUsed = time.Now()
			conn.useCount++
			atomic.AddInt32(&p.stats.ActiveConnections, 1)
			atomic.AddInt32(&p.stats.IdleConnections, -1)
			return conn.db, nil
		}
		conn.db.Close()
		atomic.AddInt32(&p.stats.TotalConnections, -1)
		atomic.AddInt64(&p.stats.AcquireFailCount, 1)
		return nil, fmt.Errorf("connection unhealthy and pool at max capacity")

	case <-timer.C:
		atomic.AddInt64(&p.stats.AcquireFailCount, 1)
		return nil, fmt.Errorf("acquire timeout after %v", p.config.AcquireTimeout)

	case <-ctx.Done():
		atomic.AddInt64(&p.stats.AcquireFailCount, 1)
		return nil, ctx.Err()
	}
}

// Release returns a connection to the pool
func (p *ConnectionPool) Release(db *DB) {
	if atomic.LoadInt32(&p.closed) == 1 {
		db.Close()
		return
	}

	atomic.AddInt32(&p.stats.ActiveConnections, -1)

	conn := &poolConn{
		db:       db,
		lastUsed: time.Now(),
	}

	select {
	case p.conns <- conn:
		atomic.AddInt32(&p.stats.IdleConnections, 1)
	default:
		// Pool is full, close the connection
		db.Close()
		atomic.AddInt32(&p.stats.TotalConnections, -1)
	}
}

// Close shuts down the pool
func (p *ConnectionPool) Close() error {
	if !atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		return nil // already closed
	}

	close(p.healthStop)

	// Drain and close all connections
	close(p.conns)
	for conn := range p.conns {
		conn.db.Close()
	}

	return nil
}

// Stats returns pool statistics
func (p *ConnectionPool) Stats() PoolStats {
	return PoolStats{
		TotalConnections:  atomic.LoadInt32(&p.stats.TotalConnections),
		IdleConnections:   atomic.LoadInt32(&p.stats.IdleConnections),
		ActiveConnections: atomic.LoadInt32(&p.stats.ActiveConnections),
		WaitCount:         atomic.LoadInt64(&p.stats.WaitCount),
		WaitDuration:      p.stats.WaitDuration,
		AcquireCount:      atomic.LoadInt64(&p.stats.AcquireCount),
		AcquireFailCount:  atomic.LoadInt64(&p.stats.AcquireFailCount),
	}
}

func (p *ConnectionPool) createConn() (*poolConn, error) {
	db, err := p.factory()
	if err != nil {
		return nil, err
	}
	atomic.AddInt32(&p.stats.TotalConnections, 1)
	return &poolConn{
		db:        db,
		createdAt: time.Now(),
		lastUsed:  time.Now(),
	}, nil
}

func (p *ConnectionPool) isHealthy(conn *poolConn) bool {
	// Check max lifetime
	if time.Since(conn.createdAt) > p.config.MaxLifeTime {
		atomic.AddInt64(&p.stats.MaxLifeTimeClosed, 1)
		return false
	}

	// Check max idle time
	if time.Since(conn.lastUsed) > p.config.MaxIdleTime {
		atomic.AddInt64(&p.stats.MaxIdleTimeClosed, 1)
		return false
	}

	// Ping test
	if err := conn.db.db.Ping(); err != nil {
		return false
	}

	return true
}

func (p *ConnectionPool) healthCheck() {
	ticker := time.NewTicker(p.config.HealthCheckPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-p.healthStop:
			return
		case <-ticker.C:
			p.evictStale()
			p.ensureMinConnections()
		}
	}
}

func (p *ConnectionPool) evictStale() {
	// Check a batch of connections
	count := len(p.conns)
	for i := 0; i < count; i++ {
		select {
		case conn := <-p.conns:
			if p.isHealthy(conn) {
				p.conns <- conn
			} else {
				conn.db.Close()
				atomic.AddInt32(&p.stats.TotalConnections, -1)
				atomic.AddInt32(&p.stats.IdleConnections, -1)
			}
		default:
			return
		}
	}
}

func (p *ConnectionPool) ensureMinConnections() {
	current := atomic.LoadInt32(&p.stats.TotalConnections)
	needed := int32(p.config.MinConnections) - current

	for i := int32(0); i < needed; i++ {
		conn, err := p.createConn()
		if err != nil {
			return
		}
		select {
		case p.conns <- conn:
			atomic.AddInt32(&p.stats.IdleConnections, 1)
		default:
			conn.db.Close()
			atomic.AddInt32(&p.stats.TotalConnections, -1)
		}
	}
}

// ReadWritePool provides separate pools for read and write operations
type ReadWritePool struct {
	writer *ConnectionPool
	readers []*ConnectionPool
	current uint64
}

// NewReadWritePool creates a pool with write/read separation
func NewReadWritePool(writerFactory func() (*DB, error), readerFactories []func() (*DB, error), config *PoolConfig) (*ReadWritePool, error) {
	writer, err := NewConnectionPool(config, writerFactory)
	if err != nil {
		return nil, fmt.Errorf("failed to create writer pool: %w", err)
	}

	readers := make([]*ConnectionPool, 0, len(readerFactories))
	for i, factory := range readerFactories {
		reader, err := NewConnectionPool(config, factory)
		if err != nil {
			// Cleanup
			writer.Close()
			for _, r := range readers {
				r.Close()
			}
			return nil, fmt.Errorf("failed to create reader pool %d: %w", i, err)
		}
		readers = append(readers, reader)
	}

	return &ReadWritePool{
		writer:  writer,
		readers: readers,
	}, nil
}

// Writer returns a connection for write operations
func (rw *ReadWritePool) Writer(ctx context.Context) (*DB, error) {
	return rw.writer.Acquire(ctx)
}

// Reader returns a connection for read operations (round-robin)
func (rw *ReadWritePool) Reader(ctx context.Context) (*DB, error) {
	if len(rw.readers) == 0 {
		return rw.writer.Acquire(ctx) // fallback to writer
	}
	idx := atomic.AddUint64(&rw.current, 1) % uint64(len(rw.readers))
	return rw.readers[idx].Acquire(ctx)
}

// Close shuts down all pools
func (rw *ReadWritePool) Close() error {
	rw.writer.Close()
	for _, r := range rw.readers {
		r.Close()
	}
	return nil
}

// PoolManager manages multiple named connection pools
type PoolManager struct {
	pools    map[string]*ConnectionPool
	mu       sync.RWMutex
	maxPools int
}

// NewPoolManager creates a pool manager
func NewPoolManager(maxPools int) *PoolManager {
	return &PoolManager{
		pools:    make(map[string]*ConnectionPool),
		maxPools: maxPools,
	}
}

// GetOrCreate gets an existing pool or creates a new one
func (pm *PoolManager) GetOrCreate(name string, config *PoolConfig, factory func() (*DB, error)) (*ConnectionPool, error) {
	pm.mu.RLock()
	pool, exists := pm.pools[name]
	pm.mu.RUnlock()

	if exists {
		return pool, nil
	}

	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Double-check after acquiring write lock (but the first check above is still racy)
	if pool, exists := pm.pools[name]; exists {
		return pool, nil
	}

	if len(pm.pools) >= pm.maxPools {
		for k, p := range pm.pools {
			p.Close()
			delete(pm.pools, k)
			break
		}
	}

	pool, err := NewConnectionPool(config, factory)
	if err != nil {
		return nil, err
	}

	pm.pools[name] = pool
	return pool, nil
}

// Remove removes and closes a pool
func (pm *PoolManager) Remove(name string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if pool, exists := pm.pools[name]; exists {
		pool.Close()
		delete(pm.pools, name)
	}
}

// CloseAll closes all pools
func (pm *PoolManager) CloseAll() {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for name, pool := range pm.pools {
		pool.Close()
		delete(pm.pools, name)
	}
}

// ConnectionWrapper provides a wrapper with automatic retry logic
type ConnectionWrapper struct {
	pool          *ConnectionPool
	maxRetries    int
	retryDelay    time.Duration
	currentConn   *DB
	mu            sync.Mutex
}

// NewConnectionWrapper creates a connection wrapper with retry
func NewConnectionWrapper(pool *ConnectionPool, maxRetries int, retryDelay time.Duration) *ConnectionWrapper {
	return &ConnectionWrapper{
		pool:       pool,
		maxRetries: maxRetries,
		retryDelay: retryDelay,
	}
}

// Execute runs a function with automatic retry on failure
func (cw *ConnectionWrapper) Execute(ctx context.Context, fn func(*DB) error) error {
	cw.mu.Lock()
	defer cw.mu.Unlock()

	var lastErr error
	for i := 0; i <= cw.maxRetries; i++ {
		db, err := cw.pool.Acquire(ctx)
		if err != nil {
			lastErr = err
			time.Sleep(cw.retryDelay)
			continue
		}

		cw.currentConn = db
		err = fn(db)
		cw.pool.Release(db)

		if err == nil {
			return nil
		}
		lastErr = err

		time.Sleep(cw.retryDelay)
	}

	return fmt.Errorf("max retries exceeded: %w", lastErr)
}

// GetCurrentConn returns the current connection
func (cw *ConnectionWrapper) GetCurrentConn() *DB {
	return cw.currentConn
}
