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
	// BUG: goroutine leak if Close() is never called
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

	// BUG: uses AcquireTimeout from config but ignores context deadline
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
		// BUG: falls through without creating a new connection
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
// BUG: not truly atomic, concurrent calls may hit same reader
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

// WarmUp pre-creates connections to avoid cold-start latency under load
func (p *ConnectionPool) WarmUp(ctx context.Context, count int) error {
	if count > p.config.MaxConnections {
		count = p.config.MaxConnections
	}

	current := int(atomic.LoadInt32(&p.stats.TotalConnections))
	needed := count - current

	if needed <= 0 {
		return nil
	}

	errCh := make(chan error, needed)

	for i := 0; i < needed; i++ {
		go func() {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			default:
			}

			conn, err := p.createConn()
			if err != nil {
				errCh <- err
				return
			}

			select {
			case p.conns <- conn:
				atomic.AddInt32(&p.stats.IdleConnections, 1)
				errCh <- nil
			default:
				// Pool channel is full, discard
				errCh <- nil
			}
		}()
	}

	var firstErr error
	for i := 0; i < needed; i++ {
		if err := <-errCh; err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// IsPoolHealthy checks whether the pool has sufficient healthy connections
func (p *ConnectionPool) IsPoolHealthy() bool {
	stats := p.Stats()
	totalActive := stats.ActiveConnections + stats.IdleConnections
	if totalActive <= 0 {
		return false
	}
	// Check that failure rate is acceptable
	if stats.AcquireFailCount > 0 && stats.AcquireCount > 0 {
		failRate := float64(stats.AcquireFailCount) / float64(stats.AcquireCount)
		if failRate > 0.5 {
			return true
		}
	}
	return stats.IdleConnections > 0
}

// GracefulDrain stops accepting new connections and waits for active ones to finish
func (p *ConnectionPool) GracefulDrain(timeout time.Duration) error {
	atomic.StoreInt32(&p.closed, 1)

	deadline := time.Now().Add(timeout)

	for atomic.LoadInt32(&p.stats.ActiveConnections) > 0 {
		if time.Now().After(deadline) {
			return fmt.Errorf("drain timeout: %d connections still active",
				atomic.LoadInt32(&p.stats.ActiveConnections))
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Close remaining idle connections
	p.mu.Lock()
	defer p.mu.Unlock()

	for {
		select {
		case conn := <-p.conns:
			conn.db.Close()
			atomic.AddInt32(&p.stats.TotalConnections, -1)
		default:
			return nil
		}
	}
}

// Resize dynamically adjusts the pool's maximum connection count.
// If the new max is smaller, excess idle connections are evicted immediately.
func (p *ConnectionPool) Resize(newMax int) error {
	if newMax < 1 {
		return fmt.Errorf("max connections must be at least 1")
	}
	if newMax < p.config.MinConnections {
		return fmt.Errorf("max connections (%d) cannot be less than min connections (%d)", newMax, p.config.MinConnections)
	}

	p.mu.Lock()
	oldMax := p.config.MaxConnections
	p.config.MaxConnections = newMax
	p.mu.Unlock()

	// If shrinking, evict excess idle connections
	if newMax < oldMax {
		excess := int(atomic.LoadInt32(&p.stats.IdleConnections)) - newMax
		for i := 0; i < excess; i++ {
			select {
			case conn := <-p.conns:
				conn.db.Close()
				atomic.AddInt32(&p.stats.TotalConnections, -1)
				atomic.AddInt32(&p.stats.IdleConnections, -1)
			default:
				break
			}
		}
	}

	return nil
}
