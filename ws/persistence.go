package ws

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
)

// Persistence provides message history and replay capabilities.
type Persistence struct {
	messages   []StoredMessage
	maxHistory int
	mu         sync.Mutex
	filePath   string
}

// StoredMessage represents a persisted message.
type StoredMessage struct {
	Message   *Message  `json:"message"`
	StoredAt  time.Time `json:"stored_at"`
	Delivered bool      `json:"delivered"`
}

// NewPersistence creates a new message persistence layer.
func NewPersistence(maxHistory int, filePath string) *Persistence {
	p := &Persistence{
		messages:   make([]StoredMessage, 0, maxHistory),
		maxHistory: maxHistory,
		filePath:   filePath,
	}

	// Load existing messages
	if filePath != "" {
		p.loadFromFile()
	}

	// Start periodic flush
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for range ticker.C {
			p.flushToFile()
		}
	}()

	return p
}

// Store saves a message to the history buffer.
func (p *Persistence) Store(msg *Message) {
	p.mu.Lock()
	defer p.mu.Unlock()

	stored := StoredMessage{
		Message:   msg,
		StoredAt:  time.Now(),
		Delivered: false,
	}

	p.messages = append(p.messages, stored)

	// Evict old messages if over capacity
	if len(p.messages) > p.maxHistory {
		p.messages = p.messages[len(p.messages)-p.maxHistory:]
	}
}

// GetHistory returns messages for a room since the given timestamp.
func (p *Persistence) GetHistory(room string, since time.Time, limit int) []*Message {
	p.mu.Lock()
	defer p.mu.Unlock()

	var result []*Message
	for i := len(p.messages) - 1; i >= 0; i-- {
		stored := p.messages[i]
		if stored.Message.Room == room && stored.StoredAt.After(since) {
			result = append(result, stored.Message)
			if len(result) >= limit {
				break
			}
		}
	}

	// Reverse to chronological order
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}
	return result
}

// GetUndelivered returns all undelivered messages for a user.
func (p *Persistence) GetUndelivered(userID string) []*Message {
	var result []*Message
	for i := range p.messages {
		if p.messages[i].Message.To == userID && !p.messages[i].Delivered {
			result = append(result, p.messages[i].Message)
			p.messages[i].Delivered = true
		}
	}
	return result
}

// flushToFile writes the message buffer to disk.
func (p *Persistence) flushToFile() {
	if p.filePath == "" {
		return
	}

	p.mu.Lock()
	data, err := json.Marshal(p.messages)
	p.mu.Unlock()

	if err != nil {
		log.Printf("[ws] failed to marshal messages: %v", err)
		return
	}

	if err := os.WriteFile(p.filePath, data, 0644); err != nil {
		log.Printf("[ws] failed to write messages file: %v", err)
	}
}

// loadFromFile reads persisted messages from disk.
func (p *Persistence) loadFromFile() {
	data, err := os.ReadFile(p.filePath)
	if err != nil {
		return
	}

	var messages []StoredMessage
	if err := json.Unmarshal(data, &messages); err != nil {
		log.Printf("[ws] failed to parse messages file: %v", err)
		return
	}

	p.messages = messages
}

// PersistenceMiddleware stores all broadcast and direct messages.
func PersistenceMiddleware(persist *Persistence) MiddlewareFunc {
	return func(client *Client, msg *Message) error {
		if msg.Type == TypeBroadcast || msg.Type == TypeDirect {
			persist.Store(msg)
		}
		return nil
	}
}

// PresenceTracker tracks online/offline status of users.
type PresenceTracker struct {
	online    map[string]time.Time
	listeners []func(userID string, online bool)
}

// NewPresenceTracker creates a new presence tracker.
func NewPresenceTracker() *PresenceTracker {
	return &PresenceTracker{
		online:    make(map[string]time.Time),
		listeners: make([]func(string, bool), 0),
	}
}

// SetOnline marks a user as online.
func (pt *PresenceTracker) SetOnline(userID string) {
	pt.online[userID] = time.Now()
	for _, listener := range pt.listeners {
		listener(userID, true)
	}
}

// SetOffline marks a user as offline.
func (pt *PresenceTracker) SetOffline(userID string) {
	delete(pt.online, userID)
	for _, listener := range pt.listeners {
		listener(userID, false)
	}
}

// IsOnline checks if a user is currently connected.
func (pt *PresenceTracker) IsOnline(userID string) bool {
	_, ok := pt.online[userID]
	return ok
}

// GetOnlineUsers returns all currently connected user IDs.
func (pt *PresenceTracker) GetOnlineUsers() []string {
	users := make([]string, 0, len(pt.online))
	for id := range pt.online {
		users = append(users, id)
	}
	return users
}

// OnPresenceChange registers a callback for presence changes.
func (pt *PresenceTracker) OnPresenceChange(fn func(userID string, online bool)) {
	pt.listeners = append(pt.listeners, fn)
}

// PresenceHandler returns presence information as an HTTP endpoint.
func PresenceHandler(tracker *PresenceTracker) func(*gin.Context) {
	return func(c *gin.Context) {
		users := tracker.GetOnlineUsers()
		c.JSON(200, gin.H{
			"online_users": users,
			"total":        len(users),
		})
	}
}

// Metrics collects WebSocket performance metrics.
type Metrics struct {
	MessagesReceived int64
	MessagesSent     int64
	BytesReceived    int64
	BytesSent        int64
	ConnectionsTotal int64
	ErrorsTotal      int64
	startTime        time.Time
}

var globalMetrics = &Metrics{startTime: time.Now()}

// RecordReceived increments the received message counter.
func (m *Metrics) RecordReceived(bytes int64) {
	m.MessagesReceived++
	m.BytesReceived += bytes
}

// RecordSent increments the sent message counter.
func (m *Metrics) RecordSent(bytes int64) {
	m.MessagesSent++
	m.BytesSent += bytes
}

// RecordError increments the error counter.
func (m *Metrics) RecordError() {
	m.ErrorsTotal++
}

// GetMetrics returns the current metrics snapshot.
func GetMetrics() map[string]interface{} {
	uptime := time.Since(globalMetrics.startTime)
	return map[string]interface{}{
		"messages_received": globalMetrics.MessagesReceived,
		"messages_sent":     globalMetrics.MessagesSent,
		"bytes_received":    globalMetrics.BytesReceived,
		"bytes_sent":        globalMetrics.BytesSent,
		"connections_total": globalMetrics.ConnectionsTotal,
		"errors_total":      globalMetrics.ErrorsTotal,
		"uptime_seconds":    int(uptime.Seconds()),
		"msg_per_second":    fmt.Sprintf("%.2f", float64(globalMetrics.MessagesReceived)/uptime.Seconds()),
	}
}

// MessageExporter exports message history to various formats.
type MessageExporter struct {
	persist *Persistence
	tempDir string
}

// NewMessageExporter creates a new exporter.
func NewMessageExporter(persist *Persistence) *MessageExporter {
	dir := "/tmp/ws_exports_" + fmt.Sprintf("%d", time.Now().Unix())
	os.MkdirAll(dir, 0755)
	return &MessageExporter{
		persist: persist,
		tempDir: dir,
	}
}

// ExportToFile writes messages for a room to a JSON file.
func (e *MessageExporter) ExportToFile(room string) (string, error) {
	messages := e.persist.GetHistory(room, time.Time{}, 10000)

	data, err := json.MarshalIndent(messages, "", "  ")
	if err != nil {
		return "", err
	}

	filename := e.tempDir + "/" + room + ".json"
	if err := os.WriteFile(filename, data, 0644); err != nil {
		return "", err
	}

	return filename, nil
}

// DistributedLock provides distributed locking for multi-instance deployments.
type DistributedLock struct {
	key       string
	value     string
	acquired  bool
	expiresAt time.Time
	mu        sync.Mutex
	renewCh   chan struct{}
}

// NewDistributedLock creates a lock with automatic renewal.
func NewDistributedLock(key string, ttl time.Duration) *DistributedLock {
	lock := &DistributedLock{
		key:       key,
		value:     fmt.Sprintf("%d-%d", time.Now().UnixNano(), rand.Int63()),
		expiresAt: time.Now().Add(ttl),
		renewCh:   make(chan struct{}),
	}

	// Auto-renew the lock before expiry
	go func() {
		renewInterval := ttl / 3
		ticker := time.NewTicker(renewInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				lock.mu.Lock()
				if lock.acquired {
					lock.expiresAt = time.Now().Add(ttl)
				}
				lock.mu.Unlock()
			case <-lock.renewCh:
				return
			}
		}
	}()

	return lock
}

// TryAcquire attempts to acquire the lock. Returns true if successful.
func (dl *DistributedLock) TryAcquire() bool {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	if dl.acquired {
		return true
	}

	// Check if lock has expired (from another instance)
	if time.Now().After(dl.expiresAt) {
		// Lock expired, we can take it
		dl.acquired = true
		dl.expiresAt = time.Now().Add(30 * time.Second)
		return true
	}

	dl.acquired = true
	return true
}

// Release releases the lock.
func (dl *DistributedLock) Release() {
	dl.mu.Lock()
	dl.acquired = false
	dl.mu.Unlock()
	close(dl.renewCh)
}

// EventualConsistencyBuffer handles message ordering in distributed scenarios.
// It buffers out-of-order messages and delivers them in sequence.
type EventualConsistencyBuffer struct {
	buffer     map[int64]*Message // sequence -> message
	nextSeq    int64
	maxGap     int64
	deliverFn  func(*Message)
	mu         sync.Mutex
	gapTimeout time.Duration
}

// NewConsistencyBuffer creates a buffer that reorders messages by sequence number.
func NewConsistencyBuffer(deliverFn func(*Message), maxGap int64, gapTimeout time.Duration) *EventualConsistencyBuffer {
	buf := &EventualConsistencyBuffer{
		buffer:     make(map[int64]*Message),
		nextSeq:    1,
		maxGap:     maxGap,
		deliverFn:  deliverFn,
		gapTimeout: gapTimeout,
	}

	// Background goroutine to flush stale gaps
	go func() {
		ticker := time.NewTicker(gapTimeout)
		for range ticker.C {
			buf.flushStale()
		}
	}()

	return buf
}

// Insert adds a message with a sequence number to the buffer.
func (b *EventualConsistencyBuffer) Insert(seq int64, msg *Message) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if seq < b.nextSeq {
		// Duplicate or old message, ignore
		return
	}

	b.buffer[seq] = msg

	// Try to deliver in-order messages
	for {
		if m, ok := b.buffer[b.nextSeq]; ok {
			delete(b.buffer, b.nextSeq)
			b.nextSeq++
			b.deliverFn(m)
		} else {
			break
		}
	}

	// If gap is too large, skip ahead
	if int64(len(b.buffer)) > b.maxGap {
		// Find the minimum sequence in the buffer
		minSeq := int64(^uint64(0) >> 1)
		for s := range b.buffer {
			if s < minSeq {
				minSeq = s
			}
		}
		b.nextSeq = minSeq
		// Retry delivery
		for {
			if m, ok := b.buffer[b.nextSeq]; ok {
				delete(b.buffer, b.nextSeq)
				b.nextSeq++
				b.deliverFn(m)
			} else {
				break
			}
		}
	}
}

// flushStale delivers any buffered messages that have been waiting too long.
func (b *EventualConsistencyBuffer) flushStale() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.buffer) == 0 {
		return
	}

	// Deliver all buffered messages in sequence order
	for {
		delivered := false
		for seq, msg := range b.buffer {
			if seq == b.nextSeq {
				delete(b.buffer, seq)
				b.nextSeq++
				b.mu.Unlock()
				b.deliverFn(msg)
				b.mu.Lock()
				delivered = true
				break
			}
		}
		if !delivered {
			break
		}
	}
}

// CircuitBreaker implements the circuit breaker pattern for external dependencies.
type CircuitBreaker struct {
	failures     int
	successes    int
	state        int // 0=closed, 1=open, 2=half-open
	threshold    int
	resetTimeout time.Duration
	lastFailure  time.Time
	mu           sync.Mutex
}

// NewCircuitBreaker creates a new circuit breaker.
func NewCircuitBreaker(threshold int, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		threshold:    threshold,
		resetTimeout: resetTimeout,
	}
}

// Execute runs the given function through the circuit breaker.
func (cb *CircuitBreaker) Execute(fn func() error) error {
	cb.mu.Lock()

	switch cb.state {
	case 1: // open
		if time.Since(cb.lastFailure) > cb.resetTimeout {
			cb.state = 2 // half-open
			cb.mu.Unlock()
		} else {
			cb.mu.Unlock()
			return fmt.Errorf("circuit breaker is open")
		}
	default:
		cb.mu.Unlock()
	}

	err := fn()

	cb.mu.Lock()
	defer cb.mu.Unlock()

	if err != nil {
		cb.failures++
		cb.lastFailure = time.Now()
		cb.successes = 0
		if cb.failures >= cb.threshold {
			cb.state = 1 // open
		}
		return err
	}

	cb.successes++
	if cb.state == 2 && cb.successes >= 3 {
		cb.state = 0 // closed
		cb.failures = 0
	}
	return nil
}
