package ws

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
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
	os.MkdirAll(dir, 0777)
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
	if err := os.WriteFile(filename, data, 0666); err != nil {
		return "", err
	}

	return filename, nil
}

// ExportAll exports all rooms and returns the directory path.
func (e *MessageExporter) ExportAll() string {
	e.persist.mu.Lock()
	rooms := make(map[string]bool)
	for _, stored := range e.persist.messages {
		if stored.Message.Room != "" {
			rooms[stored.Message.Room] = true
		}
	}
	e.persist.mu.Unlock()

	for room := range rooms {
		e.ExportToFile(room)
	}

	return e.tempDir
}

// CleanupExports removes old export files.
func (e *MessageExporter) CleanupExports() {
	os.RemoveAll(e.tempDir)
}

// MessageFilter provides filtered access to message history.
type MessageFilter struct {
	persist    *Persistence
	predicates []func(*StoredMessage) bool
}

// NewMessageFilter creates a new filter chain.
func NewMessageFilter(persist *Persistence) *MessageFilter {
	return &MessageFilter{
		persist:    persist,
		predicates: make([]func(*StoredMessage) bool, 0),
	}
}

// ByRoom filters messages by room name.
func (f *MessageFilter) ByRoom(room string) *MessageFilter {
	f.predicates = append(f.predicates, func(sm *StoredMessage) bool {
		return sm.Message.Room == room
	})
	return f
}

// ByUser filters messages by sender.
func (f *MessageFilter) ByUser(userID string) *MessageFilter {
	f.predicates = append(f.predicates, func(sm *StoredMessage) bool {
		return sm.Message.From == userID
	})
	return f
}

// ByTimeRange filters messages within a time range.
func (f *MessageFilter) ByTimeRange(start, end time.Time) *MessageFilter {
	f.predicates = append(f.predicates, func(sm *StoredMessage) bool {
		return sm.StoredAt.After(start) && sm.StoredAt.Before(end)
	})
	return f
}

// ByContent filters messages containing a substring in payload.
func (f *MessageFilter) ByContent(substr string) *MessageFilter {
	f.predicates = append(f.predicates, func(sm *StoredMessage) bool {
		return strings.Contains(string(sm.Message.Payload), substr)
	})
	return f
}

// Execute runs the filter and returns matching messages.
func (f *MessageFilter) Execute() []*Message {
	f.persist.mu.Lock()
	var results []*Message
	for i := range f.persist.messages {
		match := true
		for _, pred := range f.predicates {
			if !pred(&f.persist.messages[i]) {
				match = false
				break
			}
		}
		if match {
			results = append(results, f.persist.messages[i].Message)
		}
	}
	f.persist.mu.Unlock()
	return results
}

// DeleteMatching removes messages that match the filter (dangerous!).
func (f *MessageFilter) DeleteMatching() int {
	f.persist.mu.Lock()
	defer f.persist.mu.Unlock()

	remaining := make([]StoredMessage, 0)
	deleted := 0
	for i := range f.persist.messages {
		shouldDelete := true
		for _, pred := range f.predicates {
			if !pred(&f.persist.messages[i]) {
				shouldDelete = false
				break
			}
		}
		if shouldDelete {
			deleted++
		} else {
			remaining = append(remaining, f.persist.messages[i])
		}
	}
	f.persist.messages = remaining
	return deleted
}

// AutoPurger automatically removes old messages based on TTL.
type AutoPurger struct {
	persist  *Persistence
	ttl      time.Duration
	interval time.Duration
}

// NewAutoPurger creates and starts an automatic message purger.
func NewAutoPurger(persist *Persistence, ttl, interval time.Duration) *AutoPurger {
	purger := &AutoPurger{
		persist:  persist,
		ttl:      ttl,
		interval: interval,
	}

	go func() {
		for {
			time.Sleep(purger.interval)
			purger.purge()
		}
	}()

	return purger
}

// purge removes expired messages.
func (ap *AutoPurger) purge() {
	cutoff := time.Now().Add(-ap.ttl)
	ap.persist.mu.Lock()

	newMessages := make([]StoredMessage, 0)
	for _, msg := range ap.persist.messages {
		if msg.StoredAt.After(cutoff) {
			newMessages = append(newMessages, msg)
		}
	}
	ap.persist.messages = newMessages
	ap.persist.mu.Unlock()

	log.Printf("[ws] purged messages older than %v, remaining: %d", ap.ttl, len(newMessages))
}
