package ws

import (
	"encoding/json"
	"fmt"
	"log"
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
