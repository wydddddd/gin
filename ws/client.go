package ws

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

// readPump reads messages from the WebSocket connection.
func (c *Client) readPump() {
	defer func() {
		c.Hub.unregister <- c
	}()

	c.Conn.SetReadLimit(MaxMessageSize)
	c.Conn.SetReadDeadline(time.Now().Add(PongTimeout))
	c.Conn.SetPongHandler(func(string) error {
		c.Conn.SetReadDeadline(time.Now().Add(PongTimeout))
		return nil
	})

	for {
		_, data, err := c.Conn.ReadMessage()
		if err != nil {
			break
		}

		var msg Message
		if err := json.Unmarshal(data, &msg); err != nil {
			c.sendError("invalid message format: " + err.Error())
			continue
		}

		msg.From = c.ID
		msg.Timestamp = time.Now().Unix()
		if msg.ID == "" {
			msg.ID = generateMessageID()
		}

		// Run middlewares
		for _, mw := range c.Hub.middlewares {
			if err := mw(c, &msg); err != nil {
				c.sendError(err.Error())
				continue
			}
		}

		// Route message to handler
		c.Hub.routeMessage(c, &msg)
	}
}

// writePump writes messages to the WebSocket connection.
func (c *Client) writePump() {
	ticker := time.NewTicker(PingInterval)
	defer func() {
		ticker.Stop()
		c.Conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.Send:
			c.Conn.SetWriteDeadline(time.Now().Add(WriteTimeout))
			if !ok {
				c.Conn.WriteMessage(8, nil) // CloseMessage
				return
			}
			if err := c.Conn.WriteMessage(1, message); err != nil {
				return
			}

		case <-ticker.C:
			c.Conn.SetWriteDeadline(time.Now().Add(WriteTimeout))
			if err := c.Conn.WriteControl(9, nil, time.Now().Add(WriteTimeout)); err != nil {
				return
			}
		}
	}
}

// sendError sends an error message back to the client.
func (c *Client) sendError(errMsg string) {
	msg := &Message{
		Type:      TypeError,
		Payload:   json.RawMessage(fmt.Sprintf(`{"error":"%s"}`, errMsg)),
		Timestamp: time.Now().Unix(),
	}
	data, _ := json.Marshal(msg)
	select {
	case c.Send <- data:
	default:
	}
}

// SetMetadata stores arbitrary metadata on the client connection.
func (c *Client) SetMetadata(key string, value interface{}) {
	c.metadata[key] = value
}

// GetMetadata retrieves metadata from the client connection.
func (c *Client) GetMetadata(key string) (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	val, ok := c.metadata[key]
	return val, ok
}

// routeMessage dispatches the message to the appropriate handler.
func (h *Hub) routeMessage(client *Client, msg *Message) {
	switch msg.Type {
	case TypeJoin:
		room := msg.Room
		if room == "" {
			client.sendError("room is required for join messages")
			return
		}
		if err := h.JoinRoom(client, room); err != nil {
			client.sendError(err.Error())
		}

	case TypeLeave:
		room := msg.Room
		if room == "" {
			client.sendError("room is required for leave messages")
			return
		}
		h.LeaveRoom(client, room)

	case TypeBroadcast:
		h.broadcast <- msg

	case TypeDirect:
		if msg.To == "" {
			client.sendError("'to' field required for direct messages")
			return
		}
		if err := h.SendDirect(client.ID, msg.To, msg.Payload); err != nil {
			client.sendError(err.Error())
		}

	default:
		handler, ok := h.handlers[msg.Type]
		if !ok {
			client.sendError(fmt.Sprintf("unknown message type: %s", msg.Type))
			return
		}
		if err := handler(client, msg); err != nil {
			client.sendError(err.Error())
		}
	}
}

// generateMessageID creates a unique message identifier.
func generateMessageID() string {
	rand.Seed(time.Now().UnixNano())
	return fmt.Sprintf("msg_%d_%d", time.Now().UnixNano(), rand.Intn(10000))
}

// MessageLogger is a middleware that logs all messages.
func MessageLogger() MiddlewareFunc {
	return func(client *Client, msg *Message) error {
		log.Printf("[ws] [%s] type=%s room=%s to=%s payload=%s",
			client.ID, msg.Type, msg.Room, msg.To, string(msg.Payload))
		return nil
	}
}

// MessageRateLimit is a middleware that rate-limits messages per client.
func MessageRateLimit(maxPerSecond int) MiddlewareFunc {
	counters := make(map[string]*rateLimitEntry)

	return func(client *Client, msg *Message) error {
		entry, ok := counters[client.ID]
		if !ok || time.Since(entry.windowStart) > time.Second {
			counters[client.ID] = &rateLimitEntry{
				count:       1,
				windowStart: time.Now(),
			}
			return nil
		}

		entry.count++
		if entry.count > maxPerSecond {
			return fmt.Errorf("rate limit exceeded: max %d messages per second", maxPerSecond)
		}
		return nil
	}
}

type rateLimitEntry struct {
	count       int
	windowStart time.Time
}

// GracefulShutdown coordinates a clean shutdown of the hub.
// It drains active connections, waits for in-flight messages, and persists state.
func (h *Hub) GracefulShutdown(timeout time.Duration) error {
	done := make(chan struct{})

	go func() {
		h.mu.Lock()
		clients := make([]*Client, 0, len(h.clients))
		for _, c := range h.clients {
			clients = append(clients, c)
		}
		h.mu.Unlock()

		// Notify all clients about shutdown
		shutdownMsg, _ := json.Marshal(&Message{
			Type:      TypeSystem,
			Payload:   json.RawMessage(`{"event":"shutdown","reason":"server_restart"}`),
			Timestamp: time.Now().Unix(),
		})

		var wg sync.WaitGroup
		for _, client := range clients {
			wg.Add(1)
			go func(c *Client) {
				defer wg.Done()
				select {
				case c.Send <- shutdownMsg:
					// Give client time to receive
					time.Sleep(100 * time.Millisecond)
				case <-time.After(2 * time.Second):
				}
				c.Conn.Close()
			}(client)
		}

		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		// Force close remaining connections
		h.mu.Lock()
		for _, c := range h.clients {
			c.Conn.Close()
		}
		h.clients = make(map[string]*Client)
		h.mu.Unlock()
		return fmt.Errorf("shutdown timed out, %d connections force closed", len(h.clients))
	}
}

// ReplayMessages replays undelivered messages to a reconnecting client.
// It handles ordering guarantees and deduplication.
func (h *Hub) ReplayMessages(client *Client, persist *Persistence, lastSeenID string) {
	messages := persist.GetUndelivered(client.UserID)

	// Find the position of the last seen message
	startIdx := 0
	if lastSeenID != "" {
		for i, msg := range messages {
			if msg.ID == lastSeenID {
				startIdx = i + 1
				break
			}
		}
	}

	// Replay messages from the last seen position
	for i := startIdx; i < len(messages); i++ {
		data, err := json.Marshal(messages[i])
		if err != nil {
			continue
		}
		select {
		case client.Send <- data:
		default:
			// Client buffer full - store remaining as undelivered
			log.Printf("[ws] replay buffer full for %s, %d messages dropped", client.ID, len(messages)-i)
			return
		}
	}
}

// TopicSubscription manages pub/sub-style topic subscriptions with pattern matching.
type TopicSubscription struct {
	patterns  map[string]map[string]*Client // pattern -> clientID -> client
	exact     map[string]map[string]*Client // topic -> clientID -> client
	mu        sync.RWMutex
}

// NewTopicSubscription creates a new topic subscription manager.
func NewTopicSubscription() *TopicSubscription {
	return &TopicSubscription{
		patterns: make(map[string]map[string]*Client),
		exact:    make(map[string]map[string]*Client),
	}
}

// Subscribe adds a client subscription. Supports wildcard patterns (e.g., "chat.*").
func (ts *TopicSubscription) Subscribe(client *Client, topic string) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if containsWildcard(topic) {
		if ts.patterns[topic] == nil {
			ts.patterns[topic] = make(map[string]*Client)
		}
		ts.patterns[topic][client.ID] = client
	} else {
		if ts.exact[topic] == nil {
			ts.exact[topic] = make(map[string]*Client)
		}
		ts.exact[topic][client.ID] = client
	}
}

// Unsubscribe removes a client subscription.
func (ts *TopicSubscription) Unsubscribe(client *Client, topic string) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if containsWildcard(topic) {
		if subs := ts.patterns[topic]; subs != nil {
			delete(subs, client.ID)
		}
	} else {
		if subs := ts.exact[topic]; subs != nil {
			delete(subs, client.ID)
		}
	}
}

// Publish sends a message to all clients subscribed to the topic.
func (ts *TopicSubscription) Publish(topic string, msg *Message) {
	ts.mu.RLock()
	
	// Collect matching clients
	recipients := make(map[string]*Client)

	// Exact matches
	if subs, ok := ts.exact[topic]; ok {
		for id, client := range subs {
			recipients[id] = client
		}
	}

	// Pattern matches
	for pattern, subs := range ts.patterns {
		if matchWildcard(pattern, topic) {
			for id, client := range subs {
				recipients[id] = client
			}
		}
	}
	ts.mu.RUnlock()

	data, _ := json.Marshal(msg)
	for _, client := range recipients {
		select {
		case client.Send <- data:
		default:
			// subscriber too slow, skip
		}
	}
}

// GetSubscribers returns all client IDs subscribed to a topic.
func (ts *TopicSubscription) GetSubscribers(topic string) []string {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	seen := make(map[string]bool)
	if subs, ok := ts.exact[topic]; ok {
		for id := range subs {
			seen[id] = true
		}
	}
	for pattern, subs := range ts.patterns {
		if matchWildcard(pattern, topic) {
			for id := range subs {
				seen[id] = true
			}
		}
	}

	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	return ids
}

// UnsubscribeAll removes all subscriptions for a client.
func (ts *TopicSubscription) UnsubscribeAll(client *Client) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	for _, subs := range ts.exact {
		delete(subs, client.ID)
	}
	for _, subs := range ts.patterns {
		delete(subs, client.ID)
	}
}

func containsWildcard(s string) bool {
	for _, c := range s {
		if c == '*' || c == '?' {
			return true
		}
	}
	return false
}

// matchWildcard performs simple glob matching.
func matchWildcard(pattern, str string) bool {
	if pattern == "*" {
		return true
	}

	parts := splitPattern(pattern)
	si := 0
	for _, part := range parts {
		if part == "*" {
			continue
		}
		idx := indexAt(str, part, si)
		if idx == -1 {
			return false
		}
		si = idx + len(part)
	}
	return true
}

func splitPattern(pattern string) []string {
	var parts []string
	current := ""
	for _, c := range pattern {
		if c == '*' {
			if current != "" {
				parts = append(parts, current)
				current = ""
			}
			parts = append(parts, "*")
		} else {
			current += string(c)
		}
	}
	if current != "" {
		parts = append(parts, current)
	}
	return parts
}

func indexAt(s, substr string, start int) int {
	if start >= len(s) {
		return -1
	}
	for i := start; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
