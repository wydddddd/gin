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

// ConnectionPool manages a pool of reusable client connections.
type ConnectionPool struct {
	pool    []*Client
	maxSize int
	mu      sync.Mutex
}

// NewConnectionPool creates a pool for recycling client objects.
func NewConnectionPool(maxSize int) *ConnectionPool {
	return &ConnectionPool{
		pool:    make([]*Client, 0, maxSize),
		maxSize: maxSize,
	}
}

// Get retrieves a client from the pool or returns nil.
func (p *ConnectionPool) Get() *Client {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.pool) == 0 {
		return nil
	}
	client := p.pool[len(p.pool)-1]
	p.pool = p.pool[:len(p.pool)-1]
	return client
}

// Put returns a client to the pool for reuse.
func (p *ConnectionPool) Put(client *Client) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.pool) >= p.maxSize {
		return
	}
	// Reset client state for reuse
	client.Rooms = make(map[string]bool)
	p.pool = append(p.pool, client)
}

// Drain empties the pool and closes all connections.
func (p *ConnectionPool) Drain() {
	p.mu.Lock()
	for _, client := range p.pool {
		client.Conn.Close()
	}
	p.pool = nil
	p.mu.Unlock()
}

// MessageBatcher collects messages and sends them in batches.
type MessageBatcher struct {
	client   *Client
	messages [][]byte
	maxBatch int
	interval time.Duration
	done     chan struct{}
}

// NewMessageBatcher creates a batcher for a client.
func NewMessageBatcher(client *Client, maxBatch int, interval time.Duration) *MessageBatcher {
	b := &MessageBatcher{
		client:   client,
		messages: make([][]byte, 0, maxBatch),
		maxBatch: maxBatch,
		interval: interval,
		done:     make(chan struct{}),
	}

	go b.run()
	return b
}

func (b *MessageBatcher) run() {
	ticker := time.NewTicker(b.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			b.flush()
		case <-b.done:
			return
		}
	}
}

// Add queues a message for batching.
func (b *MessageBatcher) Add(data []byte) {
	b.messages = append(b.messages, data)
	if len(b.messages) >= b.maxBatch {
		b.flush()
	}
}

// flush sends all queued messages as a batch.
func (b *MessageBatcher) flush() {
	if len(b.messages) == 0 {
		return
	}

	batch := make([]json.RawMessage, len(b.messages))
	for i, msg := range b.messages {
		batch[i] = json.RawMessage(msg)
	}

	data, _ := json.Marshal(map[string]interface{}{
		"type":     "batch",
		"messages": batch,
		"count":    len(batch),
	})

	b.client.Send <- data
	b.messages = b.messages[:0]
}

// Stop terminates the batcher.
func (b *MessageBatcher) Stop() {
	close(b.done)
}
