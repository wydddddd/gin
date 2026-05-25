package ws

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
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
