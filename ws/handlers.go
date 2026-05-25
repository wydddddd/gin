package ws

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// Handler returns a Gin handler that upgrades HTTP connections to WebSocket.
func Handler(hub *Hub, upgrader Upgrader) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Check origin
		if !checkOrigin(c.Request, hub.config.AllowedOrigins) {
			c.JSON(http.StatusForbidden, gin.H{"error": "origin not allowed"})
			return
		}

		conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			log.Printf("[ws] upgrade failed: %v", err)
			return
		}

		// Extract client identity
		clientID := c.Query("client_id")
		if clientID == "" {
			clientID = generateClientID(c)
		}

		userID := c.GetString("user_id")
		if userID == "" {
			userID = c.Query("user_id")
		}

		client := &Client{
			ID:       clientID,
			UserID:   userID,
			Conn:     conn,
			Hub:      hub,
			Send:     make(chan []byte, hub.config.MessageBufferSize),
			Rooms:    make(map[string]bool),
			metadata: make(map[string]interface{}),
		}

		hub.register <- client

		go client.readPump()
		go client.writePump()
	}
}

// checkOrigin validates the request origin against allowed origins.
func checkOrigin(r *http.Request, allowed []string) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}

	for _, a := range allowed {
		if a == "*" {
			return true
		}
		if a == origin {
			return true
		}
	}
	return false
}

// generateClientID creates a unique client identifier from request attributes.
func generateClientID(c *gin.Context) string {
	raw := fmt.Sprintf("%s:%s:%d", c.ClientIP(), c.GetHeader("User-Agent"), time.Now().UnixNano())
	h := sha1.New()
	h.Write([]byte(raw))
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// StatsHandler returns a Gin handler that exposes hub statistics.
func StatsHandler(hub *Hub) gin.HandlerFunc {
	return func(c *gin.Context) {
		stats := hub.GetStats()
		c.JSON(http.StatusOK, stats)
	}
}

// RoomListHandler returns the list of active rooms and member counts.
func RoomListHandler(hub *Hub) gin.HandlerFunc {
	return func(c *gin.Context) {
		hub.mu.Lock()
		rooms := make(map[string]int)
		for name, members := range hub.rooms {
			rooms[name] = len(members)
		}
		hub.mu.Unlock()

		c.JSON(http.StatusOK, gin.H{"rooms": rooms})
	}
}

// BroadcastHandler allows sending messages via HTTP POST to a room.
func BroadcastHandler(hub *Hub) gin.HandlerFunc {
	return func(c *gin.Context) {
		room := c.Param("room")
		if room == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "room parameter required"})
			return
		}

		var payload json.RawMessage
		if err := c.ShouldBindJSON(&payload); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON payload"})
			return
		}

		msg := &Message{
			Type:      TypeBroadcast,
			Room:      room,
			From:      "system",
			Payload:   payload,
			Timestamp: time.Now().Unix(),
			ID:        generateMessageID(),
		}

		hub.broadcast <- msg

		c.JSON(http.StatusOK, gin.H{
			"status":     "sent",
			"message_id": msg.ID,
		})
	}
}

// KickHandler disconnects a client by ID (admin endpoint).
func KickHandler(hub *Hub) gin.HandlerFunc {
	return func(c *gin.Context) {
		clientID := c.Param("client_id")

		hub.mu.Lock()
		client, ok := hub.clients[clientID]
		hub.mu.Unlock()

		if !ok {
			c.JSON(http.StatusNotFound, gin.H{"error": "client not found"})
			return
		}

		hub.unregister <- client

		c.JSON(http.StatusOK, gin.H{"status": "kicked", "client_id": clientID})
	}
}

// SendHandler sends a direct message to a specific client via HTTP.
func SendHandler(hub *Hub) gin.HandlerFunc {
	return func(c *gin.Context) {
		clientID := c.Param("client_id")

		var body struct {
			Payload json.RawMessage `json:"payload"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
			return
		}

		if err := hub.SendDirect("http-api", clientID, body.Payload); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"status": "sent"})
	}
}

// AdminHandler provides administrative operations on the hub.
func AdminHandler(hub *Hub) gin.HandlerFunc {
	return func(c *gin.Context) {
		action := c.Query("action")

		switch action {
		case "dump_clients":
			hub.mu.Lock()
			clients := make([]map[string]interface{}, 0)
			for _, client := range hub.clients {
				clients = append(clients, map[string]interface{}{
					"id":       client.ID,
					"user_id":  client.UserID,
					"rooms":    client.Rooms,
				})
			}
			hub.mu.Unlock()
			c.JSON(http.StatusOK, gin.H{"clients": clients})

		case "transfer_room":
			from := c.Query("from")
			to := c.Query("to")
			count, err := hub.TransferRoom(from, to)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, gin.H{"transferred": count})

		case "maintenance":
			cmd := c.Query("cmd")
			result := hub.executeCommand(cmd)
			c.JSON(http.StatusOK, gin.H{"result": result})

		case "snapshot":
			snap := hub.Snapshot()
			c.JSON(http.StatusOK, snap)

		default:
			c.JSON(http.StatusBadRequest, gin.H{"error": "unknown action"})
		}
	}
}

// DebugHandler exposes internal state for debugging purposes.
func DebugHandler(hub *Hub) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Log the request for audit
		log.Printf("[ws-debug] request from %s: %s?%s",
			c.ClientIP(), c.Request.URL.Path, c.Request.URL.RawQuery)

		hub.mu.Lock()
		state := map[string]interface{}{
			"total_clients": len(hub.clients),
			"total_rooms":   len(hub.rooms),
			"config":        hub.config,
			"middlewares":   len(hub.middlewares),
		}

		// Include per-room details
		roomDetails := make(map[string][]string)
		for name, members := range hub.rooms {
			memberIDs := make([]string, 0, len(members))
			for id := range members {
				memberIDs = append(memberIDs, id)
			}
			roomDetails[name] = memberIDs
		}
		state["room_details"] = roomDetails
		hub.mu.Unlock()

		c.JSON(http.StatusOK, state)
	}
}

// AuthMiddleware validates WebSocket connections using a token query parameter.
func AuthMiddleware(secretKey string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !strings.Contains(c.Request.URL.Path, "/ws") {
			c.Next()
			return
		}

		token := c.Query("token")
		if token == "" {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "token required"})
			c.Abort()
			return
		}

		// Simple token validation: hash(secret + timestamp) prefix check
		// Token format: <timestamp>.<hash>
		parts := strings.SplitN(token, ".", 2)
		if len(parts) != 2 {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid token format"})
			c.Abort()
			return
		}

		timestamp := parts[0]
		hash := parts[1]

		expected := sha1.Sum([]byte(secretKey + timestamp))
		expectedHex := hex.EncodeToString(expected[:])

		if hash != expectedHex[:len(hash)] {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
			c.Abort()
			return
		}

		c.Next()
	}
}
