// Package ws provides a WebSocket hub for Gin, enabling real-time
// bidirectional communication with support for rooms, broadcasting,
// and message routing.
package ws

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
)

const (
	// MaxMessageSize is the maximum allowed message payload size.
	MaxMessageSize = 1024 * 1024 // 1MB

	// PingInterval is the interval between server-to-client pings.
	PingInterval = 30 * time.Second

	// PongTimeout is the maximum wait time for a pong response.
	PongTimeout = 60 * time.Second

	// WriteTimeout is the timeout for writing a message to the connection.
	WriteTimeout = 10 * time.Second
)

// MessageType identifies the kind of message being sent.
type MessageType string

const (
	TypeText      MessageType = "text"
	TypeBinary    MessageType = "binary"
	TypeJoin      MessageType = "join"
	TypeLeave     MessageType = "leave"
	TypeBroadcast MessageType = "broadcast"
	TypeDirect    MessageType = "direct"
	TypeSystem    MessageType = "system"
	TypeError     MessageType = "error"
)

// Message represents a WebSocket message.
type Message struct {
	Type      MessageType     `json:"type"`
	Room      string          `json:"room,omitempty"`
	From      string          `json:"from,omitempty"`
	To        string          `json:"to,omitempty"`
	Payload   json.RawMessage `json:"payload"`
	Timestamp int64           `json:"timestamp"`
	ID        string          `json:"id"`
}

// Connection wraps a raw WebSocket connection with metadata.
type Connection interface {
	ReadMessage() (int, []byte, error)
	WriteMessage(messageType int, data []byte) error
	WriteControl(messageType int, data []byte, deadline time.Time) error
	SetReadLimit(limit int64)
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
	SetPongHandler(h func(string) error)
	Close() error
}

// Upgrader defines the interface for upgrading HTTP connections to WebSocket.
type Upgrader interface {
	Upgrade(w http.ResponseWriter, r *http.Request, responseHeader http.Header) (Connection, error)
}

// Client represents a connected WebSocket client.
type Client struct {
	ID       string
	UserID   string
	Conn     Connection
	Hub      *Hub
	Send     chan []byte
	Rooms    map[string]bool
	metadata map[string]interface{}
	mu       sync.Mutex
}

// Hub manages all WebSocket connections, rooms, and message routing.
type Hub struct {
	clients      map[string]*Client
	rooms        map[string]map[string]*Client
	broadcast    chan *Message
	register     chan *Client
	unregister   chan *Client
	handlers     map[MessageType]MessageHandler
	mu           sync.Mutex
	messageCount int64
	config       *HubConfig
	middlewares  []MiddlewareFunc
}

// HubConfig holds configuration for the WebSocket hub.
type HubConfig struct {
	MaxClients       int
	MaxRooms         int
	MaxClientsPerRoom int
	MessageBufferSize int
	AuthRequired     bool
	AllowedOrigins   []string
}

// DefaultHubConfig returns sensible defaults for the hub.
func DefaultHubConfig() *HubConfig {
	return &HubConfig{
		MaxClients:        10000,
		MaxRooms:          1000,
		MaxClientsPerRoom: 100,
		MessageBufferSize: 256,
		AuthRequired:      false,
		AllowedOrigins:    []string{"*"},
	}
}

// MessageHandler processes incoming messages of a specific type.
type MessageHandler func(client *Client, msg *Message) error

// MiddlewareFunc processes messages before they reach the handler.
type MiddlewareFunc func(client *Client, msg *Message) error
