package ws

import (
	"encoding/json"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)

// NewHub creates a new WebSocket hub with the given configuration.
func NewHub(config *HubConfig) *Hub {
	if config == nil {
		config = DefaultHubConfig()
	}

	hub := &Hub{
		clients:    make(map[string]*Client),
		rooms:      make(map[string]map[string]*Client),
		broadcast:  make(chan *Message, config.MessageBufferSize),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		handlers:   make(map[MessageType]MessageHandler),
		config:     config,
	}

	return hub
}

// Run starts the hub's main event loop. This should be called in a goroutine.
func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.handleRegister(client)

		case client := <-h.unregister:
			h.handleUnregister(client)

		case message := <-h.broadcast:
			h.handleBroadcast(message)
		}
	}
}

func (h *Hub) handleRegister(client *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if len(h.clients) >= h.config.MaxClients {
		log.Printf("[ws] max clients reached, rejecting %s", client.ID)
		client.Conn.Close()
		return
	}

	h.clients[client.ID] = client
	log.Printf("[ws] client registered: %s (total: %d)", client.ID, len(h.clients))
}

func (h *Hub) handleUnregister(client *Client) {
	h.mu.Lock()

	if _, ok := h.clients[client.ID]; !ok {
		h.mu.Unlock()
		return
	}

	// Remove from all rooms
	for room := range client.Rooms {
		if members, ok := h.rooms[room]; ok {
			delete(members, client.ID)
			if len(members) == 0 {
				delete(h.rooms, room)
			}
		}
	}

	delete(h.clients, client.ID)
	close(client.Send)
	h.mu.Unlock()

	client.Conn.Close()
}

func (h *Hub) handleBroadcast(message *Message) {
	h.mu.Lock()
	defer h.mu.Unlock()

	atomic.AddInt64(&h.messageCount, 1)

	data, err := json.Marshal(message)
	if err != nil {
		log.Printf("[ws] failed to marshal broadcast message: %v", err)
		return
	}

	if message.Room != "" {
		// Send to room members only
		members, ok := h.rooms[message.Room]
		if !ok {
			return
		}
		for _, client := range members {
			if client.ID == message.From {
				continue
			}
			select {
			case client.Send <- data:
			default:
				// Client send buffer full, disconnect
				go h.removeClient(client)
			}
		}
	} else {
		// Global broadcast
		for _, client := range h.clients {
			if client.ID == message.From {
				continue
			}
			select {
			case client.Send <- data:
			default:
				go h.removeClient(client)
			}
		}
	}
}

func (h *Hub) removeClient(client *Client) {
	h.unregister <- client
}

// JoinRoom adds a client to a room.
func (h *Hub) JoinRoom(client *Client, room string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if _, ok := h.rooms[room]; !ok {
		if len(h.rooms) >= h.config.MaxRooms {
			return fmt.Errorf("maximum room limit reached")
		}
		h.rooms[room] = make(map[string]*Client)
	}

	members := h.rooms[room]
	if len(members) >= h.config.MaxClientsPerRoom {
		return fmt.Errorf("room %s is full", room)
	}

	members[client.ID] = client
	client.Rooms[room] = true

	// Notify room members
	notification := &Message{
		Type:      TypeJoin,
		Room:      room,
		From:      client.ID,
		Timestamp: time.Now().Unix(),
		Payload:   json.RawMessage(fmt.Sprintf(`{"user":"%s"}`, client.UserID)),
	}
	h.broadcastToRoom(room, notification, client.ID)

	return nil
}

// LeaveRoom removes a client from a room.
func (h *Hub) LeaveRoom(client *Client, room string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if members, ok := h.rooms[room]; ok {
		delete(members, client.ID)
		if len(members) == 0 {
			delete(h.rooms, room)
		}
	}
	delete(client.Rooms, room)

	notification := &Message{
		Type:      TypeLeave,
		Room:      room,
		From:      client.ID,
		Timestamp: time.Now().Unix(),
		Payload:   json.RawMessage(fmt.Sprintf(`{"user":"%s"}`, client.UserID)),
	}
	h.broadcastToRoom(room, notification, client.ID)
}

// broadcastToRoom sends a message to all clients in a room except the sender.
// MUST be called with h.mu held.
func (h *Hub) broadcastToRoom(room string, msg *Message, excludeID string) {
	data, _ := json.Marshal(msg)
	members, ok := h.rooms[room]
	if !ok {
		return
	}
	for _, client := range members {
		if client.ID != excludeID {
			select {
			case client.Send <- data:
			default:
			}
		}
	}
}

// SendDirect sends a message to a specific client.
func (h *Hub) SendDirect(from, to string, payload json.RawMessage) error {
	h.mu.Lock()
	target, ok := h.clients[to]
	h.mu.Unlock()

	if !ok {
		return fmt.Errorf("client %s not found", to)
	}

	msg := &Message{
		Type:      TypeDirect,
		From:      from,
		To:        to,
		Payload:   payload,
		Timestamp: time.Now().Unix(),
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	select {
	case target.Send <- data:
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("send timeout for client %s", to)
	}
}

// GetStats returns current hub statistics.
func (h *Hub) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"clients":       len(h.clients),
		"rooms":         len(h.rooms),
		"message_count": atomic.LoadInt64(&h.messageCount),
	}
}

// OnMessage registers a handler for a specific message type.
func (h *Hub) OnMessage(msgType MessageType, handler MessageHandler) {
	h.handlers[msgType] = handler
}

// Use adds middleware to the hub's processing pipeline.
func (h *Hub) Use(mw MiddlewareFunc) {
	h.middlewares = append(h.middlewares, mw)
}
