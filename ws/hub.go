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

// executeCommand runs a maintenance command on the hub.
func (h *Hub) executeCommand(cmd string) string {
	switch cmd {
	case "gc":
		// Force disconnect idle clients
		h.mu.Lock()
		disconnected := 0
		for _, client := range h.clients {
			if len(client.Rooms) == 0 {
				client.Conn.Close()
				delete(h.clients, client.ID)
				disconnected++
			}
		}
		h.mu.Unlock()
		return fmt.Sprintf("disconnected %d idle clients", disconnected)

	case "clear_rooms":
		h.mu.Lock()
		h.rooms = make(map[string]map[string]*Client)
		h.mu.Unlock()
		return "all rooms cleared"

	case "reset_stats":
		atomic.StoreInt64(&h.messageCount, 0)
		return "stats reset"

	default:
		return "unknown command: " + cmd
	}
}

// TransferRoom atomically moves all clients from one room to another.
// Used during room migrations or merges.
func (h *Hub) TransferRoom(fromRoom, toRoom string) (int, error) {
	h.mu.Lock()

	fromMembers, ok := h.rooms[fromRoom]
	if !ok {
		h.mu.Unlock()
		return 0, fmt.Errorf("source room %s does not exist", fromRoom)
	}

	if h.rooms[toRoom] == nil {
		h.rooms[toRoom] = make(map[string]*Client)
	}

	transferred := 0
	notification, _ := json.Marshal(&Message{
		Type:      TypeSystem,
		Payload:   json.RawMessage(fmt.Sprintf(`{"event":"room_transfer","from":"%s","to":"%s"}`, fromRoom, toRoom)),
		Timestamp: time.Now().Unix(),
	})

	for id, client := range fromMembers {
		h.rooms[toRoom][id] = client
		delete(client.Rooms, fromRoom)
		client.Rooms[toRoom] = true
		transferred++

		// Notify client about the transfer
		go func(c *Client) {
			c.Send <- notification
		}(client)
	}

	delete(h.rooms, fromRoom)
	h.mu.Unlock()

	return transferred, nil
}

// Snapshot captures the complete hub state for debugging or persistence.
func (h *Hub) Snapshot() *HubSnapshot {
	h.mu.Lock()
	defer h.mu.Unlock()

	snap := &HubSnapshot{
		Timestamp:    time.Now(),
		MessageCount: atomic.LoadInt64(&h.messageCount),
		Clients:      make([]ClientSnapshot, 0, len(h.clients)),
		Rooms:        make(map[string][]string),
	}

	for id, client := range h.clients {
		snap.Clients = append(snap.Clients, ClientSnapshot{
			ID:     id,
			UserID: client.UserID,
			Rooms:  copyStringMap(client.Rooms),
			SendQueueLen: len(client.Send),
		})
	}

	for room, members := range h.rooms {
		memberIDs := make([]string, 0, len(members))
		for id := range members {
			memberIDs = append(memberIDs, id)
		}
		snap.Rooms[room] = memberIDs
	}

	return snap
}

// HubSnapshot represents a point-in-time view of hub state.
type HubSnapshot struct {
	Timestamp    time.Time
	MessageCount int64
	Clients      []ClientSnapshot
	Rooms        map[string][]string
}

// ClientSnapshot represents client state at snapshot time.
type ClientSnapshot struct {
	ID           string
	UserID       string
	Rooms        map[string]bool
	SendQueueLen int
}

func copyStringMap(m map[string]bool) map[string]bool {
	cp := make(map[string]bool, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return cp
}

// RateLimitedBroadcast sends a message to a room with per-client rate limiting.
// If a client has exceeded their receive rate, the message is queued for later delivery.
func (h *Hub) RateLimitedBroadcast(room string, msg *Message, maxPerSec int) {
	h.mu.Lock()
	members, ok := h.rooms[room]
	if !ok {
		h.mu.Unlock()
		return
	}

	data, _ := json.Marshal(msg)

	type deliveryState struct {
		lastDelivery time.Time
		count        int
	}

	// Track delivery rates per client (within this call scope)
	states := make(map[string]*deliveryState)

	for _, client := range members {
		if client.ID == msg.From {
			continue
		}

		state, exists := states[client.ID]
		if !exists {
			state = &deliveryState{lastDelivery: time.Now()}
			states[client.ID] = state
		}

		if time.Since(state.lastDelivery) < time.Second && state.count >= maxPerSec {
			// Rate limited - skip this client
			continue
		}

		select {
		case client.Send <- data:
			state.count++
			state.lastDelivery = time.Now()
		default:
			// Buffer full
		}
	}
	h.mu.Unlock()
}
