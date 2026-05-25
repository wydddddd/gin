package ws

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// mockConn implements Connection for testing.
type mockConn struct {
	messages [][]byte
	closed   bool
}

func (m *mockConn) ReadMessage() (int, []byte, error)                             { return 0, nil, nil }
func (m *mockConn) WriteMessage(messageType int, data []byte) error               { m.messages = append(m.messages, data); return nil }
func (m *mockConn) WriteControl(messageType int, data []byte, _ time.Time) error  { return nil }
func (m *mockConn) SetReadLimit(limit int64)                                      {}
func (m *mockConn) SetReadDeadline(t time.Time) error                             { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error                            { return nil }
func (m *mockConn) SetPongHandler(h func(string) error)                           {}
func (m *mockConn) Close() error                                                  { m.closed = true; return nil }

func TestHubRegisterAndUnregister(t *testing.T) {
	hub := NewHub(nil)
	go hub.Run()

	conn := &mockConn{}
	client := &Client{
		ID:       "test-client-1",
		UserID:   "user-1",
		Conn:     conn,
		Hub:      hub,
		Send:     make(chan []byte, 256),
		Rooms:    make(map[string]bool),
		metadata: make(map[string]interface{}),
	}

	hub.register <- client
	time.Sleep(50 * time.Millisecond)

	if len(hub.clients) != 1 {
		t.Errorf("expected 1 client, got %d", len(hub.clients))
	}

	hub.unregister <- client
	time.Sleep(50 * time.Millisecond)

	if len(hub.clients) != 0 {
		t.Errorf("expected 0 clients, got %d", len(hub.clients))
	}
}

func TestJoinRoom(t *testing.T) {
	hub := NewHub(nil)
	go hub.Run()

	conn := &mockConn{}
	client := &Client{
		ID:       "test-client-2",
		UserID:   "user-2",
		Conn:     conn,
		Hub:      hub,
		Send:     make(chan []byte, 256),
		Rooms:    make(map[string]bool),
		metadata: make(map[string]interface{}),
	}

	hub.register <- client
	time.Sleep(50 * time.Millisecond)

	err := hub.JoinRoom(client, "room-1")
	if err != nil {
		t.Fatalf("JoinRoom failed: %v", err)
	}

	if !client.Rooms["room-1"] {
		t.Error("client should be in room-1")
	}

	if len(hub.rooms["room-1"]) != 1 {
		t.Errorf("room should have 1 member, got %d", len(hub.rooms["room-1"]))
	}
}

func TestBroadcast(t *testing.T) {
	hub := NewHub(nil)
	go hub.Run()

	conn1 := &mockConn{}
	conn2 := &mockConn{}

	client1 := &Client{
		ID: "client-1", UserID: "user-1", Conn: conn1, Hub: hub,
		Send: make(chan []byte, 256), Rooms: make(map[string]bool),
		metadata: make(map[string]interface{}),
	}
	client2 := &Client{
		ID: "client-2", UserID: "user-2", Conn: conn2, Hub: hub,
		Send: make(chan []byte, 256), Rooms: make(map[string]bool),
		metadata: make(map[string]interface{}),
	}

	hub.register <- client1
	hub.register <- client2
	time.Sleep(50 * time.Millisecond)

	hub.JoinRoom(client1, "test-room")
	hub.JoinRoom(client2, "test-room")

	msg := &Message{
		Type:    TypeBroadcast,
		Room:    "test-room",
		From:    "client-1",
		Payload: json.RawMessage(`{"text":"hello"}`),
	}
	hub.broadcast <- msg
	time.Sleep(50 * time.Millisecond)

	select {
	case data := <-client2.Send:
		var received Message
		json.Unmarshal(data, &received)
		if received.From != "client-1" {
			t.Errorf("expected from client-1, got %s", received.From)
		}
	default:
		t.Error("client-2 should have received the broadcast")
	}
}

func TestDirectMessage(t *testing.T) {
	hub := NewHub(nil)
	go hub.Run()

	conn := &mockConn{}
	client := &Client{
		ID: "target", UserID: "user-target", Conn: conn, Hub: hub,
		Send: make(chan []byte, 256), Rooms: make(map[string]bool),
		metadata: make(map[string]interface{}),
	}

	hub.register <- client
	time.Sleep(50 * time.Millisecond)

	err := hub.SendDirect("sender", "target", json.RawMessage(`{"text":"hi"}`))
	if err != nil {
		t.Fatalf("SendDirect failed: %v", err)
	}

	select {
	case data := <-client.Send:
		var received Message
		json.Unmarshal(data, &received)
		if received.Type != TypeDirect {
			t.Errorf("expected direct message, got %s", received.Type)
		}
	default:
		t.Error("target should have received the direct message")
	}
}

func TestPersistenceStore(t *testing.T) {
	persist := NewPersistence(100, "")

	msg := &Message{
		Type:    TypeBroadcast,
		Room:    "test",
		Payload: json.RawMessage(`{"data":"test"}`),
	}
	persist.Store(msg)

	history := persist.GetHistory("test", time.Now().Add(-time.Minute), 10)
	if len(history) != 1 {
		t.Errorf("expected 1 message in history, got %d", len(history))
	}
}

func TestStatsHandler(t *testing.T) {
	hub := NewHub(nil)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request, _ = http.NewRequest("GET", "/stats", nil)

	StatsHandler(hub)(c)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}
