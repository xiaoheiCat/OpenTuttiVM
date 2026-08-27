// Package realtime implements the business WebSocket hub: presence,
// workspace operation submission, agent/port events, and targeted delivery.
package realtime

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"

	"github.com/coder/websocket"
	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"

	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/preview"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/room"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/sequencer"
)

// ClientMessage is anything a room client sends on the business socket.
type ClientMessage struct {
	Type     string                          `json:"type"` // "op" | "ports" | "ping" | "conflict_resolved"
	Envelope json.RawMessage                 `json:"envelope,omitempty"`
	Ports    *vmprotocol.PortsChangedPayload `json:"ports,omitempty"`
	Path     string                          `json:"path,omitempty"`
}

// ServerMessage is anything the server sends.
type ServerMessage struct {
	Type  string           `json:"type"` // "event"
	Event vmprotocol.Event `json:"event"`
}

// Conn is one authenticated device websocket.
type Conn struct {
	RoomID     string
	DeviceID   string
	DeviceSlug string
	Ctx        context.Context
	send       chan ServerMessage
}

// NewConn builds a connection handle.
func NewConn(ctx context.Context, roomID, deviceID, deviceSlug string) *Conn {
	return &Conn{
		RoomID: roomID, DeviceID: deviceID, DeviceSlug: deviceSlug, Ctx: ctx,
		send: make(chan ServerMessage, 64),
	}
}

// Hub tracks room connections and fans messages.
type Hub struct {
	seq      *sequencer.Manager
	rooms    *room.Service
	previews *preview.Registry
	log      *slog.Logger

	mu    sync.RWMutex
	conns map[string]map[string]*Conn
}

// NewHub wires the hub. Attach the sequencer after construction to break
// the hub/sequencer cycle.
func NewHub(seq *sequencer.Manager, rooms *room.Service, previews *preview.Registry, log *slog.Logger) *Hub {
	return &Hub{seq: seq, rooms: rooms, previews: previews, log: log, conns: map[string]map[string]*Conn{}}
}

// SetSequencer attaches the operation sequencer.
func (h *Hub) SetSequencer(seq *sequencer.Manager) { h.seq = seq }

// BroadcastRoom implements sequencer.Sender and room.Broadcaster.
func (h *Hub) BroadcastRoom(roomID string, ev vmprotocol.Event) {
	msg := ServerMessage{Type: "event", Event: ev}
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, c := range h.conns[roomID] {
		select {
		case c.send <- msg:
		default:
			// Slow consumer: drop rather than block the fanout; the client
			// resyncs from snapshots on reconnect.
		}
	}
}

// SendTo delivers to one device in one room.
func (h *Hub) SendTo(roomID, deviceID string, ev vmprotocol.Event) {
	msg := ServerMessage{Type: "event", Event: ev}
	h.mu.RLock()
	defer h.mu.RUnlock()
	if c := h.conns[roomID][deviceID]; c != nil {
		select {
		case c.send <- msg:
		default:
		}
	}
}

// Attach registers an authenticated connection.
func (h *Hub) Attach(c *Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.conns[c.RoomID] == nil {
		h.conns[c.RoomID] = map[string]*Conn{}
	}
	h.conns[c.RoomID][c.DeviceID] = c
}

// Detach removes a connection and runs the disconnect path.
func (h *Hub) Detach(c *Conn) {
	h.mu.Lock()
	if devs := h.conns[c.RoomID]; devs != nil {
		delete(devs, c.DeviceID)
		if len(devs) == 0 {
			delete(h.conns, c.RoomID)
		}
	}
	h.mu.Unlock()

	if _, err := h.rooms.MarkOffline(c.Ctx, c.RoomID, c.DeviceID); err != nil {
		h.log.Warn("mark offline", "room", c.RoomID, "device", c.DeviceID, "err", err)
	}
	h.BroadcastRoom(c.RoomID, vmprotocol.Event{
		Topic:   vmprotocol.TopicPresence,
		RoomID:  c.RoomID,
		Payload: mustJSON(vmprotocol.PresenceDevice{DeviceID: c.DeviceID, Online: false}),
	})
}

// Handle pumps one websocket until it closes.
func (h *Hub) Handle(c *Conn, ws *websocket.Conn) {
	h.Attach(c)
	defer h.Detach(c)
	defer ws.Close(websocket.StatusNormalClosure, "")

	writeCtx, cancelWrites := context.WithCancel(c.Ctx)
	defer cancelWrites()
	go func() {
		for msg := range c.send {
			data, err := json.Marshal(msg)
			if err != nil {
				continue
			}
			if err := ws.Write(writeCtx, websocket.MessageText, data); err != nil {
				cancelWrites()
				return
			}
		}
	}()

	for {
		_, data, err := ws.Read(c.Ctx)
		if err != nil {
			return
		}
		var msg ClientMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}
		switch msg.Type {
		case "op":
			var env vmprotocol.Envelope
			if err := json.Unmarshal(msg.Envelope, &env); err != nil {
				continue
			}
			env.RoomID = c.RoomID
			if env.AuthorDeviceID == "" {
				env.AuthorDeviceID = c.DeviceID
			}
			_ = h.seq.Submit(env) // rejections become targeted events
		case "ports":
			if msg.Ports == nil {
				continue
			}
			p := *msg.Ports
			p.DeviceID = c.DeviceID
			key := vmprotocol.RouteKey{RoomID: c.RoomID, DeviceID: c.DeviceID, SessionID: p.SessionID, Port: p.Port}
			if p.Listening {
				h.previews.Upsert(preview.Entry{
					RouteKey:     key,
					SessionLabel: p.SessionLabel,
					Agent:        labelAgent(p.SessionLabel),
					Protocol:     p.Protocol,
					DeviceSlug:   c.DeviceSlug,
				})
			} else {
				h.previews.Remove(key)
			}
			h.BroadcastRoom(c.RoomID, vmprotocol.Event{
				Topic: vmprotocol.TopicPortsChanged, RoomID: c.RoomID, Payload: mustJSON(p),
			})
		case "conflict_resolved":
			if err := h.seq.ResolveConflict(c.RoomID, msg.Path); err != nil {
				h.log.Warn("resolve conflict", "room", c.RoomID, "path", msg.Path, "err", err)
			}
		case "ping":
			_ = h.rooms.MarkOnline(c.Ctx, c.RoomID, c.DeviceID)
		}
	}
}

func labelAgent(sessionLabel string) string {
	for i := 0; i < len(sessionLabel); i++ {
		if sessionLabel[i] == '-' {
			return sessionLabel[:i]
		}
	}
	return sessionLabel
}

func mustJSON(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}
