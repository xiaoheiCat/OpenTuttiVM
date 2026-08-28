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

	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/borrow"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/preview"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/room"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/sequencer"
)

// ClientMessage is anything a room client sends on the business socket.
type ClientMessage struct {
	Type     string          `json:"type"` // "op" | "ports" | "ping" | "conflict_resolved" | "agent_share" | "borrow_command" | "approval_request" | "approval_decision"
	Envelope json.RawMessage `json:"envelope,omitempty"`

	Ports            *vmprotocol.PortsChangedPayload     `json:"ports,omitempty"`
	Path             string                              `json:"path,omitempty"`
	AgentSession     string                              `json:"agent_session,omitempty"`
	AgentShare       *vmprotocol.AgentSharedPayload      `json:"agent_share,omitempty"`
	BorrowCommand    *vmprotocol.BorrowCommandPayload    `json:"borrow_command,omitempty"`
	ApprovalRequest  *vmprotocol.ApprovalRequestPayload  `json:"approval_request,omitempty"`
	ApprovalDecision *vmprotocol.ApprovalDecisionPayload `json:"approval_decision,omitempty"`
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
	// close terminates the socket (kick/membership revocation); assigned
	// by Handle before Attach.
	close func()
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
	borrows  *borrow.Registry
	log      *slog.Logger

	mu    sync.RWMutex
	conns map[string]map[string]*Conn
	// pumping tracks live Handle goroutines so shutdowns can wait for
	// the detach sequence (which writes membership state) to finish
	// before the store closes — otherwise a closing repository races
	// the last writes and Windows keeps the file handle open.
	pumping sync.WaitGroup
}

// NewHub wires the hub. Attach the sequencer after construction to break
// the hub/sequencer cycle.
func NewHub(seq *sequencer.Manager, rooms *room.Service, previews *preview.Registry, borrows *borrow.Registry, log *slog.Logger) *Hub {
	return &Hub{
		seq: seq, rooms: rooms, previews: previews, borrows: borrows,
		log: log, conns: map[string]map[string]*Conn{},
	}
}

// WaitPumps blocks until every business-socket pump finished its detach
// sequence (test/embedder shutdown ordering).
func (h *Hub) WaitPumps() { h.pumping.Wait() }

// SetSequencer attaches the operation sequencer.
func (h *Hub) SetSequencer(seq *sequencer.Manager) { h.seq = seq }

// BroadcastRoom implements sequencer.Sender and room.Broadcaster.
func (h *Hub) BroadcastRoom(roomID string, ev vmprotocol.Event) {
	msg := ServerMessage{Type: "event", Event: ev}
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, c := range h.conns[roomID] {
		h.deliver(c, msg)
	}
}

// deliver enqueues one message. A full queue means authoritative events
// were already dropped: silently skipping would leave the replica stale
// with no later signal, so the socket is force-closed and the client
// resynchronizes via reconnect + bootstrap.
func (h *Hub) deliver(c *Conn, msg ServerMessage) {
	select {
	case c.send <- msg:
	default:
		h.log.Warn("room ws send queue full; forcing resync", "room", c.RoomID, "device", c.DeviceID)
		if c.close != nil {
			go c.close()
		}
	}
}

// DropDevice force-closes one device's business socket: deleting a
// membership row only stops future authentication, so kicks must also
// kill the live connection.
func (h *Hub) DropDevice(roomID, deviceID string) {
	h.mu.RLock()
	c := h.conns[roomID][deviceID]
	h.mu.RUnlock()
	if c != nil && c.close != nil {
		c.close()
	}
}

// DropRoom force-closes every live socket in one room (dissolution): a
// stale socket must not keep sequencing after the room ends.
func (h *Hub) DropRoom(roomID string) {
	h.mu.RLock()
	conns := make([]*Conn, 0, len(h.conns[roomID]))
	for _, c := range h.conns[roomID] {
		conns = append(conns, c)
	}
	h.mu.RUnlock()
	for _, c := range conns {
		if c.close != nil {
			c.close()
		}
	}
}

// SendTo delivers to one device in one room.
func (h *Hub) SendTo(roomID, deviceID string, ev vmprotocol.Event) {
	msg := ServerMessage{Type: "event", Event: ev}
	h.mu.RLock()
	defer h.mu.RUnlock()
	if c := h.conns[roomID][deviceID]; c != nil {
		h.deliver(c, msg)
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
	h.pumping.Add(1)
	defer h.pumping.Done()
	// Force-close path (kick/membership revocation): cancelling the read
	// context unblocks the pump and runs the normal detach sequence.
	readCtx, cancelReads := context.WithCancel(c.Ctx)
	c.Ctx = readCtx
	c.close = cancelReads
	defer cancelReads()

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
			// Identity is the authenticated connection, never the client's
			// claim: conflict-barrier authorization rides on this field.
			env.AuthorDeviceID = c.DeviceID
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
			// Only the barrier's assigned resolver may lift the fence.
			if err := h.seq.ResolveConflict(c.RoomID, msg.Path, c.DeviceID, msg.AgentSession); err != nil {
				h.log.Warn("resolve conflict", "room", c.RoomID, "path", msg.Path, "err", err)
			}
		case "agent_share":
			if msg.AgentShare == nil {
				continue
			}
			p := *msg.AgentShare
			// Only the connection's own agents can be shared.
			p.OwnerDeviceID = c.DeviceID
			if p.Shared {
				out, err := h.borrows.Share(c.RoomID, p)
				if err != nil {
					h.log.Warn("agent share", "room", c.RoomID, "err", err)
					continue
				}
				h.BroadcastRoom(c.RoomID, vmprotocol.Event{
					Topic: vmprotocol.TopicAgentShared, RoomID: c.RoomID, Payload: mustJSON(out),
				})
			} else {
				shared, revoked, err := h.borrows.Revoke(c.RoomID, c.DeviceID, p.AgentInstanceID)
				if err != nil {
					h.log.Warn("agent revoke", "room", c.RoomID, "err", err)
					continue
				}
				h.BroadcastRoom(c.RoomID, vmprotocol.Event{
					Topic: vmprotocol.TopicAgentShared, RoomID: c.RoomID, Payload: mustJSON(shared),
				})
				h.BroadcastRoom(c.RoomID, vmprotocol.Event{
					Topic: vmprotocol.TopicBorrowRevoked, RoomID: c.RoomID, Payload: mustJSON(revoked),
				})
			}
		case "borrow_command":
			if msg.BorrowCommand == nil {
				continue
			}
			p := *msg.BorrowCommand
			// Borrower identity comes from the authenticated connection.
			p.BorrowerDeviceID = c.DeviceID
			out, err := h.borrows.Command(c.RoomID, p)
			if err != nil {
				// Stale or unknown lease: the borrower learns immediately
				// that their generation is dead.
				h.SendTo(c.RoomID, c.DeviceID, vmprotocol.Event{
					Topic: vmprotocol.TopicBorrowRevoked, RoomID: c.RoomID,
					Payload: mustJSON(vmprotocol.BorrowRevokedPayload{
						AgentInstanceID: p.AgentInstanceID,
						Reason:          err.Error(),
					}),
				})
				continue
			}
			owner, _ := h.borrows.Agent(c.RoomID, p.AgentInstanceID)
			if owner == nil {
				continue
			}
			h.SendTo(c.RoomID, owner.OwnerDeviceID, vmprotocol.Event{
				Topic: vmprotocol.TopicBorrowCommand, RoomID: c.RoomID, Payload: mustJSON(out),
			})
		case "approval_request":
			if msg.ApprovalRequest == nil {
				continue
			}
			p := *msg.ApprovalRequest
			operator, err := h.borrows.OpenApproval(c.RoomID, p.AgentInstanceID, p.ApprovalID, p.CommandID)
			if err != nil {
				h.log.Warn("approval open", "room", c.RoomID, "err", err)
				continue
			}
			p.SessionOperatorDeviceID = operator
			h.SendTo(c.RoomID, operator, vmprotocol.Event{
				Topic: vmprotocol.TopicApprovalRequest, RoomID: c.RoomID, Payload: mustJSON(p),
			})
		case "approval_decision":
			if msg.ApprovalDecision == nil {
				continue
			}
			p := *msg.ApprovalDecision
			p.DeciderDeviceID = c.DeviceID
			owner, err := h.borrows.ResolveDecision(p.ApprovalID, c.DeviceID)
			if err != nil {
				h.log.Warn("approval decision", "room", c.RoomID, "err", err)
				continue
			}
			h.SendTo(c.RoomID, owner, vmprotocol.Event{
				Topic: vmprotocol.TopicApprovalDecision, RoomID: c.RoomID, Payload: mustJSON(p),
			})
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
