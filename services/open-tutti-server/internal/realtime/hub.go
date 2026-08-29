// Package realtime implements the business WebSocket hub: presence,
// workspace operation submission, agent/port events, and targeted delivery.
package realtime

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	borrowagent "github.com/xiaoheiCat/OpenTuttiVM/packages/agent/borrow"
	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"

	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/borrow"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/preview"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/room"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/sequencer"
)

// ClientMessage is anything a room client sends on the business socket.
type ClientMessage struct {
	Type     string          `json:"type"` // "op" | "ports" | "ping" | "policy" | "conflict_resolved" | "agent_share" | "borrow_command" | "approval_request" | "approval_decision"
	Envelope json.RawMessage `json:"envelope,omitempty"`

	Ports            *vmprotocol.PortsChangedPayload      `json:"ports,omitempty"`
	Policy           *PolicyReportPayload                 `json:"policy,omitempty"`
	Path             string                               `json:"path,omitempty"`
	AgentSession     string                               `json:"agent_session,omitempty"`
	AgentShare       *borrowagent.AgentSharedPayload      `json:"agent_share,omitempty"`
	BorrowCommand    *borrowagent.BorrowCommandPayload    `json:"borrow_command,omitempty"`
	ApprovalRequest  *borrowagent.ApprovalRequestPayload  `json:"approval_request,omitempty"`
	ApprovalDecision *borrowagent.ApprovalDecisionPayload `json:"approval_decision,omitempty"`
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
	// lastSeen is the last inbound-frame time (UnixNano): the idle
	// reaper force-closes silent sockets so MarkOffline runs and the
	// owner grace period cannot stall behind a half-open connection.
	lastSeen atomic.Int64
	// close terminates the socket (kick/membership revocation); assigned
	// by Handle before Attach.
	close func()
}

// NewConn builds a connection handle.
func NewConn(ctx context.Context, roomID, deviceID, deviceSlug string) *Conn {
	c := &Conn{
		RoomID: roomID, DeviceID: deviceID, DeviceSlug: deviceSlug, Ctx: ctx,
		send: make(chan ServerMessage, 64),
	}
	// Zero would read as "silent since the epoch" to the idle reaper.
	c.lastSeen.Store(time.Now().UnixNano())
	return c
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
	h := &Hub{
		seq: seq, rooms: rooms, previews: previews, borrows: borrows,
		log: log, conns: map[string]map[string]*Conn{},
	}
	go h.reapIdle()
	return h
}

// Client heartbeat policy: clients ping every 25s; a connection silent
// for three minutes is dead (or hostile) — platform TCP keepalives
// alone can take far longer, leaving MarkOffline unrun and the owner
// grace period stalled behind a half-open socket.
const idleReapAfter = 3 * time.Minute

// reapIdle force-closes silent sockets so their detach sequence runs.
func (h *Hub) reapIdle() {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for range t.C {
		cutoff := time.Now().Add(-idleReapAfter).UnixNano()
		h.mu.RLock()
		var stale []*Conn
		for _, room := range h.conns {
			for _, c := range room {
				if c.lastSeen.Load() < cutoff {
					stale = append(stale, c)
				}
			}
		}
		h.mu.RUnlock()
		for _, c := range stale {
			h.log.Warn("closing idle room socket", "room", c.RoomID, "device", c.DeviceID)
			if c.close != nil {
				c.close()
			}
		}
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
func (h *Hub) Attach(c *Conn, admit func() error) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	// Admission recheck INSIDE the registration lock: a kick between the
	// caller's MarkOnline and Attach deletes the membership while
	// DropDevice finds nothing registered to close; without this check
	// the kicked client attaches indefinitely and keeps submitting
	// operations. Kick (DropDevice) and registration serialize on h.mu,
	// so either the kick lands first (admission fails) or DropDevice
	// closes the just-attached connection.
	if admit != nil {
		if err := admit(); err != nil {
			return err
		}
	}
	if h.conns[c.RoomID] == nil {
		h.conns[c.RoomID] = map[string]*Conn{}
	}
	if old := h.conns[c.RoomID][c.DeviceID]; old != nil && old != c {
		// Superseded predecessor (reconnect raced the old socket's
		// exit): force its pump to return so its DETACH cannot evict
		// this replacement — an unconditional delete there would mute
		// the live socket (no broadcasts, no targeted messages) while
		// grace handling treats the device as gone.
		if old.close != nil {
			old.close()
		}
	}
	h.conns[c.RoomID][c.DeviceID] = c
	return nil
}

// Detach removes a connection and runs the disconnect path — but only
// when this connection still owns the registration: a replacement that
// attached first keeps its entry, its broadcasts, and its online state.
func (h *Hub) Detach(c *Conn) {
	h.mu.Lock()
	registered := h.conns[c.RoomID] != nil && h.conns[c.RoomID][c.DeviceID] == c
	if registered {
		delete(h.conns[c.RoomID], c.DeviceID)
		if len(h.conns[c.RoomID]) == 0 {
			delete(h.conns, c.RoomID)
		}
	}
	h.mu.Unlock()
	if !registered {
		// A REPLACEMENT socket owns the registration: dropping routes
		// now would withdraw the replacement's just-announced ports and
		// break .tutti resolution and relay authorization for the live
		// device until yet another reconnect.
		return
	}
	// Unexpected business-socket loss must withdraw this device's
	// announced routes immediately (leave and kick already do): stale
	// /routes entries kept resolving .tutti names to an offline device
	// until some explicit lifecycle event cleared them. Only after the
	// ownership check above — see the comment there.
	if h.previews != nil {
		h.previews.DropDevice(c.RoomID, c.DeviceID)
	}

	// c.Ctx is the cancelled read context on forced closures (queue
	// overflow, kicks) — an already-cancelled context would fail
	// MarkOffline and leave the membership stuck online, blocking
	// grace-period transfer and dissolution forever. Run the disconnect
	// write on a live context with a bounded timeout.
	offlineCtx, cancel := context.WithTimeout(context.WithoutCancel(c.Ctx), 5*time.Second)
	defer cancel()
	if _, err := h.rooms.MarkOffline(offlineCtx, c.RoomID, c.DeviceID); err != nil {
		h.log.Warn("mark offline", "room", c.RoomID, "device", c.DeviceID, "err", err)
	}
	h.BroadcastRoom(c.RoomID, vmprotocol.Event{
		Topic:   vmprotocol.TopicPresence,
		RoomID:  c.RoomID,
		Payload: mustJSON(vmprotocol.PresenceDevice{DeviceID: c.DeviceID, Online: false}),
	})
}

// Handle pumps one websocket until it closes. admit is the membership
// recheck executed inside Attach's registration lock.
func (h *Hub) Handle(c *Conn, ws *websocket.Conn, admit func() error) {
	h.pumping.Add(1)
	defer h.pumping.Done()
	// Force-close path (kick/membership revocation): cancelling the read
	// context unblocks the pump and runs the normal detach sequence.
	readCtx, cancelReads := context.WithCancel(c.Ctx)
	c.Ctx = readCtx
	c.close = cancelReads
	defer cancelReads()

	if err := h.Attach(c, admit); err != nil {
		ws.Close(websocket.StatusPolicyViolation, "membership revoked")
		return
	}
	defer h.Detach(c)
	defer ws.Close(websocket.StatusNormalClosure, "")

	writeCtx, cancelWrites := context.WithCancel(c.Ctx)
	defer cancelWrites()
	go func() {
		// The write context cancels when the pump returns (or the write
		// fails): without the Done select this goroutine would block on
		// the never-closed channel forever, leaking one goroutine +
		// channel + connection per disconnect.
		for {
			select {
			case msg, ok := <-c.send:
				if !ok {
					return
				}
				data, err := json.Marshal(msg)
				if err != nil {
					continue
				}
				if err := ws.Write(writeCtx, websocket.MessageText, data); err != nil {
					cancelWrites()
					return
				}
			case <-writeCtx.Done():
				return
			}
		}
	}()

	for {
		_, data, err := ws.Read(c.Ctx)
		if err != nil {
			return
		}
		c.lastSeen.Store(time.Now().UnixNano())
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
			// Membership recheck at the registry mutation point: the
			// pump may have read this frame before a kick/leave
			// deleted the membership and dropped the socket — an
			// Upsert racing DropDevice would resurrect the evicted
			// device's route (and gateways would keep binding an
			// unreachable endpoint until the room ends).
			if err := h.rooms.MembershipOf(c.Ctx, c.RoomID, c.DeviceID); err != nil {
				continue
			}
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
					Topic: borrowagent.TopicAgentShared, RoomID: c.RoomID, Payload: mustJSON(out),
				})
			} else {
				shared, revoked, err := h.borrows.Revoke(c.RoomID, c.DeviceID, p.AgentInstanceID)
				if err != nil {
					h.log.Warn("agent revoke", "room", c.RoomID, "err", err)
					continue
				}
				h.BroadcastRoom(c.RoomID, vmprotocol.Event{
					Topic: borrowagent.TopicAgentShared, RoomID: c.RoomID, Payload: mustJSON(shared),
				})
				h.BroadcastRoom(c.RoomID, vmprotocol.Event{
					Topic: borrowagent.TopicBorrowRevoked, RoomID: c.RoomID, Payload: mustJSON(revoked),
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
					Topic: borrowagent.TopicBorrowRevoked, RoomID: c.RoomID,
					Payload: mustJSON(borrowagent.BorrowRevokedPayload{
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
				Topic: borrowagent.TopicBorrowCommand, RoomID: c.RoomID, Payload: mustJSON(out),
			})
		case "approval_request":
			if msg.ApprovalRequest == nil {
				continue
			}
			p := *msg.ApprovalRequest
			// Only the agent's OWNER may open an approval: any
			// participant could otherwise fabricate prompt text and
			// choices for a shared agent they do not own and have the
			// borrower's decision routed to the real runtime.
			agent, _ := h.borrows.Agent(c.RoomID, p.AgentInstanceID)
			if agent == nil || agent.OwnerDeviceID != c.DeviceID {
				h.log.Warn("approval open by non-owner",
					"room", c.RoomID, "agent", p.AgentInstanceID, "sender", c.DeviceID)
				continue
			}
			operator, err := h.borrows.OpenApproval(c.RoomID, p.AgentInstanceID, p.ApprovalID, p.CommandID)
			if err != nil {
				h.log.Warn("approval open", "room", c.RoomID, "err", err)
				continue
			}
			p.SessionOperatorDeviceID = operator
			h.SendTo(c.RoomID, operator, vmprotocol.Event{
				Topic: borrowagent.TopicApprovalRequest, RoomID: c.RoomID, Payload: mustJSON(p),
			})
		case "approval_decision":
			if msg.ApprovalDecision == nil {
				continue
			}
			p := *msg.ApprovalDecision
			p.DeciderDeviceID = c.DeviceID
			owner, err := h.borrows.ResolveDecision(c.RoomID, p.AgentInstanceID, p.ApprovalID, c.DeviceID)
			if err != nil {
				h.log.Warn("approval decision", "room", c.RoomID, "err", err)
				continue
			}
			h.SendTo(c.RoomID, owner, vmprotocol.Event{
				Topic: borrowagent.TopicApprovalDecision, RoomID: c.RoomID, Payload: mustJSON(p),
			})
		case "policy":
			if msg.Policy != nil {
				// Succession readiness: only members reporting a full
				// replica may automatically inherit a lost owner.
				if err := h.rooms.ReportReplicaPolicy(c.Ctx, c.RoomID, c.DeviceID, msg.Policy.Policy); err != nil {
					h.log.Warn("policy report", "room", c.RoomID, "device", c.DeviceID, "err", err)
				}
			}
		case "ping":
			_ = h.rooms.MarkOnline(c.Ctx, c.RoomID, c.DeviceID)
			// The client's idle deadline counts RECEIVED frames: without
			// an answer a healthy-but-quiet room would cancel its socket
			// every idle window (reconnect/bootstrap churn). A bare
			// "pong" (not an event) keeps OnEvent streams clean.
			h.deliver(c, ServerMessage{Type: "pong"})
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

// PolicyReportPayload reports a member's replica policy ("full"|"lazy").
type PolicyReportPayload struct {
	Policy string `json:"policy"`
}

func mustJSON(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}
