// Package borrow is the server-side registry for Agent Borrowing: owners
// share agent instances into a room, borrowers command them, and execution
// stays on the owner's device. The registry enforces the locked rules:
// lease-generation fencing for revocation, BorrowSafe gating for sharing,
// and borrower-decided approvals.
package borrow

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	borrowagent "github.com/xiaoheiCat/OpenTuttiVM/packages/agent/borrow"
)

// Errors surfaced to API/WS callers.
var (
	ErrNotOwner            = errors.New("only the agent owner may change sharing")
	ErrNotBorrowable       = errors.New("agent does not satisfy the BorrowSafe contract")
	ErrUnknownAgent        = errors.New("agent instance not shared in this room")
	ErrStaleLease          = errors.New("borrowing lease revoked (stale generation)")
	ErrNotOperator         = errors.New("only the session operator may decide approvals")
	ErrCommandFailedOwner  = errors.New("only the owning device may report command failure")
	ErrDeliveryUnavailable = errors.New("borrow delivery unavailable")
	ErrDuplicateCommand    = errors.New("borrow command id already used with different payload")
	ErrInvalidChoice       = errors.New("approval choice is not one of the advertised options")
)

// AgentInstance is one shared agent in one room.
type AgentInstance struct {
	ID              string
	OwnerDeviceID   string
	Provider        string
	Borrowable      bool
	BorrowSafety    string
	Capabilities    borrowagent.AgentCapabilities
	LeaseGeneration uint64
	Shared          bool
	// LastBorrower is the current session operator: the device whose
	// command is executing. Approvals route to them, never to the owner.
	LastBorrower string
}

// Registry tracks shared agents per room. All methods are safe for
// concurrent use: independent room sockets mutate the registry in
// parallel.
type Registry struct {
	mu        sync.Mutex
	agents    map[string]map[string]*AgentInstance
	approvals map[string]openApproval
	// commandBorrowers remembers which borrower issued a command id, so
	// approval prompts route to the borrower that originated the
	// execution even after other borrowers send their own commands.
	// Bounded FIFO: only recent commands can still be executing.
	commandBorrowers map[string]commandRecord
	// commandOrder bounds the tracked commands PER ROOM+AGENT: a global
	// FIFO let 65 unrelated commands evict a still-executing command's
	// mapping, so its later approval prompt failed closed and the
	// operator never saw it.
	commandOrder map[string][]string
}

type openApproval struct {
	roomID     string
	agentOwner string
	agentID    string
	operator   string
	generation uint64
	openedAt   time.Time
	options    []string
}

type commandRecord struct {
	payload  borrowagent.BorrowCommandPayload
	borrower string
	delivery commandDeliveryState
}

type commandDeliveryState uint8

const (
	commandQueued commandDeliveryState = iota + 1
	commandDelivered
)

// maxTrackedCommandsPerAgent bounds one room+agent's tracked commands:
// prompts only arrive for commands still executing on that agent.
const maxTrackedCommandsPerAgent = 16

// maxOutstandingApprovals bounds one agent's pending approvals, and
// approvalTTL expires them even without a resolution (interactive
// prompts are short-lived; nothing else sweeps them).
const (
	maxOutstandingApprovals = 32
	approvalTTL             = 10 * time.Minute
)

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{
		agents:           map[string]map[string]*AgentInstance{},
		approvals:        map[string]openApproval{},
		commandBorrowers: map[string]commandRecord{},
		commandOrder:     map[string][]string{},
	}
}

// Share enables or disables borrowing for one agent instance. Only the
// owner may share; every share start bumps the lease generation so old
// commands cannot ride a re-share.
// maxAgentsPerDevice bounds one owner's concurrently registered shared
// agents in a room.
const maxAgentsPerDevice = 32

func (r *Registry) Share(roomID string, p borrowagent.AgentSharedPayload) (borrowagent.AgentSharedPayload, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if p.OwnerDeviceID == "" || p.AgentInstanceID == "" {
		return p, fmt.Errorf("owner_device_id and agent_instance_id required")
	}
	room := r.agents[roomID]
	if room == nil {
		room = map[string]*AgentInstance{}
		r.agents[roomID] = room
	}
	// Per-owner quota mirrors maxRoutesPerDevice: without it an owner
	// cycling unique agent ids retained unbounded registry entries
	// (and broadcast echoes) until dissolution.
	if room[p.AgentInstanceID] == nil {
		owned := 0
		for _, a := range room {
			if a.OwnerDeviceID == p.OwnerDeviceID {
				owned++
			}
		}
		if owned >= maxAgentsPerDevice {
			return p, fmt.Errorf("device shares at most %d agents", maxAgentsPerDevice)
		}
	}
	existing := room[p.AgentInstanceID]
	if existing != nil && existing.OwnerDeviceID != p.OwnerDeviceID {
		return p, ErrNotOwner
	}
	if p.Shared && !p.Borrowable {
		// Locked principle: unsafe adapters stay self-usable but never
		// borrowable — no lowering the safety boundary.
		return p, ErrNotBorrowable
	}
	inst := &AgentInstance{
		ID: p.AgentInstanceID, OwnerDeviceID: p.OwnerDeviceID,
		Provider: p.Provider, Borrowable: p.Borrowable,
		BorrowSafety: p.BorrowSafety, Capabilities: p.Capabilities,
		Shared: p.Shared,
	}
	if existing != nil {
		inst.LeaseGeneration = existing.LeaseGeneration + 1
		// A re-share starts a NEW lease: carrying the previous
		// generation's operator, command mappings, and approvals would
		// let an old-generation borrower keep receiving and deciding
		// approvals after its lease was fenced — the same cleanup
		// revocation performs.
		inst.LastBorrower = ""
		for key, ap := range r.approvals {
			if ap.roomID == roomID && keyAgent(key) == p.AgentInstanceID {
				delete(r.approvals, key)
			}
		}
		for key := range r.commandBorrowers {
			if k := strings.SplitN(key, "\x00", 3); len(k) == 3 && k[0] == roomID && k[1] == p.AgentInstanceID {
				delete(r.commandBorrowers, key)
			}
		}
	} else {
		inst.LeaseGeneration = 1
	}
	room[p.AgentInstanceID] = inst
	p.LeaseGeneration = inst.LeaseGeneration
	return p, nil
}

// Revoke ends borrowing: the generation bumps, and every in-flight command
// against the old generation becomes invalid. Returns the broadcast
// payloads (agent.shared with shared=false plus revocation details).
func (r *Registry) Revoke(roomID, ownerDeviceID, agentInstanceID string) (borrowagent.AgentSharedPayload, borrowagent.BorrowRevokedPayload, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	room := r.agents[roomID]
	inst := room[agentInstanceID]
	if inst == nil {
		return borrowagent.AgentSharedPayload{}, borrowagent.BorrowRevokedPayload{}, ErrUnknownAgent
	}
	if inst.OwnerDeviceID != ownerDeviceID {
		return borrowagent.AgentSharedPayload{}, borrowagent.BorrowRevokedPayload{}, ErrNotOwner
	}
	inst.LeaseGeneration++
	inst.Shared = false
	inst.LastBorrower = ""
	// The revoked generation's bookkeeping dies with the lease: a
	// decision submitted for an approval opened before revocation must
	// not still route to the owner, and stale command mappings must not
	// keep steering approval prompts.
	for key, ap := range r.approvals {
		if ap.roomID == roomID && keyAgent(key) == agentInstanceID {
			delete(r.approvals, key)
		}
	}
	for key := range r.commandBorrowers {
		if k := strings.SplitN(key, "\x00", 3); len(k) == 3 && k[0] == roomID && k[1] == agentInstanceID {
			delete(r.commandBorrowers, key)
		}
	}
	shared := borrowagent.AgentSharedPayload{
		AgentInstanceID: inst.ID, OwnerDeviceID: inst.OwnerDeviceID,
		Provider: inst.Provider, Borrowable: inst.Borrowable,
		BorrowSafety: inst.BorrowSafety, Capabilities: inst.Capabilities,
		LeaseGeneration: inst.LeaseGeneration, Shared: false,
	}
	revoked := borrowagent.BorrowRevokedPayload{
		AgentInstanceID: inst.ID, Reason: "owner_revoked", FinalGeneration: inst.LeaseGeneration,
	}
	return shared, revoked, nil
}

// Command validates a borrower's command against the current lease and
// stamps the borrower identity. The caller routes the returned payload to
// the owning device.
func (r *Registry) Command(roomID string, p borrowagent.BorrowCommandPayload) (borrowagent.BorrowCommandPayload, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out, _, err := r.commandLocked(roomID, p)
	return out, err
}

func (r *Registry) commandLocked(roomID string, p borrowagent.BorrowCommandPayload) (borrowagent.BorrowCommandPayload, bool, error) {
	room := r.agents[roomID]
	inst := room[p.AgentInstanceID]
	if inst == nil {
		return p, false, ErrUnknownAgent
	}
	// A known instance carrying a dead generation reports the revocation
	// specifically — the borrower learns their lease is gone, not that the
	// agent never existed.
	if p.LeaseGeneration != inst.LeaseGeneration {
		return p, false, ErrStaleLease
	}
	if !inst.Shared {
		return p, false, ErrUnknownAgent
	}
	if p.CommandID != "" {
		key := roomID + "\x00" + p.AgentInstanceID + "\x00" + p.CommandID
		if previous, ok := r.commandBorrowers[key]; ok {
			if previous.payload.Input != p.Input || previous.payload.AgentInstanceID != p.AgentInstanceID || previous.payload.LeaseGeneration != p.LeaseGeneration || previous.borrower != p.BorrowerDeviceID {
				return p, false, ErrDuplicateCommand
			}
			return previous.payload, false, nil
		}
	}
	// BorrowerDeviceID was stamped by the hub from the authenticated
	// connection before this call; recording it makes this device the
	// session operator for subsequent approvals.
	inst.LastBorrower = p.BorrowerDeviceID
	// Track the originating borrower per command, keyed by
	// room+agent+command: provider-local command ids collide across
	// agents and rooms, and a global key would let one command's entry
	// overwrite another's borrower. Prompts arriving mid-execution route
	// to THIS borrower, not to whoever commanded most recently. Bounded
	// FIFO: only recent commands can still be executing.
	if p.CommandID != "" {
		agentKey := roomID + "\x00" + p.AgentInstanceID
		key := agentKey + "\x00" + p.CommandID
		if _, exists := r.commandBorrowers[key]; !exists {
			order := append(r.commandOrder[agentKey], key)
			if len(order) > maxTrackedCommandsPerAgent {
				delete(r.commandBorrowers, order[0])
				order = order[1:]
			}
			r.commandOrder[agentKey] = order
		}
		r.commandBorrowers[key] = commandRecord{payload: p, borrower: p.BorrowerDeviceID, delivery: commandQueued}
	}
	return p, true, nil
}

// DispatchCommand validates and records a command while holding the lease
// lock, then invokes the bounded enqueue callback before releasing it. The
// callback must not perform socket I/O; its generation is the final delivery
// fence used by the writer.
func (r *Registry) DispatchCommand(roomID string, p borrowagent.BorrowCommandPayload, deliver func(string, uint64, borrowagent.BorrowCommandPayload) bool) error {
	r.mu.Lock()
	out, fresh, err := r.commandLocked(roomID, p)
	if err != nil {
		r.mu.Unlock()
		return err
	}
	inst := r.agents[roomID][p.AgentInstanceID]
	owner, generation := inst.OwnerDeviceID, inst.LeaseGeneration
	r.mu.Unlock()
	if !fresh {
		return nil
	}
	if !deliver(owner, generation, out) {
		r.mu.Lock()
		key := roomID + "\x00" + p.AgentInstanceID + "\x00" + p.CommandID
		if record, ok := r.commandBorrowers[key]; ok && record.delivery == commandQueued && record.payload.LeaseGeneration == generation {
			delete(r.commandBorrowers, key)
		}
		r.mu.Unlock()
		return ErrDeliveryUnavailable
	}
	r.mu.Lock()
	key := roomID + "\x00" + p.AgentInstanceID + "\x00" + p.CommandID
	if record, ok := r.commandBorrowers[key]; ok && record.delivery == commandQueued && record.payload.LeaseGeneration == generation {
		record.delivery = commandDelivered
		r.commandBorrowers[key] = record
	}
	r.mu.Unlock()
	return nil
}

func (r *Registry) CommandFailed(roomID, ownerDeviceID string, p borrowagent.CommandFailedPayload) (borrowagent.CommandFailedPayload, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	inst := r.agents[roomID][p.AgentInstanceID]
	if inst == nil {
		return p, ErrUnknownAgent
	}
	if inst.OwnerDeviceID != ownerDeviceID {
		return p, ErrCommandFailedOwner
	}
	if p.LeaseGeneration != inst.LeaseGeneration {
		return p, ErrStaleLease
	}
	record, ok := r.commandBorrowers[roomID+"\x00"+p.AgentInstanceID+"\x00"+p.CommandID]
	if !ok {
		return p, ErrUnknownAgent
	}
	p.BorrowerDeviceID = record.borrower
	return p, nil
}

// CurrentOperator returns the current session operator (borrower) of an
// agent instance; approvals route there.
func (r *Registry) CurrentOperator(roomID, agentInstanceID string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	room := r.agents[roomID]
	inst := room[agentInstanceID]
	if inst == nil {
		return "", ErrUnknownAgent
	}
	if inst.LastBorrower == "" {
		return "", errors.New("no active borrowing session")
	}
	return inst.LastBorrower, nil
}

// OpenApproval records a pending approval prompt and routes it to the
// borrower that originated the referenced command — the owner never
// receives it. A known commandID wins over LastBorrower so a second
// borrower's fresh command cannot steal the first execution's prompt; a
// legacy empty commandID falls back to the current session operator.
// Returns the operator device id for targeted delivery.
func (r *Registry) OpenApproval(roomID, agentInstanceID, approvalID, commandID string, options []string) (operator string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.openApprovalLocked(roomID, agentInstanceID, approvalID, commandID, options)
}

func (r *Registry) openApprovalLocked(roomID, agentInstanceID, approvalID, commandID string, options []string) (operator string, err error) {
	room := r.agents[roomID]
	inst := room[agentInstanceID]
	if inst == nil {
		return "", ErrUnknownAgent
	}
	operator = inst.LastBorrower
	if commandID != "" {
		if record, ok := r.commandBorrowers[roomID+"\x00"+agentInstanceID+"\x00"+commandID]; ok {
			operator = record.borrower
		} else {
			// Unknown NONEMPTY command id fails closed: the mapping may
			// have aged out of the bounded FIFO while another borrower
			// took over the agent, and the LastBorrower fallback would
			// route this prompt (and its decision authority) to the
			// wrong participant. Only legacy empty ids fall back.
			return "", errors.New("unknown borrow command for approval routing")
		}
	}
	if operator == "" {
		return "", errors.New("no active borrowing session")
	}
	// Scoped key: provider-local approval ids collide across rooms and
	// agent runtimes, and a process-global key would let one room's
	// prompt overwrite another's (decisions then rejected, or worse,
	// consumed by the wrong room's approval).
	// Bound the outstanding set: approvals expire a fixed window after
	// they open (the advertised deadline has no cross-goroutine sweep,
	// so expiry piggybacks here and on resolution), and one agent never
	// holds more than maxOutstandingApprovals pending ids — an
	// authenticated owner could otherwise pin unbounded ids in the heap
	// (the socket accepts ~49 MiB frames).
	now := time.Now()
	for key, ap := range r.approvals {
		if now.Sub(ap.openedAt) > approvalTTL {
			delete(r.approvals, key)
		}
	}
	if _, exists := r.approvals[approvalScope(roomID, agentInstanceID, approvalID)]; !exists {
		open := 0
		for _, ap := range r.approvals {
			if ap.roomID == roomID && ap.agentOwner == inst.OwnerDeviceID && ap.agentID == agentInstanceID {
				open++
			}
		}
		if open >= maxOutstandingApprovals {
			// Evict the OLDEST pending approval of this agent: prompts
			// are interactive and short-lived; a flood must not wedge
			// the registry.
			var oldestKey string
			var oldestAt time.Time
			for key, ap := range r.approvals {
				if ap.roomID == roomID && ap.agentOwner == inst.OwnerDeviceID && ap.agentID == agentInstanceID {
					if oldestKey == "" || ap.openedAt.Before(oldestAt) {
						oldestKey, oldestAt = key, ap.openedAt
					}
				}
			}
			if oldestKey != "" {
				delete(r.approvals, oldestKey)
			}
		}
	}
	r.approvals[approvalScope(roomID, agentInstanceID, approvalID)] = openApproval{
		roomID: roomID, agentOwner: inst.OwnerDeviceID, agentID: agentInstanceID, operator: operator,
		generation: inst.LeaseGeneration,
		openedAt:   now,
		options:    append([]string(nil), options...),
	}
	return operator, nil
}

// DispatchApproval validates and records a prompt under the lease lock, then
// invokes a bounded enqueue callback carrying its generation. The callback
// must not perform socket I/O.
func (r *Registry) DispatchApproval(roomID, agentInstanceID, approvalID, commandID string, options []string, deliver func(string, uint64) bool) error {
	r.mu.Lock()
	operator, err := r.openApprovalLocked(roomID, agentInstanceID, approvalID, commandID, options)
	if err != nil {
		r.mu.Unlock()
		return err
	}
	generation := r.agents[roomID][agentInstanceID].LeaseGeneration
	apKey := approvalScope(roomID, agentInstanceID, approvalID)
	ap := r.approvals[apKey]
	r.mu.Unlock()
	if !deliver(operator, generation) {
		r.mu.Lock()
		if inst := r.agents[roomID][agentInstanceID]; inst != nil && inst.LeaseGeneration == generation && inst.Shared {
			r.approvals[apKey] = ap
		}
		r.mu.Unlock()
		return ErrDeliveryUnavailable
	}
	return nil
}

// approvalScope keys pending approvals by room, agent instance, and the
// provider-local approval id.
func approvalScope(roomID, agentInstanceID, approvalID string) string {
	return roomID + "\x00" + agentInstanceID + "\x00" + approvalID
}

// keyAgent extracts the agent-instance segment of an approval key.
func keyAgent(scope string) string {
	parts := strings.SplitN(scope, "\x00", 3)
	if len(parts) != 3 {
		return ""
	}
	return parts[1]
}

// ResolveDecision validates that the deciding device is the session
// operator and returns the owning device for targeted routing. The scope
// (room, agent instance) disambiguates provider-local approval ids.
func (r *Registry) ResolveDecision(roomID, agentInstanceID, approvalID, deciderDeviceID string, choice int) (ownerDeviceID string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ap, ok := r.approvals[approvalScope(roomID, agentInstanceID, approvalID)]
	if !ok {
		return "", errors.New("unknown or expired approval")
	}
	if ap.operator != deciderDeviceID {
		return "", ErrNotOperator
	}
	if choice != -1 && (choice < 0 || choice >= len(ap.options)) {
		return "", ErrInvalidChoice
	}
	delete(r.approvals, approvalScope(roomID, agentInstanceID, approvalID))
	return ap.agentOwner, nil
}

// ResolveDecisionDispatch consumes a decision under the registry lock and
// passes a generation-stamped delivery to a bounded, nonblocking enqueue.
func (r *Registry) ResolveDecisionDispatch(roomID, agentInstanceID, approvalID, deciderDeviceID string, choice int, deliver func(string, uint64) bool) error {
	r.mu.Lock()
	ap, ok := r.approvals[approvalScope(roomID, agentInstanceID, approvalID)]
	if !ok {
		r.mu.Unlock()
		return errors.New("unknown or expired approval")
	}
	if ap.operator != deciderDeviceID {
		r.mu.Unlock()
		return ErrNotOperator
	}
	if choice != -1 && (choice < 0 || choice >= len(ap.options)) {
		r.mu.Unlock()
		return ErrInvalidChoice
	}
	delete(r.approvals, approvalScope(roomID, agentInstanceID, approvalID))
	owner, generation := ap.agentOwner, ap.generation
	r.mu.Unlock()
	if !deliver(owner, generation) {
		r.mu.Lock()
		if inst := r.agents[roomID][agentInstanceID]; inst != nil && inst.LeaseGeneration == generation && inst.Shared {
			r.approvals[approvalScope(roomID, agentInstanceID, approvalID)] = ap
		}
		r.mu.Unlock()
		return ErrDeliveryUnavailable
	}
	return nil
}

// ValidateDelivery is the final fence for a message admitted while the
// registry lock was held. The writer calls it after dequeue, so revocation can
// invalidate queued work without making the registry lock cover socket I/O.
func (r *Registry) ValidateDelivery(roomID, agentInstanceID, approvalID, recipient string, generation uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	inst := r.agents[roomID][agentInstanceID]
	if inst == nil || !inst.Shared || inst.LeaseGeneration != generation {
		return false
	}
	// Approval decisions consume the pending approval before the prompt may
	// leave a slow queue. Generation fencing is sufficient here: the queued
	// recipient is the authenticated connection selected when the prompt was
	// opened, while revoke/re-share always changes the generation.
	if approvalID != "" {
		return true
	}
	return inst.OwnerDeviceID == recipient
}

// Agent returns one instance (status/testing).
func (r *Registry) Agent(roomID, agentInstanceID string) (*AgentInstance, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	inst := r.agents[roomID][agentInstanceID]
	return inst, inst != nil
}

// ClearRoom drops all shared agents and pending approvals for a room
// (dissolution).
func (r *Registry) ClearRoom(roomID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.agents, roomID)
	for id, ap := range r.approvals {
		if ap.roomID == roomID {
			delete(r.approvals, id)
		}
	}
	// Command routing dies with the room too: room ids are never
	// reused and nothing else sweeps these maps, so dissolved rooms
	// permanently retained their per-agent command history on a
	// long-running server under normal room churn.
	prefix := roomID + "\x00"
	for key := range r.commandBorrowers {
		if strings.HasPrefix(key, prefix) {
			delete(r.commandBorrowers, key)
		}
	}
	for key := range r.commandOrder {
		if strings.HasPrefix(key, prefix) {
			delete(r.commandOrder, key)
		}
	}
}

// DropDevice removes every agent a departing device owns — kick or
// leave — and returns one revocation payload per shared agent so the
// caller can broadcast them. Without this, remaining members could keep
// commanding agents whose owner is gone, and ghost approvals/operators
// would persist until the whole room dissolved.
func (r *Registry) DropDevice(roomID, ownerDeviceID string) []borrowagent.BorrowRevokedPayload {
	r.mu.Lock()
	defer r.mu.Unlock()
	var revoked []borrowagent.BorrowRevokedPayload
	for id, inst := range r.agents[roomID] {
		if inst.OwnerDeviceID != ownerDeviceID {
			// The departing device may be the BORROWER operating this
			// agent: stale LastBorrower/command mappings kept routing
			// later prompts to an offline non-member connection, and a
			// running command stayed blocked with nobody to decide.
			if inst.LastBorrower == ownerDeviceID {
				inst.LastBorrower = ""
			}
			continue
		}
		inst.LeaseGeneration++
		inst.Shared = false
		inst.LastBorrower = ""
		revoked = append(revoked, borrowagent.BorrowRevokedPayload{
			AgentInstanceID: inst.ID, Reason: "owner_left", FinalGeneration: inst.LeaseGeneration,
		})
		delete(r.agents[roomID], id)
	}
	for key, ap := range r.approvals {
		// The OPERATOR's pending approvals die with it too: an
		// already-read decision frame can race socket cancellation, and
		// a rejoining device must not resolve a stale approval from
		// before its revocation — the decision would be forwarded to
		// the agent owner after membership is gone.
		if ap.roomID == roomID && (ap.agentOwner == ownerDeviceID || ap.operator == ownerDeviceID) {
			delete(r.approvals, key)
		}
	}
	prefix := roomID + "\x00"
	for key, record := range r.commandBorrowers {
		if strings.HasPrefix(key, prefix) && record.borrower == ownerDeviceID {
			delete(r.commandBorrowers, key)
		}
	}
	for key := range r.commandBorrowers {
		// key = roomID \x00 agentInstanceID \x00 commandID
		parts := strings.SplitN(key, "\x00", 3)
		if len(parts) == 3 && parts[0] == roomID {
			if inst := r.agents[roomID][parts[1]]; inst == nil || inst.OwnerDeviceID == ownerDeviceID {
				delete(r.commandBorrowers, key)
			}
		}
	}
	return revoked
}
