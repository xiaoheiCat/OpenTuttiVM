// Package borrow is the server-side registry for Agent Borrowing: owners
// share agent instances into a room, borrowers command them, and execution
// stays on the owner's device. The registry enforces the locked rules:
// lease-generation fencing for revocation, BorrowSafe gating for sharing,
// and borrower-decided approvals.
package borrow

import (
	"errors"
	"fmt"
	"sync"

	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
)

// Errors surfaced to API/WS callers.
var (
	ErrNotOwner      = errors.New("only the agent owner may change sharing")
	ErrNotBorrowable = errors.New("agent does not satisfy the BorrowSafe contract")
	ErrUnknownAgent  = errors.New("agent instance not shared in this room")
	ErrStaleLease    = errors.New("borrowing lease revoked (stale generation)")
	ErrNotOperator   = errors.New("only the session operator may decide approvals")
)

// AgentInstance is one shared agent in one room.
type AgentInstance struct {
	ID              string
	OwnerDeviceID   string
	Provider        string
	Borrowable      bool
	BorrowSafety    string
	Capabilities    vmprotocol.AgentCapabilities
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
}

type openApproval struct {
	roomID     string
	agentOwner string
	operator   string
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{
		agents:    map[string]map[string]*AgentInstance{},
		approvals: map[string]openApproval{},
	}
}

// Share enables or disables borrowing for one agent instance. Only the
// owner may share; every share start bumps the lease generation so old
// commands cannot ride a re-share.
func (r *Registry) Share(roomID string, p vmprotocol.AgentSharedPayload) (vmprotocol.AgentSharedPayload, error) {
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
		inst.LastBorrower = existing.LastBorrower
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
func (r *Registry) Revoke(roomID, ownerDeviceID, agentInstanceID string) (vmprotocol.AgentSharedPayload, vmprotocol.BorrowRevokedPayload, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	room := r.agents[roomID]
	inst := room[agentInstanceID]
	if inst == nil {
		return vmprotocol.AgentSharedPayload{}, vmprotocol.BorrowRevokedPayload{}, ErrUnknownAgent
	}
	if inst.OwnerDeviceID != ownerDeviceID {
		return vmprotocol.AgentSharedPayload{}, vmprotocol.BorrowRevokedPayload{}, ErrNotOwner
	}
	inst.LeaseGeneration++
	inst.Shared = false
	inst.LastBorrower = ""
	shared := vmprotocol.AgentSharedPayload{
		AgentInstanceID: inst.ID, OwnerDeviceID: inst.OwnerDeviceID,
		Provider: inst.Provider, Borrowable: inst.Borrowable,
		BorrowSafety: inst.BorrowSafety, Capabilities: inst.Capabilities,
		LeaseGeneration: inst.LeaseGeneration, Shared: false,
	}
	revoked := vmprotocol.BorrowRevokedPayload{
		AgentInstanceID: inst.ID, Reason: "owner_revoked", FinalGeneration: inst.LeaseGeneration,
	}
	return shared, revoked, nil
}

// Command validates a borrower's command against the current lease and
// stamps the borrower identity. The caller routes the returned payload to
// the owning device.
func (r *Registry) Command(roomID string, p vmprotocol.BorrowCommandPayload) (vmprotocol.BorrowCommandPayload, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	room := r.agents[roomID]
	inst := room[p.AgentInstanceID]
	if inst == nil {
		return p, ErrUnknownAgent
	}
	// A known instance carrying a dead generation reports the revocation
	// specifically — the borrower learns their lease is gone, not that the
	// agent never existed.
	if p.LeaseGeneration != inst.LeaseGeneration {
		return p, ErrStaleLease
	}
	if !inst.Shared {
		return p, ErrUnknownAgent
	}
	// BorrowerDeviceID was stamped by the hub from the authenticated
	// connection before this call; recording it makes this device the
	// session operator for subsequent approvals.
	inst.LastBorrower = p.BorrowerDeviceID
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
// current session operator (the borrower) — the owner never receives it.
// Returns the operator device id for targeted delivery.
func (r *Registry) OpenApproval(roomID, agentInstanceID, approvalID string) (operator string, err error) {
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
	r.approvals[approvalID] = openApproval{
		roomID: roomID, agentOwner: inst.OwnerDeviceID, operator: inst.LastBorrower,
	}
	return inst.LastBorrower, nil
}

// ResolveDecision validates that the deciding device is the session
// operator and returns the owning device for targeted routing.
func (r *Registry) ResolveDecision(approvalID, deciderDeviceID string) (ownerDeviceID string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ap, ok := r.approvals[approvalID]
	if !ok {
		return "", errors.New("unknown or expired approval")
	}
	if ap.operator != deciderDeviceID {
		return "", ErrNotOperator
	}
	delete(r.approvals, approvalID)
	return ap.agentOwner, nil
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
}
