// Package borrow is the Agent Borrowing contract: owners share agent
// instances into a room, borrowers command them, execution stays on the
// owner's device (their open-tutti-vm-<roomId>), and streams, terminal
// output, and file changes flow back through the room so everyone sees
// them live. The server never holds provider credentials and never runs
// agent code.
//
// The payloads and topics live in their own seam (not vm-protocol) so
// the workspace synchronization contract stays narrow; the event ENVELOPE
// itself (vmprotocol.Event) is shared transport.
package borrow

import vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"

// Borrowing topics on the room event bus.
const (
	// TopicAgentShared announces an owner enabled or disabled borrowing
	// for one agent instance.
	TopicAgentShared vmprotocol.EventTopic = "agent.shared"
	// TopicBorrowCommand routes a borrower's command to the owning
	// device.
	TopicBorrowCommand vmprotocol.EventTopic = "agent.borrow_command"
	// TopicBorrowRevoked tells holders of an old lease generation that
	// borrowing ended; they must stop accepting borrower input.
	TopicBorrowRevoked vmprotocol.EventTopic = "agent.borrow_revoked"
	// TopicApprovalRequest routes a permission prompt to the current
	// borrower (the session operator decides, never the owner).
	TopicApprovalRequest vmprotocol.EventTopic = "agent.approval_request"
	// TopicApprovalDecision carries the borrower's choice back to the
	// owning device.
	TopicApprovalDecision vmprotocol.EventTopic = "agent.approval_decision"
)

// AgentSharedPayload is the payload of agent.shared. Sharing is scoped to
// one room and fenced by a lease generation: revocation bumps the
// generation, instantly invalidating every in-flight command issued
// against the old one.
type AgentSharedPayload struct {
	// AgentInstanceID identifies the shared agent (provider + owner).
	AgentInstanceID string `json:"agent_instance_id"`
	OwnerDeviceID   string `json:"owner_device_id"`
	// Provider is the agent kind, e.g. "claude-code", "codex".
	Provider string `json:"provider"`
	// Borrowable reports whether the adapter satisfied the BorrowSafe
	// contract; non-isolated providers stay self-usable but cannot be
	// shared.
	Borrowable bool `json:"borrowable"`
	// BorrowSafety notes the isolation verdict for UI display.
	BorrowSafety string `json:"borrow_safety,omitempty"`
	// Capabilities lists what comes with the agent: file-only skills are
	// injected read-only into sessions; MCP servers and tools execute on
	// the owner device through its Capability Broker.
	Capabilities AgentCapabilities `json:"capabilities"`
	// LeaseGeneration fences borrowing commands.
	LeaseGeneration uint64 `json:"lease_generation"`
	Shared          bool   `json:"shared"`
}

// AgentCapabilities describes the abilities an agent brings into a room.
type AgentCapabilities struct {
	Skills []string `json:"skills,omitempty"`
	MCP    []string `json:"mcp,omitempty"`
	Tools  []string `json:"tools,omitempty"`
}

// BorrowCommandPayload is the payload of agent.borrow_command: a borrower's
// instruction routed to the owning device. The server stamps the borrower
// identity and rejects commands carrying a stale lease generation.
type BorrowCommandPayload struct {
	CommandID       string `json:"command_id"`
	AgentInstanceID string `json:"agent_instance_id"`
	// BorrowerDeviceID is stamped by the server from the authenticated
	// connection, never trusted from the wire.
	BorrowerDeviceID string `json:"borrower_device_id"`
	// LeaseGeneration must match the current generation; stale commands
	// are dropped as revoked.
	LeaseGeneration uint64 `json:"lease_generation"`
	Input           string `json:"input"`
}

// BorrowRevokedPayload announces borrowing ended (owner revoked, agent left
// the room, or the room is ending); holders of the old generation must stop
// accepting borrower input.
type BorrowRevokedPayload struct {
	AgentInstanceID string `json:"agent_instance_id"`
	Reason          string `json:"reason"`
	FinalGeneration uint64 `json:"final_generation"`
}

// ApprovalRequestPayload is the payload of agent.approval_request: a
// permission prompt from an executing agent. Per the locked rules the
// CURRENT BORROWER decides — the owner is a capability provider, not the
// session operator, and receives no prompt.
type ApprovalRequestPayload struct {
	ApprovalID      string `json:"approval_id"`
	AgentInstanceID string `json:"agent_instance_id"`
	// CommandID identifies the borrow command whose execution raised
	// this prompt: approvals route to THAT command's borrower, so a
	// second borrower's command can never steal the first one's prompt.
	// The owner-side runtime echoes it from the executing command.
	CommandID string `json:"command_id,omitempty"`
	// SessionOperatorDeviceID is the borrower who decides.
	SessionOperatorDeviceID string `json:"session_operator_device_id"`
	Provider                string `json:"provider"`
	Prompt                  string `json:"prompt"`
	// Options are the provider's offered choices (e.g. allow once / always
	// / deny), index-aligned with the decision's choice.
	Options []string `json:"options,omitempty"`
	// DeadlineMS is the unix-millisecond deadline after which the
	// executing agent interrupts itself (5 minutes per the locked rule).
	DeadlineMS int64 `json:"deadline_ms"`
}

// ApprovalDecisionPayload is the payload of agent.approval_decision routed
// back to the owning device. Choice indexes the request's Options; -1 means
// timeout/dismissed.
type ApprovalDecisionPayload struct {
	ApprovalID string `json:"approval_id"`
	// AgentInstanceID scopes the decision: provider-local approval ids
	// collide across rooms and agents, and the server keys pending
	// approvals by room+agent+id.
	AgentInstanceID string `json:"agent_instance_id,omitempty"`
	// DeciderDeviceID is stamped by the server from the authenticated
	// connection and must equal the request's session operator.
	DeciderDeviceID string `json:"decider_device_id"`
	Choice          int    `json:"choice"`
}
