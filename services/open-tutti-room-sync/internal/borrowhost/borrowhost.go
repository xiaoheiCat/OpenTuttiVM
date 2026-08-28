// Package borrowhost is the owning-device side of Agent Borrowing: the
// events the server routes here (a borrower's command, permission
// prompts and decisions, share/revocation notices) delegate to the host
// application for execution against the real agent runtime.
//
// The room-sync container itself never runs agent code — per the room
// model, agents execute on each user's own device through the host
// (Tutti's Agent Host owns session/turn lifecycle). Hosts inject their
// adapter at startup; Noop logs and acks nothing, which keeps the
// server's routing observable while a host is absent.
package borrowhost

import (
	"log/slog"

	vmagent "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-agent"
)

// Host executes borrowed-agent lifecycle work on the owning device.
// Implementations live in the host application (Agent Host adapter);
// every method may be called from the WS event loop.
type Host interface {
	// ExecuteCommand runs one borrower instruction against the shared
	// agent instance; results stream back through the host's own
	// channels (terminal/file operations), not this return value.
	ExecuteCommand(p vmagent.BorrowCommandPayload) error
	// ApprovalRequest surfaces a permission prompt raised during a
	// borrowed execution to the OWNER's UI? No — the server routes
	// prompts to the borrower; the owner receives nothing. This lands
	// here only for owner-authored sessions.
	ApprovalRequest(p vmagent.ApprovalRequestPayload) error
	// ApprovalDecision resumes a borrowed execution that was waiting on
	// the borrower's choice.
	ApprovalDecision(p vmagent.ApprovalDecisionPayload) error
	// Shared reflects this device's own share-state changes.
	Shared(p vmagent.AgentSharedPayload) error
	// Revoked ends every in-flight command holding the old generation.
	Revoked(p vmagent.BorrowRevokedPayload) error
}

// Noop is the default host: it observes routed events without executing
// anything, so deployments without a host adapter still see the traffic
// (and revocations still fence local state) in logs.
type Noop struct{ Log *slog.Logger }

func (n *Noop) log(what string, v ...any) {
	if n.Log != nil {
		n.Log.Info("borrowhost: no host adapter; event observed", append([]any{"what", what}, v...)...)
	}
}

// ExecuteCommand implements Host.
func (n *Noop) ExecuteCommand(p vmagent.BorrowCommandPayload) error {
	n.log("borrow_command", "agent", p.AgentInstanceID, "command", p.CommandID)
	return nil
}

// ApprovalRequest implements Host.
func (n *Noop) ApprovalRequest(p vmagent.ApprovalRequestPayload) error {
	n.log("approval_request", "approval", p.ApprovalID)
	return nil
}

// ApprovalDecision implements Host.
func (n *Noop) ApprovalDecision(p vmagent.ApprovalDecisionPayload) error {
	n.log("approval_decision", "approval", p.ApprovalID, "choice", p.Choice)
	return nil
}

// Shared implements Host.
func (n *Noop) Shared(p vmagent.AgentSharedPayload) error {
	n.log("agent_shared", "agent", p.AgentInstanceID, "shared", p.Shared)
	return nil
}

// Revoked implements Host.
func (n *Noop) Revoked(p vmagent.BorrowRevokedPayload) error {
	n.log("borrow_revoked", "agent", p.AgentInstanceID, "generation", p.FinalGeneration)
	return nil
}
