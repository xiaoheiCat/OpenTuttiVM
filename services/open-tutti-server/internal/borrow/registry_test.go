package borrow

import (
	"errors"
	"testing"

	vmagent "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-agent"
	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
)

func shareClaude(t *testing.T, r *Registry) vmagent.AgentSharedPayload {
	t.Helper()
	p := vmagent.AgentSharedPayload{
		AgentInstanceID: "agent-claude-1", OwnerDeviceID: "dev_alice",
		Provider: "claude-code", Borrowable: true, Shared: true,
		Capabilities: vmagent.AgentCapabilities{
			Skills: []string{"repo-walk"}, MCP: []string{"github"}, Tools: []string{"bash"},
		},
	}
	out, err := r.Share("room1", p)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestShareRejectsUnsafeAdapter(t *testing.T) {
	r := NewRegistry()
	p := vmagent.AgentSharedPayload{
		AgentInstanceID: "agent-x", OwnerDeviceID: "dev_alice",
		Provider: "claude-code", Borrowable: false, Shared: true,
		BorrowSafety: "BorrowSafe isolation unavailable",
	}
	if _, err := r.Share("room1", p); !errors.Is(err, ErrNotBorrowable) {
		t.Fatalf("unsafe share err = %v", err)
	}
}

func TestOnlyOwnerMayShareOrRevoke(t *testing.T) {
	r := NewRegistry()
	shareClaude(t, r)
	p := vmagent.AgentSharedPayload{
		AgentInstanceID: "agent-claude-1", OwnerDeviceID: "dev_mallory",
		Provider: "claude-code", Borrowable: true, Shared: true,
	}
	if _, err := r.Share("room1", p); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("hijack share err = %v", err)
	}
	if _, _, err := r.Revoke("room1", "dev_mallory", "agent-claude-1"); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("hijack revoke err = %v", err)
	}
}

func TestRevocationFencesStaleGenerations(t *testing.T) {
	r := NewRegistry()
	shared := shareClaude(t, r)

	// Borrower commands against the live lease are accepted and become
	// the session operator.
	cmd := vmagent.BorrowCommandPayload{
		CommandID: "c1", AgentInstanceID: "agent-claude-1",
		BorrowerDeviceID: "dev_bob", LeaseGeneration: shared.LeaseGeneration,
		Input: "look at issue 12",
	}
	if _, err := r.Command("room1", cmd); err != nil {
		t.Fatal(err)
	}

	// Owner revokes: generation bumps.
	_, revoked, err := r.Revoke("room1", "dev_alice", "agent-claude-1")
	if err != nil {
		t.Fatal(err)
	}
	if revoked.FinalGeneration != shared.LeaseGeneration+1 || revoked.Reason != "owner_revoked" {
		t.Fatalf("revoked payload %+v", revoked)
	}

	// Old-generation commands die immediately, even from the same
	// borrower who held the lease.
	stale := cmd
	stale.CommandID = "c2"
	if _, err := r.Command("room1", stale); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("stale command err = %v", err)
	}
	// And the shared instance no longer accepts commands at all.
	fresh := cmd
	fresh.CommandID = "c3"
	fresh.LeaseGeneration = revoked.FinalGeneration
	if _, err := r.Command("room1", fresh); !errors.Is(err, ErrUnknownAgent) {
		t.Fatalf("post-revoke command err = %v", err)
	}
}

func TestApprovalsRouteToBorrowerNotOwner(t *testing.T) {
	r := NewRegistry()
	shared := shareClaude(t, r)
	cmd := vmagent.BorrowCommandPayload{
		CommandID: "c1", AgentInstanceID: "agent-claude-1",
		BorrowerDeviceID: "dev_bob", LeaseGeneration: shared.LeaseGeneration,
	}
	if _, err := r.Command("room1", cmd); err != nil {
		t.Fatal(err)
	}

	// The executing agent on dev_alice raises a permission prompt; the
	// current borrower (dev_bob) is the operator.
	operator, err := r.OpenApproval("room1", "agent-claude-1", "ap1", "c1")
	if err != nil || operator != "dev_bob" {
		t.Fatalf("operator = %q err = %v", operator, err)
	}

	// The owner must NOT be able to decide.
	if _, err := r.ResolveDecision("room1", "agent-claude-1", "ap1", "dev_alice"); !errors.Is(err, ErrNotOperator) {
		t.Fatalf("owner decision err = %v", err)
	}
	// The borrower decides; routing returns the owning device.
	owner, err := r.ResolveDecision("room1", "agent-claude-1", "ap1", "dev_bob")
	if err != nil || owner != "dev_alice" {
		t.Fatalf("borrower decision owner = %q err = %v", owner, err)
	}
	// Decisions are single-use.
	if _, err := r.ResolveDecision("room1", "agent-claude-1", "ap1", "dev_bob"); err == nil {
		t.Fatal("expected approval to be consumed")
	}
}

func TestApprovalRoutesToOriginatingCommandBorrower(t *testing.T) {
	r := NewRegistry()
	shared := shareClaude(t, r)
	bob := vmagent.BorrowCommandPayload{
		CommandID: "cmd-bob", AgentInstanceID: "agent-claude-1",
		BorrowerDeviceID: "dev_bob", LeaseGeneration: shared.LeaseGeneration,
	}
	if _, err := r.Command("room1", bob); err != nil {
		t.Fatal(err)
	}
	// Carol commands while Bob's execution is still running — she must
	// not become the operator of Bob's pending prompt.
	carol := vmagent.BorrowCommandPayload{
		CommandID: "cmd-carol", AgentInstanceID: "agent-claude-1",
		BorrowerDeviceID: "dev_carol", LeaseGeneration: shared.LeaseGeneration,
	}
	if _, err := r.Command("room1", carol); err != nil {
		t.Fatal(err)
	}
	operator, err := r.OpenApproval("room1", "agent-claude-1", "ap-bob", "cmd-bob")
	if err != nil || operator != "dev_bob" {
		t.Fatalf("prompt routed to %q err = %v (want dev_bob)", operator, err)
	}
	if _, err := r.ResolveDecision("room1", "agent-claude-1", "ap-bob", "dev_carol"); !errors.Is(err, ErrNotOperator) {
		t.Fatalf("carol deciding bob's prompt err = %v", err)
	}
	// Without a command id the current operator (carol) receives it.
	operator, err = r.OpenApproval("room1", "agent-claude-1", "ap-carol", "")
	if err != nil || operator != "dev_carol" {
		t.Fatalf("legacy routing operator = %q err = %v", operator, err)
	}
}

func TestReShareStartsNewGenerationAndClearsOperator(t *testing.T) {
	r := NewRegistry()
	first := shareClaude(t, r)
	cmd := vmagent.BorrowCommandPayload{
		CommandID: "c1", AgentInstanceID: "agent-claude-1",
		BorrowerDeviceID: "dev_bob", LeaseGeneration: first.LeaseGeneration,
	}
	if _, err := r.Command("room1", cmd); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.Revoke("room1", "dev_alice", "agent-claude-1"); err != nil {
		t.Fatal(err)
	}
	second := shareClaude(t, r)
	if second.LeaseGeneration != first.LeaseGeneration+2 {
		t.Fatalf("re-share generation = %d", second.LeaseGeneration)
	}
	// No operator until a new command arrives.
	if _, err := r.OpenApproval("room1", "agent-claude-1", "ap2", ""); err == nil {
		t.Fatal("expected no active borrowing session after re-share")
	}
}

func TestClearRoomDropsAgentsAndApprovals(t *testing.T) {
	r := NewRegistry()
	shared := shareClaude(t, r)
	cmd := vmagent.BorrowCommandPayload{
		CommandID: "c1", AgentInstanceID: "agent-claude-1",
		BorrowerDeviceID: "dev_bob", LeaseGeneration: shared.LeaseGeneration,
	}
	if _, err := r.Command("room1", cmd); err != nil {
		t.Fatal(err)
	}
	if _, err := r.OpenApproval("room1", "agent-claude-1", "ap1", "c1"); err != nil {
		t.Fatal(err)
	}
	r.ClearRoom("room1")
	if _, ok := r.Agent("room1", "agent-claude-1"); ok {
		t.Fatal("agent survived ClearRoom")
	}
	if _, err := r.ResolveDecision("room1", "agent-claude-1", "ap1", "dev_bob"); err == nil {
		t.Fatal("approval survived ClearRoom")
	}
}
