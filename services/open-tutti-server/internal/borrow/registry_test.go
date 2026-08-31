package borrow

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	borrowagent "github.com/xiaoheiCat/OpenTuttiVM/packages/agent/borrow"
)

func shareClaude(t *testing.T, r *Registry) borrowagent.AgentSharedPayload {
	t.Helper()
	p := borrowagent.AgentSharedPayload{
		AgentInstanceID: "agent-claude-1", OwnerDeviceID: "dev_alice",
		Provider: "claude-code", Borrowable: true, Shared: true,
		Capabilities: borrowagent.AgentCapabilities{
			Skills: []string{"repo-walk"}, MCP: []string{"github"}, Tools: []string{"bash"},
		},
	}
	out, err := r.Share("room1", p)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestUnexpectedBorrowerDisconnectRequiresGenerationBoundInterrupt(t *testing.T) {
	r := NewRegistry()
	shared := shareClaude(t, r)
	r.SetPresence("room1", "dev_bob", true)
	cmd := borrowagent.BorrowCommandPayload{CommandID: "running", AgentInstanceID: shared.AgentInstanceID, BorrowerDeviceID: "dev_bob", LeaseGeneration: shared.LeaseGeneration}
	if err := r.DispatchCommand("room1", cmd, func(string, uint64, borrowagent.BorrowCommandPayload) bool { return true }); err != nil {
		t.Fatal(err)
	}
	r.SetPresence("room1", "dev_bob", false)
	newCommand := cmd
	newCommand.CommandID = "new-after-disconnect"
	if _, err := r.Command("room1", newCommand); !errors.Is(err, ErrOperatorDisconnected) {
		t.Fatalf("command during disconnect grace = %v", err)
	}
	if _, err := r.OpenApproval("room1", shared.AgentInstanceID, "approval", cmd.CommandID, []string{"yes"}); !errors.Is(err, ErrOperatorDisconnected) {
		t.Fatalf("approval during disconnect grace = %v", err)
	}
	if got := r.ExpireDisconnectGrace(time.Now().Add(borrowerDisconnectGrace / 2)); len(got) != 0 {
		t.Fatalf("interrupt emitted before grace: %+v", got)
	}
	got := r.ExpireDisconnectGrace(time.Now().Add(borrowerDisconnectGrace * 2))
	if len(got) != 1 {
		t.Fatalf("interrupt count = %d", len(got))
	}
	req := got[0]
	if req.RoomID != "room1" || req.OwnerDeviceID != "dev_alice" || req.Payload.CommandID != cmd.CommandID || req.Payload.LeaseGeneration != shared.LeaseGeneration {
		t.Fatalf("interrupt request = %+v", req)
	}
	if got := r.ExpireDisconnectGrace(time.Now().Add(borrowerDisconnectGrace * 3)); len(got) != 0 {
		t.Fatalf("interrupt repeated: %+v", got)
	}
	if _, err := r.OpenApproval("room1", shared.AgentInstanceID, "approval-after-request", cmd.CommandID, []string{"yes"}); !errors.Is(err, ErrOperatorDisconnected) {
		t.Fatalf("interrupt request must not fake command terminal: %v", err)
	}
}

func TestBorrowerReconnectWithinGraceKeepsCommand(t *testing.T) {
	r := NewRegistry()
	shared := shareClaude(t, r)
	r.BeginPresence("room1", "dev_bob")
	cmd := borrowagent.BorrowCommandPayload{CommandID: "running", AgentInstanceID: shared.AgentInstanceID, BorrowerDeviceID: "dev_bob", LeaseGeneration: shared.LeaseGeneration}
	if err := r.DispatchCommand("room1", cmd, func(string, uint64, borrowagent.BorrowCommandPayload) bool { return true }); err != nil {
		t.Fatal(err)
	}
	r.SetPresence("room1", "dev_bob", false)
	r.BeginPresence("room1", "dev_bob")
	if got := r.ExpireDisconnectGrace(time.Now().Add(borrowerDisconnectGrace * 2)); len(got) != 0 {
		t.Fatalf("reconnected borrower was interrupted: %+v", got)
	}
	if _, err := r.OpenApproval("room1", shared.AgentInstanceID, "approval", cmd.CommandID, []string{"yes"}); err != nil {
		t.Fatalf("approval after reconnect: %v", err)
	}
}

func TestShareRejectsUnsafeAdapter(t *testing.T) {
	r := NewRegistry()
	p := borrowagent.AgentSharedPayload{
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
	p := borrowagent.AgentSharedPayload{
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
	cmd := borrowagent.BorrowCommandPayload{
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

func TestCommandRequiresIDAndQueuedDeliveryDiesWithBorrower(t *testing.T) {
	r := NewRegistry()
	shared := shareClaude(t, r)
	r.SetPresence("room1", "dev_bob", true)
	empty := borrowagent.BorrowCommandPayload{AgentInstanceID: shared.AgentInstanceID, BorrowerDeviceID: "dev_bob", LeaseGeneration: shared.LeaseGeneration}
	if _, err := r.Command("room1", empty); !errors.Is(err, ErrCommandIDRequired) {
		t.Fatalf("empty command id = %v", err)
	}
	cmd := empty
	cmd.CommandID = "queued"
	if err := r.DispatchCommand("room1", cmd, func(string, uint64, borrowagent.BorrowCommandPayload) bool { return true }); err != nil {
		t.Fatal(err)
	}
	r.SetPresence("room1", "dev_bob", false)
	if r.ValidateDeliveryForBorrower("room1", shared.AgentInstanceID, "", cmd.CommandID, "dev_alice", "dev_bob", shared.LeaseGeneration) {
		t.Fatal("queued command survived borrower departure")
	}
}

func TestApprovalCommandRequiresID(t *testing.T) {
	r := NewRegistry()
	shared := shareClaude(t, r)
	if _, err := r.OpenApproval("room1", shared.AgentInstanceID, "approval", "", nil); err == nil {
		t.Fatal("empty approval command id accepted")
	}
}

func TestApprovalsRouteToBorrowerNotOwner(t *testing.T) {
	r := NewRegistry()
	shared := shareClaude(t, r)
	cmd := borrowagent.BorrowCommandPayload{
		CommandID: "c1", AgentInstanceID: "agent-claude-1",
		BorrowerDeviceID: "dev_bob", LeaseGeneration: shared.LeaseGeneration,
	}
	if _, err := r.Command("room1", cmd); err != nil {
		t.Fatal(err)
	}

	// The executing agent on dev_alice raises a permission prompt; the
	// current borrower (dev_bob) is the operator.
	operator, err := r.OpenApproval("room1", "agent-claude-1", "ap1", "c1", []string{"allow", "deny"})
	if err != nil || operator != "dev_bob" {
		t.Fatalf("operator = %q err = %v", operator, err)
	}

	// The owner must NOT be able to decide.
	if _, err := r.ResolveDecision("room1", "agent-claude-1", "ap1", "dev_alice", 0); !errors.Is(err, ErrNotOperator) {
		t.Fatalf("owner decision err = %v", err)
	}
	// The borrower decides; routing returns the owning device.
	owner, err := r.ResolveDecision("room1", "agent-claude-1", "ap1", "dev_bob", 1)
	if err != nil || owner != "dev_alice" {
		t.Fatalf("borrower decision owner = %q err = %v", owner, err)
	}
	// Decisions are single-use.
	if _, err := r.ResolveDecision("room1", "agent-claude-1", "ap1", "dev_bob", 1); err == nil {
		t.Fatal("expected approval to be consumed")
	}
}

func TestApprovalRoutesToOriginatingCommandBorrower(t *testing.T) {
	r := NewRegistry()
	shared := shareClaude(t, r)
	bob := borrowagent.BorrowCommandPayload{
		CommandID: "cmd-bob", AgentInstanceID: "agent-claude-1",
		BorrowerDeviceID: "dev_bob", LeaseGeneration: shared.LeaseGeneration,
	}
	if _, err := r.Command("room1", bob); err != nil {
		t.Fatal(err)
	}
	// Carol commands while Bob's execution is still running — she must
	// not become the operator of Bob's pending prompt.
	carol := borrowagent.BorrowCommandPayload{
		CommandID: "cmd-carol", AgentInstanceID: "agent-claude-1",
		BorrowerDeviceID: "dev_carol", LeaseGeneration: shared.LeaseGeneration,
	}
	if _, err := r.Command("room1", carol); err != nil {
		t.Fatal(err)
	}
	operator, err := r.OpenApproval("room1", "agent-claude-1", "ap-bob", "cmd-bob", []string{"yes"})
	if err != nil || operator != "dev_bob" {
		t.Fatalf("prompt routed to %q err = %v (want dev_bob)", operator, err)
	}
	if _, err := r.ResolveDecision("room1", "agent-claude-1", "ap-bob", "dev_carol", 0); !errors.Is(err, ErrNotOperator) {
		t.Fatalf("carol deciding bob's prompt err = %v", err)
	}
	// Approval prompts must always identify the command they belong to.
	if _, err = r.OpenApproval("room1", "agent-claude-1", "ap-carol", "", nil); !errors.Is(err, ErrCommandIDRequired) {
		t.Fatalf("empty command id approval = %v", err)
	}
}

func TestReShareStartsNewGenerationAndClearsOperator(t *testing.T) {
	r := NewRegistry()
	first := shareClaude(t, r)
	cmd := borrowagent.BorrowCommandPayload{
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
	if _, err := r.OpenApproval("room1", "agent-claude-1", "ap2", "", nil); err == nil {
		t.Fatal("expected no active borrowing session after re-share")
	}
}

func TestClearRoomDropsAgentsAndApprovals(t *testing.T) {
	r := NewRegistry()
	shared := shareClaude(t, r)
	cmd := borrowagent.BorrowCommandPayload{
		CommandID: "c1", AgentInstanceID: "agent-claude-1",
		BorrowerDeviceID: "dev_bob", LeaseGeneration: shared.LeaseGeneration,
	}
	if _, err := r.Command("room1", cmd); err != nil {
		t.Fatal(err)
	}
	if _, err := r.OpenApproval("room1", "agent-claude-1", "ap1", "c1", nil); err != nil {
		t.Fatal(err)
	}
	r.ClearRoom("room1")
	if _, ok := r.Agent("room1", "agent-claude-1"); ok {
		t.Fatal("agent survived ClearRoom")
	}
	if _, err := r.ResolveDecision("room1", "agent-claude-1", "ap1", "dev_bob", -1); err == nil {
		t.Fatal("approval survived ClearRoom")
	}
}

func TestCommandFailureIsBoundToOwnerBorrowerAndGeneration(t *testing.T) {
	r := NewRegistry()
	shared := shareClaude(t, r)
	cmd := borrowagent.BorrowCommandPayload{CommandID: "cmd", AgentInstanceID: shared.AgentInstanceID, BorrowerDeviceID: "dev_bob", LeaseGeneration: shared.LeaseGeneration}
	if _, err := r.Command("room1", cmd); err != nil {
		t.Fatal(err)
	}
	failure := borrowagent.CommandFailedPayload{CommandID: cmd.CommandID, AgentInstanceID: cmd.AgentInstanceID, LeaseGeneration: cmd.LeaseGeneration, Reason: "host unavailable"}
	out, err := r.CommandFailed("room1", "dev_alice", failure)
	if err != nil || out.BorrowerDeviceID != "dev_bob" {
		t.Fatalf("failure routing = %+v err=%v", out, err)
	}
	if _, err := r.CommandFailed("room1", "dev_bob", failure); !errors.Is(err, ErrCommandFailedOwner) {
		t.Fatalf("non-owner failure err = %v", err)
	}
	failure.LeaseGeneration++
	if _, err := r.CommandFailed("room1", "dev_alice", failure); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("stale failure err = %v", err)
	}
}

func TestDispatchCallbacksDoNotRunUnderRegistryLock(t *testing.T) {
	r := NewRegistry()
	shared := shareClaude(t, r)
	cmd := borrowagent.BorrowCommandPayload{
		CommandID: "dispatch", AgentInstanceID: shared.AgentInstanceID,
		BorrowerDeviceID: "dev_bob", LeaseGeneration: shared.LeaseGeneration,
	}
	if err := r.DispatchCommand("room1", cmd, func(owner string, generation uint64, _ borrowagent.BorrowCommandPayload) bool {
		if owner != "dev_alice" || generation != shared.LeaseGeneration {
			t.Errorf("invalid command delivery fence")
		}
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if err := r.DispatchApproval("room1", cmd.AgentInstanceID, "approval", cmd.CommandID, []string{"yes"}, func(operator string, generation uint64) bool {
		if operator != "dev_bob" || generation != shared.LeaseGeneration {
			t.Errorf("invalid approval delivery: operator=%q generation=%d", operator, generation)
		}
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if err := r.ResolveDecisionDispatch("room1", cmd.AgentInstanceID, "approval", "dev_bob", 0, func(owner string, generation uint64) bool {
		if owner != "dev_alice" || generation != shared.LeaseGeneration {
			t.Errorf("invalid decision delivery: owner=%q generation=%d", owner, generation)
		}
		return true
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCommandIDRetriesAreIdempotentAndPayloadBound(t *testing.T) {
	r := NewRegistry()
	shared := shareClaude(t, r)
	cmd := borrowagent.BorrowCommandPayload{CommandID: "same", AgentInstanceID: shared.AgentInstanceID, BorrowerDeviceID: "dev_bob", LeaseGeneration: shared.LeaseGeneration, Input: "one"}
	if _, err := r.Command("room1", cmd); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Command("room1", cmd); err != nil {
		t.Fatalf("same retry: %v", err)
	}
	changed := cmd
	changed.Input = "two"
	if _, err := r.Command("room1", changed); !errors.Is(err, ErrDuplicateCommand) {
		t.Fatalf("changed retry = %v", err)
	}
}

func TestOutstandingCommandsDoNotEvictApprovalRoute(t *testing.T) {
	r := NewRegistry()
	shared := shareClaude(t, r)
	start := make(chan struct{})
	errs := make(chan error, 17)
	var wg sync.WaitGroup
	for i := 0; i < 17; i++ {
		wg.Add(1)
		cmd := borrowagent.BorrowCommandPayload{CommandID: fmt.Sprintf("cmd-%d", i), AgentInstanceID: shared.AgentInstanceID, BorrowerDeviceID: "dev_bob", LeaseGeneration: shared.LeaseGeneration}
		go func() {
			defer wg.Done()
			<-start
			errs <- r.DispatchCommand("room1", cmd, func(string, uint64, borrowagent.BorrowCommandPayload) bool { return true })
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent command: %v", err)
		}
	}
	if operator, err := r.OpenApproval("room1", shared.AgentInstanceID, "approval-first", "cmd-0", []string{"yes"}); err != nil || operator != "dev_bob" {
		t.Fatalf("first approval route = %q, %v", operator, err)
	}
}

func TestCommandLimitRejectsWithoutEvictingActiveMappings(t *testing.T) {
	r := NewRegistry()
	shared := shareClaude(t, r)
	for i := 0; i < maxTrackedCommandsPerAgent; i++ {
		cmd := borrowagent.BorrowCommandPayload{CommandID: fmt.Sprintf("limit-%d", i), AgentInstanceID: shared.AgentInstanceID, BorrowerDeviceID: "dev_bob", LeaseGeneration: shared.LeaseGeneration}
		if err := r.DispatchCommand("room1", cmd, func(string, uint64, borrowagent.BorrowCommandPayload) bool { return true }); err != nil {
			t.Fatal(err)
		}
	}
	cmd := borrowagent.BorrowCommandPayload{CommandID: "over-limit", AgentInstanceID: shared.AgentInstanceID, BorrowerDeviceID: "dev_carol", LeaseGeneration: shared.LeaseGeneration}
	if err := r.DispatchCommand("room1", cmd, func(string, uint64, borrowagent.BorrowCommandPayload) bool { return true }); !errors.Is(err, ErrOutstandingCommands) {
		t.Fatalf("over-limit error = %v", err)
	}
	if operator, err := r.OpenApproval("room1", shared.AgentInstanceID, "approval-limit", "limit-0", []string{"yes"}); err != nil || operator != "dev_bob" {
		t.Fatalf("active mapping after rejection = %q, %v", operator, err)
	}
}

func TestDispatchFailureDoesNotConsumeCommandID(t *testing.T) {
	r := NewRegistry()
	shared := shareClaude(t, r)
	cmd := borrowagent.BorrowCommandPayload{CommandID: "retry", AgentInstanceID: shared.AgentInstanceID, BorrowerDeviceID: "dev_bob", LeaseGeneration: shared.LeaseGeneration, Input: "one"}
	if err := r.DispatchCommand("room1", cmd, func(string, uint64, borrowagent.BorrowCommandPayload) bool { return false }); !errors.Is(err, ErrDeliveryUnavailable) {
		t.Fatalf("first delivery = %v", err)
	}
	var delivered int
	if err := r.DispatchCommand("room1", cmd, func(string, uint64, borrowagent.BorrowCommandPayload) bool { delivered++; return true }); err != nil {
		t.Fatalf("retry delivery = %v", err)
	}
	if delivered != 1 {
		t.Fatalf("delivery count = %d", delivered)
	}
	changed := cmd
	changed.Input = "two"
	if err := r.DispatchCommand("room1", changed, func(string, uint64, borrowagent.BorrowCommandPayload) bool { return true }); !errors.Is(err, ErrDuplicateCommand) {
		t.Fatalf("changed retry = %v", err)
	}
}

func TestApprovalChoiceValidationDoesNotConsume(t *testing.T) {
	r := NewRegistry()
	shared := shareClaude(t, r)
	cmd := borrowagent.BorrowCommandPayload{CommandID: "choice", AgentInstanceID: shared.AgentInstanceID, BorrowerDeviceID: "dev_bob", LeaseGeneration: shared.LeaseGeneration}
	if _, err := r.Command("room1", cmd); err != nil {
		t.Fatal(err)
	}
	if _, err := r.OpenApproval("room1", cmd.AgentInstanceID, "ap-choice", cmd.CommandID, []string{"yes"}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ResolveDecision("room1", cmd.AgentInstanceID, "ap-choice", "dev_bob", 2); !errors.Is(err, ErrInvalidChoice) {
		t.Fatalf("invalid choice = %v", err)
	}
	if _, err := r.ResolveDecision("room1", cmd.AgentInstanceID, "ap-choice", "dev_bob", -1); err != nil {
		t.Fatalf("approval was consumed: %v", err)
	}
}
