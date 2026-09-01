package realtime

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	borrowagent "github.com/xiaoheiCat/OpenTuttiVM/packages/agent/borrow"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/borrow"
)

func TestInboundMessageBudgetKeepsLargeFramesForOpsOnly(t *testing.T) {
	for _, tt := range []struct {
		name string
		typ  string
		want bool
	}{
		{name: "control frame", typ: "ping", want: false},
		{name: "unknown frame", typ: "future_control", want: false},
		{name: "operation frame", typ: "op", want: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(struct {
				Type    string `json:"type"`
				Payload string `json:"payload"`
			}{Type: tt.typ, Payload: strings.Repeat("x", maxControlMessageBytes)})
			if err != nil {
				t.Fatal(err)
			}
			if _, got := inboundMessageWithinBudget(data); got != tt.want {
				t.Fatalf("inboundMessageWithinBudget(%q) = %v, want %v", tt.typ, got, tt.want)
			}
		})
	}
}

func TestInboundMessageBudgetRejectsMalformedAndOversizedOperation(t *testing.T) {
	if _, ok := inboundMessageWithinBudget([]byte(`{"type":"ping"`)); ok {
		t.Fatal("malformed frame accepted")
	}
	data, err := json.Marshal(struct {
		Type    string `json:"type"`
		Payload string `json:"payload"`
	}{Type: "op", Payload: strings.Repeat("x", int(maxOperationMessageBytes))})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := inboundMessageWithinBudget(data); ok {
		t.Fatal("oversized operation accepted")
	}
}

func TestInboundMessageBudgetRejectsLargeFramesWithoutBoundedType(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{name: "operation prefix after giant unknown field", data: []byte(`{"unknown":"` + strings.Repeat("x", maxControlMessageBytes) + `","type":"op"}`)},
		{name: "unknown type", data: []byte(`{"type":"future","payload":"` + strings.Repeat("x", maxControlMessageBytes) + `"}`)},
		{name: "malformed discriminator", data: []byte(`{"type":"op" garbage` + strings.Repeat("x", maxControlMessageBytes))},
		{name: "long type", data: []byte(`{"type":"` + strings.Repeat("o", maxDiscriminatorTypeLen+1) + `","payload":"` + strings.Repeat("x", maxControlMessageBytes) + `"}`)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, ok := inboundMessageWithinBudget(tt.data); ok {
				t.Fatal("large frame received an operation budget")
			}
		})
	}
}

func TestValidApprovalDecisionBoundsUntrustedFieldsAndChoice(t *testing.T) {
	tests := []struct {
		name  string
		input borrowagent.ApprovalDecisionPayload
		want  bool
	}{
		{name: "valid timeout", input: borrowagent.ApprovalDecisionPayload{ApprovalID: "ap", AgentInstanceID: "agent", Choice: -1}, want: true},
		{name: "valid first choice", input: borrowagent.ApprovalDecisionPayload{ApprovalID: "ap", AgentInstanceID: "agent", Choice: 0}, want: true},
		{name: "valid last bounded choice", input: borrowagent.ApprovalDecisionPayload{ApprovalID: "ap", AgentInstanceID: "agent", Choice: maxApprovalDecisionChoice - 1}, want: true},
		{name: "approval id too long", input: borrowagent.ApprovalDecisionPayload{ApprovalID: strings.Repeat("a", maxApprovalDecisionIDLen+1), AgentInstanceID: "agent", Choice: 0}},
		{name: "agent id too long", input: borrowagent.ApprovalDecisionPayload{ApprovalID: "ap", AgentInstanceID: strings.Repeat("a", maxApprovalDecisionIDLen+1), Choice: 0}},
		{name: "choice below timeout", input: borrowagent.ApprovalDecisionPayload{ApprovalID: "ap", AgentInstanceID: "agent", Choice: -2}},
		{name: "choice above bounded range", input: borrowagent.ApprovalDecisionPayload{ApprovalID: "ap", AgentInstanceID: "agent", Choice: maxApprovalDecisionChoice}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := validApprovalDecision(tt.input); got != tt.want {
				t.Fatalf("validApprovalDecision() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAttachReplacementDoesNotLetOldConnectionAdvancePresence(t *testing.T) {
	borrows := borrow.NewRegistry()
	shared := borrowagent.AgentSharedPayload{
		AgentInstanceID: "agent-1", OwnerDeviceID: "owner", Borrowable: true, Shared: true,
	}
	if _, err := borrows.Share("room", shared); err != nil {
		t.Fatal(err)
	}
	hub := NewHub(nil, nil, nil, borrows, nil)
	old := NewConn(context.Background(), "room", "borrower", "old")
	current := NewConn(context.Background(), "room", "borrower", "current")
	if err := hub.Attach(old, nil); err != nil {
		t.Fatal(err)
	}
	if err := hub.Attach(current, nil); err != nil {
		t.Fatal(err)
	}

	cmd := borrowagent.BorrowCommandPayload{
		CommandID: "command", AgentInstanceID: shared.AgentInstanceID,
		BorrowerDeviceID: "borrower", LeaseGeneration: 1,
	}
	if err := borrows.DispatchCommand("room", cmd, func(string, uint64, borrowagent.BorrowCommandPayload) bool { return true }); err != nil {
		t.Fatal(err)
	}
	if !borrows.ValidateDeliveryForBorrower("room", shared.AgentInstanceID, "", cmd.CommandID, "owner", "borrower", 1) {
		t.Fatal("current connection presence was invalidated by old connection ordering")
	}
	hub.Detach(old)
	if !borrows.ValidateDeliveryForBorrower("room", shared.AgentInstanceID, "", cmd.CommandID, "owner", "borrower", 1) {
		t.Fatal("old connection detach invalidated current connection presence")
	}
}
