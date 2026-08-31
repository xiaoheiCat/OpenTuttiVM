package realtime

import (
	"strings"
	"testing"

	borrowagent "github.com/xiaoheiCat/OpenTuttiVM/packages/agent/borrow"
)

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
