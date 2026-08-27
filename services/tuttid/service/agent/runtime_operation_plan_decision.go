package agent

import (
	"context"

	agenthost "github.com/xiaoheiCat/OpenTuttiVM/packages/agent/host"
	agentactivitybiz "github.com/xiaoheiCat/OpenTuttiVM/packages/agent/store-sqlite"
)

func (s *Service) SubmitPlanDecision(
	ctx context.Context,
	workspaceID string,
	agentSessionID string,
	turnID string,
	requestID string,
	input SubmitPlanDecisionInput,
) (agentactivitybiz.RuntimeOperation, error) {
	return s.ApplicationHost().SubmitPlanDecision(
		ctx,
		agenthost.SessionRef{WorkspaceID: workspaceID, AgentSessionID: agentSessionID},
		turnID,
		requestID,
		input,
	)
}

func validatePlanDecisionStrategy(provider string, input SubmitPlanDecisionInput) error {
	return agenthost.ValidatePlanDecisionStrategy(provider, input)
}
