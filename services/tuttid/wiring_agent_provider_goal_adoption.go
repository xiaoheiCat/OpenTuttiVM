package main

import (
	"context"

	agentruntime "github.com/xiaoheiCat/OpenTuttiVM/packages/agent/daemon/runtime"
	agenthost "github.com/xiaoheiCat/OpenTuttiVM/packages/agent/host"
)

func configureAgentProviderGoalAdoption(controller *agentruntime.Controller, host *agenthost.Host) {
	if controller == nil || host == nil {
		return
	}
	controller.SetProviderGoalAdoptionSink(func(
		ctx context.Context,
		session agentruntime.Session,
		request agentruntime.ProviderGoalAdoptionRequest,
	) (agentruntime.GoalProvenanceBinding, error) {
		result, err := host.AdoptProviderGoal(ctx, agenthost.ProviderGoalAdoptionInput{
			WorkspaceID: session.RoomID, AgentSessionID: session.AgentSessionID,
			ProviderSessionID: session.ProviderSessionID,
			Fingerprint:       request.Fingerprint,
			ExpectedRevision:  request.ExpectedRevision,
			Goal:              request.Goal,
		})
		if err != nil {
			return agentruntime.GoalProvenanceBinding{}, err
		}
		return agentruntime.GoalProvenanceBinding{
			OperationID: result.OperationID,
			Revision:    result.Revision,
			RepairEpoch: result.RepairEpoch,
		}, nil
	})
}
