package api

import (
	"strings"

	tuttigenerated "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/api/generated"
	agentservice "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/agent"
)

func generatedAgentSessionGoalSyncState(
	state *agentservice.SessionGoalSyncState,
) *tuttigenerated.WorkspaceAgentSessionGoalSyncState {
	if state == nil {
		return nil
	}
	syncStatus := tuttigenerated.WorkspaceAgentSessionGoalSyncStateSyncStatus(state.SyncStatus)
	if !syncStatus.Valid() {
		syncStatus = tuttigenerated.WorkspaceAgentSessionGoalSyncStateSyncStatusUnknown
	}
	return &tuttigenerated.WorkspaceAgentSessionGoalSyncState{
		PendingOperationId: optionalStringPointer(strings.TrimSpace(state.PendingOperationID)),
		Revision:           state.Revision,
		SyncStatus:         syncStatus,
		ExecutionPending:   state.ExecutionPending,
	}
}
