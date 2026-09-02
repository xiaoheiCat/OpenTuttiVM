package api

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	agentactivitybiz "github.com/xiaoheiCat/OpenTuttiVM/packages/agent/store-sqlite"
	tuttigenerated "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/api/generated"
	agentservice "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/agent"
	agentsessionreplay "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/agentsessionreplay"
)

type goalClientSubmitService struct {
	stubAgentSessionService
	clientSubmitID string
}

type goalStimulusRecorder struct {
	AgentSessionRecordingService
	events []agentsessionreplay.ActivityEvent
}

func (r *goalStimulusRecorder) RecordActivityEvent(_ context.Context, event agentsessionreplay.ActivityEvent) error {
	r.events = append(r.events, event)
	return nil
}

func (s *goalClientSubmitService) GoalControl(_ context.Context, input agentservice.GoalControlInput) (agentservice.GoalControlSessionResult, error) {
	s.clientSubmitID = input.ClientSubmitID
	return agentservice.GoalControlSessionResult{
		Session: agentservice.Session{
			ID: input.AgentSessionID, Provider: "codex", Visible: true, CreatedAt: time.UnixMilli(1),
		},
	}, nil
}

func TestGeneratedGoalControlProjectionMakesClearExplicitAndAuthoritative(t *testing.T) {
	stale := tuttigenerated.WorkspaceAgentSessionGoal{
		Objective: "stale goal",
		Status:    tuttigenerated.Active,
	}
	session := tuttigenerated.WorkspaceAgentSession{Goal: &stale}
	goal := generatedGoalControlProjection(&session, nil)
	if goal != nil || session.Goal != nil {
		t.Fatalf("goal/session goal = %#v/%#v, want explicit clear", goal, session.Goal)
	}

	raw, err := json.Marshal(tuttigenerated.WorkspaceAgentSessionGoalControlResponse{
		Goal:    goal,
		Session: session,
	})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	value, found := payload["goal"]
	if !found || value != nil {
		t.Fatalf("goal field = %#v, found=%t; payload=%s", value, found, raw)
	}
}

func TestGeneratedAgentSessionGoalStateNormalizesInvalidEnums(t *testing.T) {
	state := generatedAgentSessionGoalState(agentactivitybiz.SessionGoalState{
		SyncStatus: "",
		Observed:   map[string]any{"objective": "ship", "status": "limited"},
	})
	if state.SyncStatus != tuttigenerated.WorkspaceAgentSessionGoalStateSyncStatusUnknown {
		t.Fatalf("syncStatus = %q", state.SyncStatus)
	}
	if state.Observed != nil {
		t.Fatalf("invalid observed goal leaked into API: %#v", state.Observed)
	}
}

func TestGoalControlForwardsOptionalClientSubmitIdentity(t *testing.T) {
	service := &goalClientSubmitService{}
	recorder := &goalStimulusRecorder{}
	clientSubmitID := "goal-submit-engine-1"
	body := tuttigenerated.WorkspaceAgentSessionGoalControlRequest{
		Action:         tuttigenerated.WorkspaceAgentSessionGoalControlRequestActionSet,
		ClientSubmitId: &clientSubmitID,
	}
	_, err := (DaemonAPI{
		AgentSessionRecordingService: recorder,
		AgentSessionService:          service,
	}).GoalControlWorkspaceAgentSession(
		context.Background(),
		tuttigenerated.GoalControlWorkspaceAgentSessionRequestObject{
			WorkspaceID:    "workspace-1",
			AgentSessionID: "session-1",
			Body:           &body,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if service.clientSubmitID != clientSubmitID {
		t.Fatalf("client submit id=%q want %q", service.clientSubmitID, clientSubmitID)
	}
	if len(recorder.events) != 1 || recorder.events[0].Payload["clientSubmitId"] != clientSubmitID {
		t.Fatalf("goal stimuli=%#v", recorder.events)
	}

	origin := tuttigenerated.GoalControlWorkspaceAgentSessionParamsXTuttiAgentCommandOriginRendererEngine
	_, err = (DaemonAPI{
		AgentSessionRecordingService: recorder,
		AgentSessionService:          service,
	}).GoalControlWorkspaceAgentSession(
		context.Background(),
		tuttigenerated.GoalControlWorkspaceAgentSessionRequestObject{
			WorkspaceID:    "workspace-1",
			AgentSessionID: "session-1",
			Params: tuttigenerated.GoalControlWorkspaceAgentSessionParams{
				XTuttiAgentCommandOrigin: &origin,
			},
			Body: &body,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(recorder.events) != 1 {
		t.Fatalf("Engine-origin goal recorded as direct stimulus: %#v", recorder.events)
	}
}
