package agent

import (
	"context"
	"testing"

	"github.com/xiaoheiCat/OpenTuttiVM/packages/agent/store-sqlite/canonical"
)

func TestConfigureSessionStateObserversRequiresRootSettlementPolicy(t *testing.T) {
	t.Parallel()

	projection := &ActivityProjection{}
	err := projection.ConfigureSessionStateObservers(SessionStateObserverRegistration{
		Observer: &recordingSessionStateObserver{},
	})
	if err == nil {
		t.Fatal("ConfigureSessionStateObservers() error = nil, want unclassified root settlement policy rejected")
	}
}

func TestConfigureSessionStateObserversRoutesRootSettlementsByPolicy(t *testing.T) {
	t.Parallel()

	rootConsumer := &recordingSessionStateObserver{}
	legacyOnlyConsumer := &recordingSessionStateObserver{}
	projection := &ActivityProjection{}
	err := projection.ConfigureSessionStateObservers(
		SessionStateObserverRegistration{
			Observer:            rootConsumer,
			RootTurnSettlements: RootTurnSettlementsObserve,
		},
		SessionStateObserverRegistration{
			Observer:            legacyOnlyConsumer,
			RootTurnSettlements: RootTurnSettlementsIgnore,
		},
	)
	if err != nil {
		t.Fatalf("ConfigureSessionStateObservers() error = %v", err)
	}

	input := canonical.ReportSessionStateInput{WorkspaceID: "ws-1", AgentSessionID: "session-1"}
	reply := canonical.ReportSessionStateReply{Accepted: true, StateApplied: true}
	projection.observeSessionState(context.Background(), input, reply)
	projection.rootTurnSettleStateObserver.ObserveAgentSessionState(context.Background(), input, reply)

	if rootConsumer.calls != 2 {
		t.Fatalf("root consumer calls = %d, want legacy + root settlement", rootConsumer.calls)
	}
	if legacyOnlyConsumer.calls != 1 {
		t.Fatalf("legacy-only consumer calls = %d, want legacy only", legacyOnlyConsumer.calls)
	}
}

type recordingSessionStateObserver struct {
	calls int
}

func (s *recordingSessionStateObserver) ObserveAgentSessionState(
	context.Context,
	canonical.ReportSessionStateInput,
	canonical.ReportSessionStateReply,
) {
	s.calls++
}
