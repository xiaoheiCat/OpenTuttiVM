package main

import (
	"context"
	"testing"

	agenthost "github.com/xiaoheiCat/OpenTuttiVM/packages/agent/host"
	replay "github.com/xiaoheiCat/OpenTuttiVM/packages/agent/session-replay"
	agentactivitybiz "github.com/xiaoheiCat/OpenTuttiVM/packages/agent/store-sqlite"
	agentservice "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/agent"
)

type countingAgentCommitObserver struct {
	calls int
}

func (o *countingAgentCommitObserver) ObserveCommitted(context.Context, agenthost.CommittedDelta) error {
	o.calls++
	return nil
}

type countingAgentProviderObservationObserver struct {
	calls int
}

type recordingCompositionProjection struct {
	root   agentservice.RootTurnObserver
	replay agentservice.ReplayCommitObserver
}

func (p *recordingCompositionProjection) SetRootTurnObserver(
	observer agentservice.RootTurnObserver,
) {
	p.root = observer
}

func (p *recordingCompositionProjection) SetReplayCommitObserver(
	observer agentservice.ReplayCommitObserver,
) {
	p.replay = observer
}

type rootTurnObserverStub struct{}

func (rootTurnObserverStub) ObserveRootTurnSettled(
	context.Context,
	string,
	string,
	agentactivitybiz.Turn,
) {
}

func TestDisabledAgentSessionRecordingCompositionCreatesNoReplayDependencies(
	t *testing.T,
) {
	projection := &countingAgentCommitObserver{}
	commitObserver, relay := buildAgentCommitObserver(projection, false)
	if commitObserver != projection || relay != nil {
		t.Fatalf("disabled commit composition = (%T, %#v), want direct projection", commitObserver, relay)
	}
	service, err := buildAgentSessionRecordingService(nil, nil, nil)
	if err != nil || service != nil {
		t.Fatalf("disabled recording service = (%#v, %v), want nil, nil", service, err)
	}
	configured := &recordingCompositionProjection{}
	runtime := rootTurnObserverStub{}
	configureAgentSessionRecordingObservers(configured, runtime, nil)
	if configured.root == nil || configured.replay != nil {
		t.Fatalf("disabled observers = root:%T replay:%T", configured.root, configured.replay)
	}
	if observer := composeAgentProviderObservationObserver(nil); observer != nil {
		t.Fatalf("disabled provider observer = %T, want nil", observer)
	}
	if verifier := composeAgentReplayVerifier(nil, nil); verifier != nil {
		t.Fatalf("disabled replay verifier = %T, want nil", verifier)
	}
}

func TestSingleProviderObservationObserverNeedsNoFanout(t *testing.T) {
	observer := &countingAgentProviderObservationObserver{}
	composed := composeAgentProviderObservationObserver(
		agentProviderObservationObservers{observer},
	)
	if composed != observer {
		t.Fatalf("single provider observer = %T, want direct observer", composed)
	}
}

func (o *countingAgentProviderObservationObserver) ObserveProviderObservations(
	context.Context,
	string,
	string,
	[]replay.ProviderObservationBatch,
) error {
	o.calls++
	return nil
}

func TestAgentObserverFanoutsDeliverOnceToEveryObserver(t *testing.T) {
	t.Parallel()

	projection := &countingAgentCommitObserver{}
	recordingCommit := &countingAgentCommitObserver{}
	commitObservers := newAgentCommitObserverRelay(projection)
	commitObservers.Add(recordingCommit)
	if err := commitObservers.ObserveCommitted(
		context.Background(),
		agenthost.CommittedDelta{},
	); err != nil {
		t.Fatalf("observe committed: %v", err)
	}
	if projection.calls != 1 || recordingCommit.calls != 1 {
		t.Fatalf(
			"commit fanout calls = projection:%d recording:%d, want 1 each",
			projection.calls,
			recordingCommit.calls,
		)
	}

	recordingProvider := &countingAgentProviderObservationObserver{}
	semanticRuntime := &countingAgentProviderObservationObserver{}
	if err := (agentProviderObservationObservers{
		recordingProvider,
		semanticRuntime,
	}).ObserveProviderObservations(
		context.Background(),
		"workspace-1",
		"session-1",
		nil,
	); err != nil {
		t.Fatalf("observe provider observations: %v", err)
	}
	if recordingProvider.calls != 1 || semanticRuntime.calls != 1 {
		t.Fatalf(
			"provider fanout calls = recording:%d semantic:%d, want 1 each",
			recordingProvider.calls,
			semanticRuntime.calls,
		)
	}
}
