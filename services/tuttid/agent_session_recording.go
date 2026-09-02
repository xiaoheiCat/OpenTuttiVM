package main

import (
	"context"
	"errors"
	"sync"

	"github.com/google/uuid"
	agentdaemon "github.com/xiaoheiCat/OpenTuttiVM/packages/agent/daemon"
	agentruntime "github.com/xiaoheiCat/OpenTuttiVM/packages/agent/daemon/runtime"
	agenthost "github.com/xiaoheiCat/OpenTuttiVM/packages/agent/host"
	replay "github.com/xiaoheiCat/OpenTuttiVM/packages/agent/session-replay"
	replaydata "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/data/agentsessionreplay"
	workspacedata "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/data/workspace"
	agentservice "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/agent"
	agentsessionreplay "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/agentsessionreplay"
	tuttitypes "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/types"
)

type agentCommitObservers []agenthost.CommitObserver

// agentCommitObserverRelay lets startup compose the Host before every
// daemon-local post-commit consumer is available. The Host keeps this stable
// relay while the recorder joins before Host recovery begins.
type agentCommitObserverRelay struct {
	mu        sync.RWMutex
	observers agentCommitObservers
}

type agentProviderObservationObservers []agentruntime.ProviderObservationObserver

type agentSessionReplayProductStateStore interface {
	ResolveRootAgentSession(context.Context, string, string) (string, error)
	WaitAgentSessionGraphSettled(context.Context, string, string) error
	CaptureReplayStateWithAgentGraph(
		context.Context,
		string,
		agenthost.HistoricalSessionGraph,
	) ([]byte, error)
}

type agentSessionReplayHostStateStore struct {
	product agentSessionReplayProductStateStore
	agents  *agentservice.Service
}

func (s agentSessionReplayHostStateStore) ResolveRootAgentSession(
	ctx context.Context,
	workspaceID, sessionID string,
) (string, error) {
	return s.product.ResolveRootAgentSession(ctx, workspaceID, sessionID)
}

func (s agentSessionReplayHostStateStore) WaitAgentSessionGraphSettled(
	ctx context.Context,
	workspaceID, rootSessionID string,
) error {
	return s.product.WaitAgentSessionGraphSettled(ctx, workspaceID, rootSessionID)
}

func (s agentSessionReplayHostStateStore) CaptureReplayState(
	ctx context.Context,
	workspaceID, rootSessionID string,
) ([]byte, error) {
	graph, err := s.agents.ApplicationHost().CaptureHistoricalSessionGraph(
		ctx,
		agenthost.SessionRef{
			WorkspaceID: workspaceID, AgentSessionID: rootSessionID,
		},
	)
	if err != nil {
		return nil, err
	}
	return s.product.CaptureReplayStateWithAgentGraph(
		ctx,
		workspaceID,
		graph,
	)
}

func (observers agentCommitObservers) ObserveCommitted(ctx context.Context, delta agenthost.CommittedDelta) error {
	var result error
	for _, observer := range observers {
		if observer != nil {
			result = errors.Join(result, observer.ObserveCommitted(ctx, delta))
		}
	}
	return result
}

func newAgentCommitObserverRelay(
	observers ...agenthost.CommitObserver,
) *agentCommitObserverRelay {
	return &agentCommitObserverRelay{observers: append(agentCommitObservers(nil), observers...)}
}

func (relay *agentCommitObserverRelay) Add(observer agenthost.CommitObserver) {
	if relay == nil || observer == nil {
		return
	}
	relay.mu.Lock()
	defer relay.mu.Unlock()
	relay.observers = append(relay.observers, observer)
}

func (relay *agentCommitObserverRelay) ObserveCommitted(
	ctx context.Context,
	delta agenthost.CommittedDelta,
) error {
	if relay == nil {
		return nil
	}
	relay.mu.RLock()
	observers := append(agentCommitObservers(nil), relay.observers...)
	relay.mu.RUnlock()
	return observers.ObserveCommitted(ctx, delta)
}

func (observers agentProviderObservationObservers) ObserveProviderObservations(
	ctx context.Context,
	workspaceID, agentSessionID string,
	batches []replay.ProviderObservationBatch,
) error {
	var result error
	for _, observer := range observers {
		if observer != nil {
			result = errors.Join(
				result,
				observer.ObserveProviderObservations(
					ctx,
					workspaceID,
					agentSessionID,
					batches,
				),
			)
		}
	}
	return result
}

func buildAgentSessionRecordingService(
	store workspacedata.CatalogStore,
	transport *agentdaemon.SessionRecordingProcessTransport,
	agents *agentservice.Service,
) (*agentsessionreplay.Service, error) {
	if transport == nil {
		return nil, nil
	}
	productState, ok := store.(agentSessionReplayProductStateStore)
	if !ok {
		return nil, errors.New("agent session recording state store is unavailable")
	}
	metadata, ok := store.(agentsessionreplay.MetadataStore)
	if !ok {
		return nil, errors.New("agent session replay metadata store is unavailable")
	}
	artifacts := &replaydata.Store{
		StateDir: tuttitypes.DefaultStateDir(),
	}
	service := &agentsessionreplay.Service{
		Workflow: &replay.Workflow{
			States: agentSessionReplayHostStateStore{
				product: productState,
				agents:  agents,
			},
			Artifacts: artifacts,
			Transport: transport,
			Store:     metadata,
			NewID:     uuid.NewString,
		},
	}
	transport.SetProviderInputUnitSink(func(unit agentruntime.ProviderInputUnit) error {
		service.ObserveProviderInputUnit(unit.RecordingID, unit.Position)
		return service.Workflow.RecordProviderInputUnit(
			context.Background(),
			unit.RecordingID,
			replay.ObservationJournalEntry{
				SchemaVersion: replay.ObservationSchemaVersion,
				Position:      unit.Position,
				UnitKind:      unit.Kind,
				Observations:  []replay.JournalObservation{},
				Correlations:  []replay.CheckpointCommitCorrelation{},
			},
		)
	})
	if err := service.Recover(context.Background()); err != nil {
		return nil, err
	}
	return service, nil
}

func configureAgentSessionRecordingObservers(
	projection interface {
		SetRootTurnObserver(agentservice.RootTurnObserver)
		SetReplayCommitObserver(agentservice.ReplayCommitObserver)
	},
	runtime agentservice.RootTurnObserver,
	recording *agentsessionreplay.Service,
) {
	projection.SetRootTurnObserver(runtime)
	if recording != nil {
		projection.SetReplayCommitObserver(recording)
	}
}

func buildAgentCommitObserver(
	projection agenthost.CommitObserver,
	recordingEnabled bool,
) (agenthost.CommitObserver, *agentCommitObserverRelay) {
	if !recordingEnabled {
		return projection, nil
	}
	relay := newAgentCommitObserverRelay(projection)
	return relay, relay
}

func composeAgentProviderObservationObserver(
	observers agentProviderObservationObservers,
) agentruntime.ProviderObservationObserver {
	switch len(observers) {
	case 0:
		return nil
	case 1:
		return observers[0]
	default:
		return observers
	}
}
