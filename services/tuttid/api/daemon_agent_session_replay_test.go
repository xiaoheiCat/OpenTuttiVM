package api

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	sessionreplay "github.com/xiaoheiCat/OpenTuttiVM/packages/agent/session-replay"
	tuttigenerated "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/api/generated"
	agentsessionreplay "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/agentsessionreplay"
)

type replayVerifierStub struct {
	err             error
	state           AgentSessionReplayPlaybackState
	checkpointState AgentSessionReplayCheckpointState
	checkpointIndex int
	lastCommand     string
	cassetteID      string
}

type replayCassetteListServiceStub struct {
	AgentSessionRecordingService
	workspaceID string
	cassettes   []agentsessionreplay.Cassette
	err         error
}

type replayWorkspacePrepareServiceStub struct {
	AgentSessionRecordingService
	cassetteIDs []string
	workspaceID string
	prepared    agentsessionreplay.ReplayWorkspaceRequest
	err         error
}

func (s *replayWorkspacePrepareServiceStub) PrepareReplayWorkspace(
	_ context.Context,
	workspaceID string,
	cassetteIDs []string,
) (agentsessionreplay.ReplayWorkspaceRequest, error) {
	s.workspaceID = workspaceID
	s.cassetteIDs = append([]string(nil), cassetteIDs...)
	return s.prepared, s.err
}

func (s *replayCassetteListServiceStub) ListCassettes(
	_ context.Context,
	workspaceID string,
) ([]agentsessionreplay.Cassette, error) {
	s.workspaceID = workspaceID
	return s.cassettes, s.err
}

func (v *replayVerifierStub) Verify(_ context.Context, cassetteID string) error {
	v.cassetteID = cassetteID
	return v.err
}

func (v *replayVerifierStub) VerifyCheckpoint(
	_ context.Context,
	cassetteID string,
	checkpointIndex int,
) (AgentSessionReplayCheckpointState, error) {
	v.cassetteID = cassetteID
	v.checkpointIndex = checkpointIndex
	return v.checkpointState, v.err
}

func (v *replayVerifierStub) PlaybackState(
	_ context.Context,
	cassetteID string,
) (AgentSessionReplayPlaybackState, error) {
	v.cassetteID = cassetteID
	return v.state, v.err
}

func (v *replayVerifierStub) SetPlaybackSpeed(
	_ context.Context,
	cassetteID string,
	speed float64,
) (AgentSessionReplayPlaybackState, error) {
	v.cassetteID = cassetteID
	v.lastCommand = "speed"
	v.state.Speed = speed
	return v.state, v.err
}

func (v *replayVerifierStub) SetPlaybackPaused(
	_ context.Context,
	cassetteID string,
	paused bool,
) (AgentSessionReplayPlaybackState, error) {
	v.cassetteID = cassetteID
	v.lastCommand = "paused"
	v.state.Paused = paused
	return v.state, v.err
}

func (v *replayVerifierStub) SetPlaybackFastForward(
	_ context.Context,
	cassetteID string,
	enabled bool,
) (AgentSessionReplayPlaybackState, error) {
	v.cassetteID = cassetteID
	v.lastCommand = "timing"
	v.state.FastForward = enabled
	return v.state, v.err
}

func (v *replayVerifierStub) SetProviderCursor(
	_ context.Context,
	cassetteID string,
	targets []sessionreplay.ProviderUnitPosition,
) (AgentSessionReplayPlaybackState, error) {
	v.cassetteID = cassetteID
	v.lastCommand = "provider-cursor"
	v.state.ProviderConnections = append(
		[]sessionreplay.ProviderUnitPosition(nil),
		targets...,
	)
	return v.state, v.err
}

func (v *replayVerifierStub) ClearProviderCursor(
	_ context.Context,
	cassetteID string,
) (AgentSessionReplayPlaybackState, error) {
	v.cassetteID = cassetteID
	v.lastCommand = "clear-provider-cursor"
	v.state.ProviderConnections = nil
	return v.state, v.err
}

func TestVerifyAgentSessionReplayTransportUsesCassetteIdentityAndFailsClosed(t *testing.T) {
	cassetteID := uuid.MustParse("277377ed-af34-454f-a8b9-1047b4064e74")
	request := tuttigenerated.VerifyAgentSessionReplayTransportRequestObject{
		CassetteID: cassetteID,
	}
	unavailable, err := (DaemonAPI{}).VerifyAgentSessionReplayTransport(
		context.Background(),
		request,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := unavailable.(tuttigenerated.VerifyAgentSessionReplayTransport503JSONResponse); !ok {
		t.Fatalf("unavailable response = %T, want 503", unavailable)
	}

	verifier := &replayVerifierStub{err: errors.New("leftover frame")}
	mismatch, err := (DaemonAPI{
		AgentSessionReplayVerifier: verifier,
	}).VerifyAgentSessionReplayTransport(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := mismatch.(tuttigenerated.VerifyAgentSessionReplayTransport409JSONResponse); !ok {
		t.Fatalf("mismatch response = %T, want 409", mismatch)
	}
	if verifier.cassetteID != cassetteID.String() {
		t.Fatalf("cassetteID = %q", verifier.cassetteID)
	}

	verifier.err = nil
	verified, err := (DaemonAPI{
		AgentSessionReplayVerifier: verifier,
	}).VerifyAgentSessionReplayTransport(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := verified.(tuttigenerated.VerifyAgentSessionReplayTransport204Response); !ok {
		t.Fatalf("verified response = %T, want 204", verified)
	}
}

func TestVerifyAgentSessionReplayCheckpointReturnsSemanticAndCanonicalState(
	t *testing.T,
) {
	cassetteID := uuid.MustParse("277377ed-af34-454f-a8b9-1047b4064e74")
	request := tuttigenerated.VerifyAgentSessionReplayCheckpointRequestObject{
		CassetteID:      cassetteID,
		CheckpointIndex: 3,
	}
	unavailable, err := (DaemonAPI{}).VerifyAgentSessionReplayCheckpoint(
		context.Background(),
		request,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := unavailable.(tuttigenerated.VerifyAgentSessionReplayCheckpoint503JSONResponse); !ok {
		t.Fatalf("unavailable response = %T, want 503", unavailable)
	}
	verifier := &replayVerifierStub{
		checkpointState: AgentSessionReplayCheckpointState{
			TriggerMatched:                  true,
			ReadinessSatisfied:              true,
			CanonicalSessionUpdatedAtUnixMS: 123,
			CanonicalMessageVersion:         7,
		},
	}
	response, err := (DaemonAPI{
		AgentSessionReplayVerifier: verifier,
	}).VerifyAgentSessionReplayCheckpoint(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	verified, ok := response.(tuttigenerated.VerifyAgentSessionReplayCheckpoint200JSONResponse)
	if !ok ||
		!verified.TriggerMatched ||
		!verified.ReadinessSatisfied ||
		verified.CanonicalSessionUpdatedAtUnixMs != 123 ||
		verified.CanonicalMessageVersion != 7 ||
		verifier.cassetteID != cassetteID.String() ||
		verifier.checkpointIndex != 3 {
		t.Fatalf("response=%#v verifier=%#v", response, verifier)
	}
	verifier.err = errors.New("checkpoint_trigger_mismatch")
	mismatch, err := (DaemonAPI{
		AgentSessionReplayVerifier: verifier,
	}).VerifyAgentSessionReplayCheckpoint(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := mismatch.(tuttigenerated.VerifyAgentSessionReplayCheckpoint409JSONResponse); !ok {
		t.Fatalf("mismatch response = %T, want 409", mismatch)
	}
}

func TestPrepareAgentSessionReplayWorkspaceReturnsCassetteLaunches(t *testing.T) {
	const workspaceID = "934219f8-5fa2-4d28-aaf0-420a73d45847"
	cassetteA := uuid.MustParse("277377ed-af34-454f-a8b9-1047b4064e74")
	cassetteB := uuid.MustParse("83b07875-4c0d-4d41-a878-08895dc2e394")
	service := &replayWorkspacePrepareServiceStub{
		prepared: agentsessionreplay.ReplayWorkspaceRequest{
			Cassettes: []agentsessionreplay.ReplayWorkspaceCassette{
				{
					Cassette: agentsessionreplay.Cassette{
						ID:                 cassetteA.String(),
						RootAgentSessionID: "agent-session-a",
					},
					Layout: agentsessionreplay.ArtifactLayout{StorageKey: "/cassette/a"},
				},
				{
					Cassette: agentsessionreplay.Cassette{
						ID:                 cassetteB.String(),
						RootAgentSessionID: "agent-session-b",
					},
					Layout: agentsessionreplay.ArtifactLayout{StorageKey: "/cassette/b"},
				},
			},
		},
	}
	response, err := (DaemonAPI{
		AgentSessionRecordingService: service,
	}).PrepareAgentSessionReplayWorkspace(
		context.Background(),
		tuttigenerated.PrepareAgentSessionReplayWorkspaceRequestObject{
			WorkspaceID: workspaceID,
			Body: &tuttigenerated.PrepareAgentSessionReplayWorkspaceRequest{
				CassetteIds: []uuid.UUID{cassetteA, cassetteB},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	created, ok := response.(tuttigenerated.PrepareAgentSessionReplayWorkspace201JSONResponse)
	if !ok || len(created.Launches) != 2 {
		t.Fatalf("response = %#v", response)
	}
	if service.workspaceID != workspaceID ||
		len(service.cassetteIDs) != 2 ||
		created.Launches[0].CassetteId != cassetteA ||
		created.Launches[0].RootAgentSessionId != "agent-session-a" ||
		created.Launches[1].CassetteDirectory != "/cassette/b" {
		t.Fatalf("service=%#v response=%#v", service, created)
	}
}

func TestPrepareAgentSessionReplayWorkspaceRejectsDuplicateCassetteIDs(t *testing.T) {
	const workspaceID = "934219f8-5fa2-4d28-aaf0-420a73d45847"
	cassetteID := uuid.MustParse("277377ed-af34-454f-a8b9-1047b4064e74")
	service := &replayWorkspacePrepareServiceStub{}
	response, err := (DaemonAPI{
		AgentSessionRecordingService: service,
	}).PrepareAgentSessionReplayWorkspace(
		context.Background(),
		tuttigenerated.PrepareAgentSessionReplayWorkspaceRequestObject{
			WorkspaceID: workspaceID,
			Body: &tuttigenerated.PrepareAgentSessionReplayWorkspaceRequest{
				CassetteIds: []uuid.UUID{cassetteID, cassetteID},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := response.(tuttigenerated.PrepareAgentSessionReplayWorkspace400JSONResponse); !ok {
		t.Fatalf("response = %#v, want 400", response)
	}
	if service.workspaceID != "" || len(service.cassetteIDs) != 0 {
		t.Fatalf("service called with workspace=%q cassettes=%#v", service.workspaceID, service.cassetteIDs)
	}
}

func TestAgentSessionReplayTransportPlaybackUsesCassetteIdentity(t *testing.T) {
	cassetteID := uuid.MustParse("277377ed-af34-454f-a8b9-1047b4064e74")
	unavailable, err := (DaemonAPI{}).GetAgentSessionReplayTransportPlayback(
		context.Background(),
		tuttigenerated.GetAgentSessionReplayTransportPlaybackRequestObject{
			CassetteID: cassetteID,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := unavailable.(tuttigenerated.GetAgentSessionReplayTransportPlayback503JSONResponse); !ok {
		t.Fatalf("unavailable response = %T, want 503", unavailable)
	}

	verifier := &replayVerifierStub{
		state: AgentSessionReplayPlaybackState{
			Speed:             1,
			PlaybackElapsedMS: 42,
			Drained:           true,
		},
	}
	ready := DaemonAPI{AgentSessionReplayVerifier: verifier}
	current, err := ready.GetAgentSessionReplayTransportPlayback(
		context.Background(),
		tuttigenerated.GetAgentSessionReplayTransportPlaybackRequestObject{
			CassetteID: cassetteID,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if response, ok := current.(tuttigenerated.GetAgentSessionReplayTransportPlayback200JSONResponse); !ok ||
		response.Speed != 1 || response.PlaybackElapsedMs != 42 ||
		!response.Drained || response.Paused ||
		response.TimingMode != tuttigenerated.AgentSessionReplayTransportPlaybackTimingModeRealtime ||
		verifier.cassetteID != cassetteID.String() {
		t.Fatalf("current response = %#v cassetteID=%q", current, verifier.cassetteID)
	}

	speed := tuttigenerated.UpdateAgentSessionReplayTransportPlaybackRequestSpeedN2
	updated, err := ready.UpdateAgentSessionReplayTransportPlayback(
		context.Background(),
		tuttigenerated.UpdateAgentSessionReplayTransportPlaybackRequestObject{
			CassetteID: cassetteID,
			Body: &tuttigenerated.UpdateAgentSessionReplayTransportPlaybackRequest{
				Command: tuttigenerated.UpdateAgentSessionReplayTransportPlaybackRequestCommandSetSpeed,
				Speed:   &speed,
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if response, ok := updated.(tuttigenerated.UpdateAgentSessionReplayTransportPlayback200JSONResponse); !ok ||
		response.Speed != 2 || !response.Drained || verifier.lastCommand != "speed" ||
		verifier.cassetteID != cassetteID.String() {
		t.Fatalf("updated response = %#v verifier=%#v", updated, verifier)
	}
}

func TestListAgentSessionCassettesScopesAndMapsCatalogEntries(t *testing.T) {
	const workspaceID = "934219f8-5fa2-4d28-aaf0-420a73d45847"
	service := &replayCassetteListServiceStub{
		cassettes: []agentsessionreplay.Cassette{{
			ID:                 "277377ed-af34-454f-a8b9-1047b4064e74",
			Name:               "checkout regression",
			SourceRecordingID:  "54f46b5c-34e5-40e2-8147-361bb0d046dc",
			AgentTargetID:      "local:codex",
			RootAgentSessionID: "session-1",
			Mode:               agentsessionreplay.ScenarioModeCreateSession,
			TotalBytes:         42,
			CreatedAtUnixMS:    123,
		}},
	}
	response, err := (DaemonAPI{
		AgentSessionRecordingService: service,
	}).ListAgentSessionCassettes(
		context.Background(),
		tuttigenerated.ListAgentSessionCassettesRequestObject{
			WorkspaceID: workspaceID,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	listed, ok := response.(tuttigenerated.ListAgentSessionCassettes200JSONResponse)
	if !ok || len(listed.Cassettes) != 1 {
		t.Fatalf("response = %#v, want one cassette", response)
	}
	if service.workspaceID != workspaceID ||
		listed.Cassettes[0].Name != "checkout regression" ||
		listed.Cassettes[0].TotalBytes != 42 {
		t.Fatalf("workspace=%q cassette=%#v", service.workspaceID, listed.Cassettes[0])
	}
}
