package api

import (
	"context"
	"errors"
	"strings"

	"github.com/google/uuid"
	sessionreplay "github.com/xiaoheiCat/OpenTuttiVM/packages/agent/session-replay"
	tuttigenerated "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/api/generated"
	"github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/apierrors"
	agentsessionreplay "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/agentsessionreplay"
)

type AgentSessionReplayVerifier interface {
	Verify(context.Context, string) error
	VerifyCheckpoint(
		context.Context,
		string,
		int,
	) (AgentSessionReplayCheckpointState, error)
	PlaybackState(context.Context, string) (AgentSessionReplayPlaybackState, error)
	SetPlaybackSpeed(context.Context, string, float64) (AgentSessionReplayPlaybackState, error)
	SetPlaybackPaused(context.Context, string, bool) (AgentSessionReplayPlaybackState, error)
	SetPlaybackFastForward(context.Context, string, bool) (AgentSessionReplayPlaybackState, error)
	SetProviderCursor(context.Context, string, []sessionreplay.ProviderUnitPosition) (AgentSessionReplayPlaybackState, error)
	ClearProviderCursor(context.Context, string) (AgentSessionReplayPlaybackState, error)
}

type AgentSessionReplayCheckpointState struct {
	TriggerMatched                  bool
	ReadinessSatisfied              bool
	CanonicalSessionUpdatedAtUnixMS int64
	CanonicalMessageVersion         uint64
}

type AgentSessionReplayPlaybackState struct {
	Speed               float64
	PlaybackElapsedMS   float64
	Drained             bool
	Paused              bool
	FastForward         bool
	ProviderConnections []sessionreplay.ProviderUnitPosition
}

func (api DaemonAPI) VerifyAgentSessionReplayTransport(
	ctx context.Context,
	request tuttigenerated.VerifyAgentSessionReplayTransportRequestObject,
) (tuttigenerated.VerifyAgentSessionReplayTransportResponseObject, error) {
	if api.AgentSessionReplayVerifier == nil {
		return tuttigenerated.VerifyAgentSessionReplayTransport503JSONResponse{
			ServiceUnavailableErrorJSONResponse: serviceUnavailableError(
				apierrors.ServiceUnavailable("agent_session_replay_verifier_unavailable"),
			),
		}, nil
	}
	if err := api.AgentSessionReplayVerifier.Verify(ctx, request.CassetteID.String()); err != nil {
		return tuttigenerated.VerifyAgentSessionReplayTransport409JSONResponse(
			agentSessionRecordingError("agent_session_replay_transport_mismatch", err),
		), nil
	}
	return tuttigenerated.VerifyAgentSessionReplayTransport204Response{}, nil
}

func (api DaemonAPI) VerifyAgentSessionReplayCheckpoint(
	ctx context.Context,
	request tuttigenerated.VerifyAgentSessionReplayCheckpointRequestObject,
) (tuttigenerated.VerifyAgentSessionReplayCheckpointResponseObject, error) {
	if api.AgentSessionReplayVerifier == nil {
		return tuttigenerated.VerifyAgentSessionReplayCheckpoint503JSONResponse{
			ServiceUnavailableErrorJSONResponse: serviceUnavailableError(
				apierrors.ServiceUnavailable(
					"agent_session_replay_checkpoint_verifier_unavailable",
				),
			),
		}, nil
	}
	state, err := api.AgentSessionReplayVerifier.VerifyCheckpoint(
		ctx,
		request.CassetteID.String(),
		request.CheckpointIndex,
	)
	if err != nil {
		return tuttigenerated.VerifyAgentSessionReplayCheckpoint409JSONResponse(
			agentSessionRecordingError(
				"agent_session_replay_checkpoint_mismatch",
				err,
			),
		), nil
	}
	return tuttigenerated.VerifyAgentSessionReplayCheckpoint200JSONResponse{
		CheckpointIndex:                 request.CheckpointIndex,
		TriggerMatched:                  state.TriggerMatched,
		ReadinessSatisfied:              state.ReadinessSatisfied,
		CanonicalSessionUpdatedAtUnixMs: state.CanonicalSessionUpdatedAtUnixMS,
		CanonicalMessageVersion: int64(
			state.CanonicalMessageVersion,
		),
	}, nil
}

func (api DaemonAPI) GetAgentSessionReplayTransportPlayback(
	ctx context.Context,
	request tuttigenerated.GetAgentSessionReplayTransportPlaybackRequestObject,
) (tuttigenerated.GetAgentSessionReplayTransportPlaybackResponseObject, error) {
	if api.AgentSessionReplayVerifier == nil {
		return agentSessionReplayPlaybackUnavailable(), nil
	}
	state, err := api.AgentSessionReplayVerifier.PlaybackState(
		ctx,
		request.CassetteID.String(),
	)
	if err != nil {
		return agentSessionReplayPlaybackUnavailable(), nil
	}
	return tuttigenerated.GetAgentSessionReplayTransportPlayback200JSONResponse(
		generatedAgentSessionReplayTransportPlayback(state),
	), nil
}

func (api DaemonAPI) UpdateAgentSessionReplayTransportPlayback(
	ctx context.Context,
	request tuttigenerated.UpdateAgentSessionReplayTransportPlaybackRequestObject,
) (tuttigenerated.UpdateAgentSessionReplayTransportPlaybackResponseObject, error) {
	if !validAgentSessionReplayPlaybackCommand(request.Body) {
		return tuttigenerated.UpdateAgentSessionReplayTransportPlayback400JSONResponse{
			InvalidRequestErrorJSONResponse: invalidRequestError(apierrors.MalformedRequest()),
		}, nil
	}
	if api.AgentSessionReplayVerifier == nil {
		return agentSessionReplayPlaybackUpdateUnavailable(), nil
	}
	cassetteID := request.CassetteID.String()
	var (
		state AgentSessionReplayPlaybackState
		err   error
	)
	switch request.Body.Command {
	case tuttigenerated.UpdateAgentSessionReplayTransportPlaybackRequestCommandSetSpeed:
		state, err = api.AgentSessionReplayVerifier.SetPlaybackSpeed(
			ctx,
			cassetteID,
			float64(*request.Body.Speed),
		)
	case tuttigenerated.UpdateAgentSessionReplayTransportPlaybackRequestCommandPause:
		state, err = api.AgentSessionReplayVerifier.SetPlaybackPaused(ctx, cassetteID, true)
	case tuttigenerated.UpdateAgentSessionReplayTransportPlaybackRequestCommandResume:
		state, err = api.AgentSessionReplayVerifier.SetPlaybackPaused(ctx, cassetteID, false)
	case tuttigenerated.UpdateAgentSessionReplayTransportPlaybackRequestCommandSetTimingMode:
		state, err = api.AgentSessionReplayVerifier.SetPlaybackFastForward(
			ctx,
			cassetteID,
			*request.Body.TimingMode ==
				tuttigenerated.UpdateAgentSessionReplayTransportPlaybackRequestTimingModeFastForward,
		)
	case tuttigenerated.UpdateAgentSessionReplayTransportPlaybackRequestCommandSetProviderCursor:
		targets := make(
			[]sessionreplay.ProviderUnitPosition,
			0,
			len(*request.Body.ProviderConnections),
		)
		for _, target := range *request.Body.ProviderConnections {
			targets = append(targets, sessionreplay.ProviderUnitPosition{
				ConnectionID: target.ConnectionId,
				ChunkSeq:     uint64(target.ChunkSeq),
				UnitIndex:    uint64(target.UnitIndex),
			})
		}
		state, err = api.AgentSessionReplayVerifier.SetProviderCursor(
			ctx,
			cassetteID,
			targets,
		)
	case tuttigenerated.UpdateAgentSessionReplayTransportPlaybackRequestCommandClearProviderCursor:
		state, err = api.AgentSessionReplayVerifier.ClearProviderCursor(ctx, cassetteID)
	}
	if err != nil {
		return agentSessionReplayPlaybackUpdateUnavailable(), nil
	}
	return tuttigenerated.UpdateAgentSessionReplayTransportPlayback200JSONResponse(
		generatedAgentSessionReplayTransportPlayback(state),
	), nil
}

func validAgentSessionReplayPlaybackCommand(
	body *tuttigenerated.UpdateAgentSessionReplayTransportPlaybackRequest,
) bool {
	if body == nil || !body.Command.Valid() {
		return false
	}
	switch body.Command {
	case tuttigenerated.UpdateAgentSessionReplayTransportPlaybackRequestCommandSetSpeed:
		return body.Speed != nil && body.Speed.Valid() && body.TimingMode == nil &&
			body.ProviderConnections == nil
	case tuttigenerated.UpdateAgentSessionReplayTransportPlaybackRequestCommandPause,
		tuttigenerated.UpdateAgentSessionReplayTransportPlaybackRequestCommandResume:
		return body.Speed == nil && body.TimingMode == nil &&
			body.ProviderConnections == nil
	case tuttigenerated.UpdateAgentSessionReplayTransportPlaybackRequestCommandSetTimingMode:
		return body.Speed == nil && body.TimingMode != nil && body.TimingMode.Valid() &&
			body.ProviderConnections == nil
	case tuttigenerated.UpdateAgentSessionReplayTransportPlaybackRequestCommandSetProviderCursor:
		return body.Speed == nil && body.TimingMode == nil &&
			body.ProviderConnections != nil
	case tuttigenerated.UpdateAgentSessionReplayTransportPlaybackRequestCommandClearProviderCursor:
		return body.Speed == nil && body.TimingMode == nil &&
			body.ProviderConnections == nil
	default:
		return false
	}
}

func generatedAgentSessionReplayTransportPlayback(
	state AgentSessionReplayPlaybackState,
) tuttigenerated.AgentSessionReplayTransportPlayback {
	timingMode := tuttigenerated.AgentSessionReplayTransportPlaybackTimingModeRealtime
	if state.FastForward {
		timingMode = tuttigenerated.AgentSessionReplayTransportPlaybackTimingModeFastForward
	}
	return tuttigenerated.AgentSessionReplayTransportPlayback{
		Drained:             state.Drained,
		Paused:              state.Paused,
		PlaybackElapsedMs:   state.PlaybackElapsedMS,
		Speed:               tuttigenerated.AgentSessionReplayTransportPlaybackSpeed(state.Speed),
		TimingMode:          timingMode,
		ProviderConnections: generatedProviderConnections(state.ProviderConnections),
	}
}

func generatedProviderConnections(
	positions []sessionreplay.ProviderUnitPosition,
) []tuttigenerated.AgentSessionReplayProviderCursor {
	result := make(
		[]tuttigenerated.AgentSessionReplayProviderCursor,
		0,
		len(positions),
	)
	for _, position := range positions {
		result = append(result, tuttigenerated.AgentSessionReplayProviderCursor{
			ConnectionId: position.ConnectionID,
			ChunkSeq:     int64(position.ChunkSeq),
			UnitIndex:    int64(position.UnitIndex),
		})
	}
	return result
}

func agentSessionReplayPlaybackUnavailable() tuttigenerated.GetAgentSessionReplayTransportPlayback503JSONResponse {
	return tuttigenerated.GetAgentSessionReplayTransportPlayback503JSONResponse{
		ServiceUnavailableErrorJSONResponse: serviceUnavailableError(
			apierrors.ServiceUnavailable("agent_session_replay_playback_unavailable"),
		),
	}
}

func agentSessionReplayPlaybackUpdateUnavailable() tuttigenerated.UpdateAgentSessionReplayTransportPlayback503JSONResponse {
	return tuttigenerated.UpdateAgentSessionReplayTransportPlayback503JSONResponse{
		ServiceUnavailableErrorJSONResponse: serviceUnavailableError(
			apierrors.ServiceUnavailable("agent_session_replay_playback_unavailable"),
		),
	}
}

func (api DaemonAPI) PrepareAgentSessionReplayWorkspace(
	ctx context.Context,
	request tuttigenerated.PrepareAgentSessionReplayWorkspaceRequestObject,
) (tuttigenerated.PrepareAgentSessionReplayWorkspaceResponseObject, error) {
	if api.AgentSessionRecordingService == nil {
		return tuttigenerated.PrepareAgentSessionReplayWorkspace503JSONResponse{
			ServiceUnavailableErrorJSONResponse: agentSessionRecordingUnavailableError(),
		}, nil
	}
	if request.Body == nil || len(request.Body.CassetteIds) == 0 {
		return tuttigenerated.PrepareAgentSessionReplayWorkspace400JSONResponse{
			InvalidRequestErrorJSONResponse: invalidRequestError(apierrors.MalformedRequest()),
		}, nil
	}
	cassetteIDs := make([]string, 0, len(request.Body.CassetteIds))
	seenCassetteIDs := make(map[string]struct{}, len(request.Body.CassetteIds))
	for _, cassetteID := range request.Body.CassetteIds {
		value := cassetteID.String()
		if _, exists := seenCassetteIDs[value]; exists {
			return tuttigenerated.PrepareAgentSessionReplayWorkspace400JSONResponse{
				InvalidRequestErrorJSONResponse: invalidRequestError(apierrors.MalformedRequest()),
			}, nil
		}
		seenCassetteIDs[value] = struct{}{}
		cassetteIDs = append(cassetteIDs, value)
	}
	prepared, err := api.AgentSessionRecordingService.PrepareReplayWorkspace(
		ctx,
		string(request.WorkspaceID),
		cassetteIDs,
	)
	if err != nil {
		if errors.Is(err, agentsessionreplay.ErrCassetteNotFound) {
			return tuttigenerated.PrepareAgentSessionReplayWorkspace404JSONResponse(
				agentSessionRecordingError("agent_session_cassette_not_found", err),
			), nil
		}
		return tuttigenerated.PrepareAgentSessionReplayWorkspace409JSONResponse(
			agentSessionRecordingError("agent_session_replay_workspace_conflict", err),
		), nil
	}
	launches := make(
		[]tuttigenerated.AgentSessionReplayCassetteLaunch,
		0,
		len(prepared.Cassettes),
	)
	for _, preparedCassette := range prepared.Cassettes {
		cassetteID, parseErr := uuid.Parse(preparedCassette.Cassette.ID)
		if parseErr != nil {
			return nil, parseErr
		}
		launches = append(launches, tuttigenerated.AgentSessionReplayCassetteLaunch{
			CassetteId:         cassetteID,
			CassetteDirectory:  preparedCassette.Layout.StorageKey,
			RootAgentSessionId: preparedCassette.Cassette.RootAgentSessionID,
		})
	}
	return tuttigenerated.PrepareAgentSessionReplayWorkspace201JSONResponse{
		Launches: launches,
	}, nil
}

func (api DaemonAPI) ListAgentSessionCassettes(
	ctx context.Context,
	request tuttigenerated.ListAgentSessionCassettesRequestObject,
) (tuttigenerated.ListAgentSessionCassettesResponseObject, error) {
	if api.AgentSessionRecordingService == nil {
		return tuttigenerated.ListAgentSessionCassettes503JSONResponse{
			ServiceUnavailableErrorJSONResponse: agentSessionRecordingUnavailableError(),
		}, nil
	}
	cassettes, err := api.AgentSessionRecordingService.ListCassettes(
		ctx,
		string(request.WorkspaceID),
	)
	if err != nil {
		return tuttigenerated.ListAgentSessionCassettes503JSONResponse{
			ServiceUnavailableErrorJSONResponse: serviceUnavailableError(
				apierrors.ServiceUnavailable(
					"agent_session_cassette_list_failed",
					apierrors.WithCause(err),
				),
			),
		}, nil
	}
	generated := make([]tuttigenerated.AgentSessionCassette, 0, len(cassettes))
	for _, cassette := range cassettes {
		item, generateErr := generatedAgentSessionCassette(cassette)
		if generateErr != nil {
			return nil, generateErr
		}
		generated = append(generated, item)
	}
	return tuttigenerated.ListAgentSessionCassettes200JSONResponse{
		Cassettes: generated,
	}, nil
}

func (api DaemonAPI) ImportAgentSessionCassettes(
	ctx context.Context,
	request tuttigenerated.ImportAgentSessionCassettesRequestObject,
) (tuttigenerated.ImportAgentSessionCassettesResponseObject, error) {
	if api.AgentSessionRecordingService == nil {
		return tuttigenerated.ImportAgentSessionCassettes503JSONResponse{
			ServiceUnavailableErrorJSONResponse: agentSessionRecordingUnavailableError(),
		}, nil
	}
	if request.Body == nil ||
		len(request.Body.SourceDirectories) == 0 ||
		len(request.Body.SourceDirectories) > 100 {
		return tuttigenerated.ImportAgentSessionCassettes400JSONResponse{
			InvalidRequestErrorJSONResponse: invalidRequestError(apierrors.MalformedRequest()),
		}, nil
	}
	seen := make(map[string]struct{}, len(request.Body.SourceDirectories))
	for _, sourceDirectory := range request.Body.SourceDirectories {
		sourceDirectory = strings.TrimSpace(sourceDirectory)
		if sourceDirectory == "" {
			return tuttigenerated.ImportAgentSessionCassettes400JSONResponse{
				InvalidRequestErrorJSONResponse: invalidRequestError(apierrors.MalformedRequest()),
			}, nil
		}
		if _, exists := seen[sourceDirectory]; exists {
			return tuttigenerated.ImportAgentSessionCassettes400JSONResponse{
				InvalidRequestErrorJSONResponse: invalidRequestError(apierrors.MalformedRequest()),
			}, nil
		}
		seen[sourceDirectory] = struct{}{}
	}
	result, err := api.AgentSessionRecordingService.Import(
		ctx,
		agentsessionreplay.ImportInput{
			WorkspaceID:       string(request.WorkspaceID),
			SourceDirectories: request.Body.SourceDirectories,
		},
	)
	if err != nil {
		return tuttigenerated.ImportAgentSessionCassettes503JSONResponse{
			ServiceUnavailableErrorJSONResponse: serviceUnavailableError(
				apierrors.ServiceUnavailable(
					"agent_session_cassette_import_failed",
					apierrors.WithCause(err),
				),
			),
		}, nil
	}
	generated := make([]tuttigenerated.AgentSessionRecording, 0, len(result.Recordings))
	for _, recording := range result.Recordings {
		item, generateErr := generatedAgentSessionRecording(recording)
		if generateErr != nil {
			return nil, generateErr
		}
		generated = append(generated, item)
	}
	failures := make(
		[]tuttigenerated.AgentSessionCassetteImportFailure,
		0,
		len(result.Failures),
	)
	for _, failure := range result.Failures {
		failures = append(failures, tuttigenerated.AgentSessionCassetteImportFailure{
			Code:            tuttigenerated.AgentSessionCassetteImportFailureCode(failure.Code),
			SourceDirectory: failure.SourceDirectory,
		})
	}
	return tuttigenerated.ImportAgentSessionCassettes200JSONResponse{
		Failures:   failures,
		Recordings: generated,
	}, nil
}

func generatedAgentSessionCassette(
	cassette agentsessionreplay.Cassette,
) (tuttigenerated.AgentSessionCassette, error) {
	id, err := uuid.Parse(cassette.ID)
	if err != nil {
		return tuttigenerated.AgentSessionCassette{}, err
	}
	sourceRecordingID, err := uuid.Parse(cassette.SourceRecordingID)
	if err != nil {
		return tuttigenerated.AgentSessionCassette{}, err
	}
	return tuttigenerated.AgentSessionCassette{
		Id:                 id,
		Name:               cassette.Name,
		SourceRecordingId:  sourceRecordingID,
		AgentTargetId:      cassette.AgentTargetID,
		RootAgentSessionId: cassette.RootAgentSessionID,
		Mode:               tuttigenerated.AgentSessionCassetteMode(cassette.Mode),
		TotalBytes:         cassette.TotalBytes,
		CreatedAtUnixMs:    cassette.CreatedAtUnixMS,
	}, nil
}
