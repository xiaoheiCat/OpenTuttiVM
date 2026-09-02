package workspace

import (
	"context"
	"errors"
	"testing"

	agentsessionreplay "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/agentsessionreplay"
)

func TestAgentSessionReplayMetadataPersistsRecordingAndCassette(t *testing.T) {
	store := openTestSQLiteStore(t)
	ctx := context.Background()
	var obsoleteTableCount int
	if err := store.readDB.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM sqlite_master
WHERE type = 'table' AND name = 'agent_session_replay_runs'
`).Scan(&obsoleteTableCount); err != nil {
		t.Fatal(err)
	}
	if obsoleteTableCount != 0 {
		t.Fatal("fresh Replay schema created the obsolete replay table")
	}
	recording := agentsessionreplay.Recording{
		ID:            "recording-1",
		Name:          "2026-07-28T10:00:00.000Z",
		ScopeID:       "workspace-1",
		AgentTargetID: "local:codex",
		ReplayPrerequisites: agentsessionreplay.ReplayPrerequisites{
			ComposerDefaults: agentsessionreplay.ReplayComposerDefaults{
				Model: "gpt-5.4", PermissionModeID: "default",
				ReasoningEffort: "medium", Speed: "normal",
			},
		},
		Mode:               agentsessionreplay.ScenarioModeCreateSession,
		RootAgentSessionID: "session-1",
		Status:             agentsessionreplay.StatusRecording,
		CreatedAtUnixMS:    10,
		UpdatedAtUnixMS:    20,
	}
	if err := store.PutRecording(ctx, recording); err != nil {
		t.Fatal(err)
	}
	gotRecording, err := store.GetRecording(ctx, recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if gotRecording.ArtifactKey != "" ||
		gotRecording.Status != agentsessionreplay.StatusRecording ||
		gotRecording.ReplayPrerequisites != recording.ReplayPrerequisites {
		t.Fatalf("recording = %#v", gotRecording)
	}

	recording.Status = agentsessionreplay.StatusComplete
	recording.CassetteID = "cassette-1"
	recording.StoppedAtUnixMS = 30
	recording.UpdatedAtUnixMS = 30
	cassette := agentsessionreplay.Cassette{
		ID:                 recording.CassetteID,
		Name:               recording.Name,
		SourceRecordingID:  recording.ID,
		AgentTargetID:      recording.AgentTargetID,
		RootAgentSessionID: recording.RootAgentSessionID,
		Mode:               recording.Mode,
		TotalBytes:         1234,
		ManifestSHA256:     "manifest-digest",
		CreatedAtUnixMS:    30,
	}
	if err := store.PublishCassette(ctx, recording, cassette); err != nil {
		t.Fatal(err)
	}
	cassettes, err := store.ListCassettes(ctx, recording.ScopeID)
	if err != nil {
		t.Fatal(err)
	}
	if len(cassettes) != 1 || cassettes[0].SourceRecordingID != recording.ID {
		t.Fatalf("cassettes = %#v", cassettes)
	}
	recording.Name = "checkout regression"
	recording.UpdatedAtUnixMS = 31
	cassette.Name = recording.Name
	cassette.ManifestSHA256 = "renamed-manifest-digest"
	if err := store.UpdateCassette(ctx, recording, cassette); err != nil {
		t.Fatal(err)
	}
	gotRecording, err = store.GetRecording(ctx, recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	gotCassette, err := store.GetCassette(ctx, cassette.ID)
	if err != nil {
		t.Fatal(err)
	}
	if gotRecording.Name != recording.Name ||
		gotCassette.Name != cassette.Name ||
		gotCassette.ManifestSHA256 != cassette.ManifestSHA256 {
		t.Fatalf("recording=%#v cassette=%#v", gotRecording, gotCassette)
	}
}

func TestAgentSessionReplayMetadataReturnsDomainNotFoundErrors(t *testing.T) {
	store := openTestSQLiteStore(t)
	ctx := context.Background()
	if _, err := store.GetRecording(ctx, "missing"); !errors.Is(err, agentsessionreplay.ErrNotFound) {
		t.Fatalf("GetRecording() error = %v", err)
	}
	if _, err := store.GetCassette(ctx, "missing"); !errors.Is(err, agentsessionreplay.ErrCassetteNotFound) {
		t.Fatalf("GetCassette() error = %v", err)
	}
}

func TestAgentSessionReplayMetadataDeletesCanceledRecording(t *testing.T) {
	store := openTestSQLiteStore(t)
	ctx := context.Background()
	recording := agentsessionreplay.Recording{
		ID:              "recording-1",
		Name:            "2026-07-28T10:00:00.000Z",
		ScopeID:         "workspace-1",
		AgentTargetID:   "local:codex",
		Mode:            agentsessionreplay.ScenarioModeCreateSession,
		Status:          agentsessionreplay.StatusReady,
		CreatedAtUnixMS: 10,
		UpdatedAtUnixMS: 10,
	}
	if err := store.PutRecording(ctx, recording); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteRecording(ctx, recording.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetRecording(ctx, recording.ID); !errors.Is(err, agentsessionreplay.ErrNotFound) {
		t.Fatalf("GetRecording() error = %v", err)
	}
	recordings, err := store.ListRecordings(ctx, recording.ScopeID)
	if err != nil {
		t.Fatal(err)
	}
	if len(recordings) != 0 {
		t.Fatalf("recordings = %#v", recordings)
	}
}

func TestAgentSessionReplayMetadataDeletesRecordingAndCassette(t *testing.T) {
	store := openTestSQLiteStore(t)
	ctx := context.Background()
	recording := agentsessionreplay.Recording{
		ID: "recording-1", Name: "checkout", CassetteID: "cassette-1",
		ScopeID: "workspace-1", AgentTargetID: "local:codex",
		Mode:   agentsessionreplay.ScenarioModeCreateSession,
		Status: agentsessionreplay.StatusComplete, CreatedAtUnixMS: 10,
		UpdatedAtUnixMS: 10,
	}
	cassette := agentsessionreplay.Cassette{
		ID: "cassette-1", Name: "checkout", SourceRecordingID: recording.ID,
		AgentTargetID:      recording.AgentTargetID,
		RootAgentSessionID: "session-1", Mode: recording.Mode,
		CreatedAtUnixMS: 10,
	}
	if err := store.PublishCassette(ctx, recording, cassette); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteRecording(ctx, recording.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetRecording(ctx, recording.ID); !errors.Is(err, agentsessionreplay.ErrNotFound) {
		t.Fatalf("GetRecording() error = %v", err)
	}
	if _, err := store.GetCassette(ctx, cassette.ID); !errors.Is(err, agentsessionreplay.ErrCassetteNotFound) {
		t.Fatalf("GetCassette() error = %v", err)
	}
}
