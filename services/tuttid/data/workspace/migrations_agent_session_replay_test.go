package workspace

import (
	"context"
	"path/filepath"
	"testing"

	agentsessionreplay "github.com/xiaoheiCat/OpenTuttiVM/packages/agent/session-replay"
)

func TestAgentSessionReplayV3MigratesMainSchema(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "tuttid.db")
	store, err := OpenSQLiteStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()
	if _, err := store.writeDB.ExecContext(ctx, `
CREATE TABLE tuttid_schema_migrations (
  id TEXT PRIMARY KEY,
  applied_at_unix_ms INTEGER NOT NULL
);
CREATE TABLE agent_session_recordings (
  recording_id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  agent_target_id TEXT NOT NULL,
  mode TEXT NOT NULL,
  root_agent_session_id TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL,
  cassette_id TEXT NOT NULL DEFAULT '',
  error_code TEXT NOT NULL DEFAULT '',
  error_message TEXT NOT NULL DEFAULT '',
  created_at_unix_ms INTEGER NOT NULL,
  recording_at_unix_ms INTEGER NOT NULL DEFAULT 0,
  stopped_at_unix_ms INTEGER NOT NULL DEFAULT 0,
  updated_at_unix_ms INTEGER NOT NULL,
  name TEXT NOT NULL DEFAULT ''
);
CREATE TABLE agent_session_cassettes (
  cassette_id TEXT PRIMARY KEY,
  source_recording_id TEXT NOT NULL UNIQUE,
  workspace_id TEXT NOT NULL,
  agent_target_id TEXT NOT NULL,
  root_agent_session_id TEXT NOT NULL,
  mode TEXT NOT NULL,
  total_bytes INTEGER NOT NULL,
  manifest_sha256 TEXT NOT NULL,
  created_at_unix_ms INTEGER NOT NULL,
  name TEXT NOT NULL DEFAULT ''
);
CREATE INDEX idx_agent_session_cassettes_workspace_created
  ON agent_session_cassettes(workspace_id, created_at_unix_ms DESC);
CREATE TABLE agent_session_replay_runs (
  replay_run_id TEXT PRIMARY KEY,
  cassette_id TEXT NOT NULL,
  status TEXT NOT NULL,
  checkpoint INTEGER NOT NULL DEFAULT 0,
  error_code TEXT NOT NULL DEFAULT '',
  error_message TEXT NOT NULL DEFAULT '',
  created_at_unix_ms INTEGER NOT NULL,
  started_at_unix_ms INTEGER NOT NULL DEFAULT 0,
  completed_at_unix_ms INTEGER NOT NULL DEFAULT 0,
  updated_at_unix_ms INTEGER NOT NULL,
  FOREIGN KEY (cassette_id) REFERENCES agent_session_cassettes(cassette_id) ON DELETE CASCADE
);
INSERT INTO tuttid_schema_migrations (id, applied_at_unix_ms)
VALUES ('agent_session_replay_v1', 1), ('agent_session_replay_v2', 2);
INSERT INTO agent_session_recordings (
  recording_id, workspace_id, agent_target_id, mode, root_agent_session_id,
  status, cassette_id, created_at_unix_ms, updated_at_unix_ms, name
) VALUES (
  'recording-1', 'workspace-1', 'local:codex', 'create-session', 'session-1',
  'complete', 'cassette-1', 10, 20, 'Recorded checkout'
);
INSERT INTO agent_session_cassettes (
  cassette_id, source_recording_id, workspace_id, agent_target_id,
  root_agent_session_id, mode, total_bytes, manifest_sha256,
  created_at_unix_ms, name
) VALUES (
  'cassette-1', 'recording-1', 'workspace-1', 'local:codex',
  'session-1', 'create-session', 123, 'digest', 20, 'Recorded checkout'
);
INSERT INTO agent_session_replay_runs (
  replay_run_id, cassette_id, status, created_at_unix_ms, updated_at_unix_ms
) VALUES ('run-1', 'cassette-1', 'complete', 30, 30);
`); err != nil {
		t.Fatal(err)
	}

	if err := store.applyAgentSessionReplayV3(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.applyAgentSessionReplayV3(ctx); err != nil {
		t.Fatalf("second migration call: %v", err)
	}
	if err := store.applyAgentSessionReplayV4(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.applyAgentSessionReplayV4(ctx); err != nil {
		t.Fatalf("second v4 migration call: %v", err)
	}

	hasWorkspaceID, err := store.hasColumn(ctx, "agent_session_cassettes", "workspace_id")
	if err != nil {
		t.Fatal(err)
	}
	if hasWorkspaceID {
		t.Fatal("agent_session_cassettes.workspace_id still exists")
	}
	hasReplayPrerequisites, err := store.hasColumn(
		ctx,
		"agent_session_recordings",
		"replay_prerequisites_json",
	)
	if err != nil {
		t.Fatal(err)
	}
	if !hasReplayPrerequisites {
		t.Fatal("agent_session_recordings.replay_prerequisites_json is missing")
	}
	var obsoleteTableCount int
	if err := store.writeDB.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM sqlite_master
WHERE type = 'table' AND name = 'agent_session_replay_runs'
`).Scan(&obsoleteTableCount); err != nil {
		t.Fatal(err)
	}
	if obsoleteTableCount != 0 {
		t.Fatal("agent_session_replay_runs still exists")
	}
	var cassetteName string
	if err := store.writeDB.QueryRowContext(ctx, `
SELECT name
FROM agent_session_cassettes
WHERE cassette_id = 'cassette-1'
`).Scan(&cassetteName); err != nil {
		t.Fatal(err)
	}
	if cassetteName != "Recorded checkout" {
		t.Fatalf("cassette name = %q", cassetteName)
	}
	recording := agentsessionreplay.Recording{
		ID:            "recording-2",
		Name:          "Recorded search",
		CassetteID:    "cassette-2",
		ScopeID:       "workspace-1",
		AgentTargetID: "local:codex",
		ReplayPrerequisites: agentsessionreplay.ReplayPrerequisites{
			ComposerDefaults: agentsessionreplay.ReplayComposerDefaults{
				Model: "gpt-5.4", PermissionModeID: "default",
				ReasoningEffort: "medium", Speed: "normal",
			},
		},
		Mode:               agentsessionreplay.ScenarioModeCreateSession,
		RootAgentSessionID: "session-2",
		Status:             agentsessionreplay.RecordingStatusComplete,
		CreatedAtUnixMS:    40,
		UpdatedAtUnixMS:    50,
	}
	cassette := agentsessionreplay.Cassette{
		ID:                 recording.CassetteID,
		Name:               recording.Name,
		SourceRecordingID:  recording.ID,
		AgentTargetID:      recording.AgentTargetID,
		RootAgentSessionID: recording.RootAgentSessionID,
		Mode:               recording.Mode,
		ManifestSHA256:     "digest-2",
		CreatedAtUnixMS:    50,
	}
	if err := store.PublishCassette(ctx, recording, cassette); err != nil {
		t.Fatalf("PublishCassette() after migration: %v", err)
	}
	applied, err := store.hasMigration(ctx, schemaMigrationAgentSessionReplayV4)
	if err != nil {
		t.Fatal(err)
	}
	if !applied {
		t.Fatal("Agent Session Replay v4 migration was not recorded")
	}
}
