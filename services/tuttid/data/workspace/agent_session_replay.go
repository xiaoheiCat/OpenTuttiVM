package workspace

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	agentsessionreplay "github.com/xiaoheiCat/OpenTuttiVM/packages/agent/session-replay"
)

func (s *SQLiteStore) PutRecording(
	ctx context.Context,
	recording agentsessionreplay.Recording,
) error {
	if s == nil || s.writeDB == nil {
		return errors.New("workspace database is not initialized")
	}
	if err := putAgentSessionRecording(ctx, s.writeDB, recording); err != nil {
		return fmt.Errorf("put Agent Session Recording: %w", err)
	}
	return nil
}

func (s *SQLiteStore) DeleteRecording(ctx context.Context, recordingID string) error {
	if s == nil || s.writeDB == nil {
		return errors.New("workspace database is not initialized")
	}
	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin delete Agent Session Recording: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(
		ctx,
		`DELETE FROM agent_session_cassettes WHERE source_recording_id = ?`,
		strings.TrimSpace(recordingID),
	); err != nil {
		return fmt.Errorf("delete Agent Session Cassette: %w", err)
	}
	if _, err := tx.ExecContext(
		ctx,
		`DELETE FROM agent_session_recordings WHERE recording_id = ?`,
		strings.TrimSpace(recordingID),
	); err != nil {
		return fmt.Errorf("delete Agent Session Recording: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit Agent Session Recording delete: %w", err)
	}
	return nil
}

func (s *SQLiteStore) PublishCassette(
	ctx context.Context,
	recording agentsessionreplay.Recording,
	cassette agentsessionreplay.Cassette,
) error {
	if s == nil || s.writeDB == nil {
		return errors.New("workspace database is not initialized")
	}
	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin publish Agent Session Cassette: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := putAgentSessionCassette(ctx, tx, cassette); err != nil {
		return fmt.Errorf("put Agent Session Cassette: %w", err)
	}
	if err := putAgentSessionRecording(ctx, tx, recording); err != nil {
		return fmt.Errorf("link Agent Session Recording to Cassette: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit Agent Session Cassette: %w", err)
	}
	return nil
}

func (s *SQLiteStore) UpdateCassette(
	ctx context.Context,
	recording agentsessionreplay.Recording,
	cassette agentsessionreplay.Cassette,
) error {
	if s == nil || s.writeDB == nil {
		return errors.New("workspace database is not initialized")
	}
	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin update Agent Session Cassette: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := putAgentSessionCassette(ctx, tx, cassette); err != nil {
		return fmt.Errorf("update Agent Session Cassette: %w", err)
	}
	if err := putAgentSessionRecording(ctx, tx, recording); err != nil {
		return fmt.Errorf("update Agent Session Recording: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit Agent Session Cassette update: %w", err)
	}
	return nil
}

func putAgentSessionCassette(
	ctx context.Context,
	execer agentSessionReplayExecer,
	cassette agentsessionreplay.Cassette,
) error {
	_, err := execer.ExecContext(ctx, `
INSERT INTO agent_session_cassettes (
  cassette_id, name, source_recording_id, agent_target_id,
  root_agent_session_id, mode, total_bytes, manifest_sha256, created_at_unix_ms
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(cassette_id) DO UPDATE SET
  name = excluded.name,
  source_recording_id = excluded.source_recording_id,
  agent_target_id = excluded.agent_target_id,
  root_agent_session_id = excluded.root_agent_session_id,
  mode = excluded.mode,
  total_bytes = excluded.total_bytes,
  manifest_sha256 = excluded.manifest_sha256,
  created_at_unix_ms = excluded.created_at_unix_ms
`, cassette.ID, cassette.Name, cassette.SourceRecordingID,
		cassette.AgentTargetID, cassette.RootAgentSessionID, cassette.Mode,
		cassette.TotalBytes, cassette.ManifestSHA256, cassette.CreatedAtUnixMS,
	)
	return err
}

type agentSessionReplayExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func putAgentSessionRecording(
	ctx context.Context,
	execer agentSessionReplayExecer,
	recording agentsessionreplay.Recording,
) error {
	prerequisites, err := json.Marshal(recording.ReplayPrerequisites)
	if err != nil {
		return fmt.Errorf("marshal Replay prerequisites: %w", err)
	}
	_, err = execer.ExecContext(ctx, `
INSERT INTO agent_session_recordings (
  recording_id, name, workspace_id, agent_target_id, mode, root_agent_session_id,
  replay_prerequisites_json, status, cassette_id, error_code, error_message, created_at_unix_ms,
  recording_at_unix_ms, stopped_at_unix_ms, updated_at_unix_ms
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(recording_id) DO UPDATE SET
  name = excluded.name,
  workspace_id = excluded.workspace_id,
  agent_target_id = excluded.agent_target_id,
  mode = excluded.mode,
  root_agent_session_id = excluded.root_agent_session_id,
  replay_prerequisites_json = excluded.replay_prerequisites_json,
  status = excluded.status,
  cassette_id = excluded.cassette_id,
  error_code = excluded.error_code,
  error_message = excluded.error_message,
  recording_at_unix_ms = excluded.recording_at_unix_ms,
  stopped_at_unix_ms = excluded.stopped_at_unix_ms,
  updated_at_unix_ms = excluded.updated_at_unix_ms
`, recording.ID, recording.Name, recording.ScopeID, recording.AgentTargetID, recording.Mode,
		recording.RootAgentSessionID, string(prerequisites), recording.Status, recording.CassetteID,
		recording.ErrorCode, recording.ErrorMessage, recording.CreatedAtUnixMS,
		recording.RecordingAtUnixMS, recording.StoppedAtUnixMS, recording.UpdatedAtUnixMS,
	)
	return err
}

func (s *SQLiteStore) GetRecording(
	ctx context.Context,
	recordingID string,
) (agentsessionreplay.Recording, error) {
	if s == nil || s.readDB == nil {
		return agentsessionreplay.Recording{}, errors.New("workspace database is not initialized")
	}
	recording, err := scanAgentSessionRecording(s.readDB.QueryRowContext(ctx, `
SELECT recording_id, name, workspace_id, agent_target_id, mode, root_agent_session_id,
       replay_prerequisites_json, status, cassette_id, error_code, error_message, created_at_unix_ms,
       recording_at_unix_ms, stopped_at_unix_ms, updated_at_unix_ms
FROM agent_session_recordings
WHERE recording_id = ?
`, strings.TrimSpace(recordingID)))
	if errors.Is(err, sql.ErrNoRows) {
		return agentsessionreplay.Recording{}, agentsessionreplay.ErrRecordingNotFound
	}
	return recording, err
}

func (s *SQLiteStore) ListRecordings(
	ctx context.Context,
	workspaceID string,
) ([]agentsessionreplay.Recording, error) {
	if s == nil || s.readDB == nil {
		return nil, errors.New("workspace database is not initialized")
	}
	workspaceID = strings.TrimSpace(workspaceID)
	query := `
SELECT recording_id, name, workspace_id, agent_target_id, mode, root_agent_session_id,
       replay_prerequisites_json, status, cassette_id, error_code, error_message, created_at_unix_ms,
       recording_at_unix_ms, stopped_at_unix_ms, updated_at_unix_ms
FROM agent_session_recordings`
	var args []any
	if workspaceID != "" {
		query += ` WHERE workspace_id = ?`
		args = append(args, workspaceID)
	}
	query += ` ORDER BY updated_at_unix_ms DESC, recording_id`
	rows, err := s.readDB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []agentsessionreplay.Recording{}
	for rows.Next() {
		recording, err := scanAgentSessionRecording(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, recording)
	}
	return result, rows.Err()
}

type agentSessionReplayScanner interface {
	Scan(...any) error
}

func scanAgentSessionRecording(scanner agentSessionReplayScanner) (agentsessionreplay.Recording, error) {
	var recording agentsessionreplay.Recording
	var prerequisites string
	err := scanner.Scan(
		&recording.ID,
		&recording.Name,
		&recording.ScopeID,
		&recording.AgentTargetID,
		&recording.Mode,
		&recording.RootAgentSessionID,
		&prerequisites,
		&recording.Status,
		&recording.CassetteID,
		&recording.ErrorCode,
		&recording.ErrorMessage,
		&recording.CreatedAtUnixMS,
		&recording.RecordingAtUnixMS,
		&recording.StoppedAtUnixMS,
		&recording.UpdatedAtUnixMS,
	)
	if err != nil {
		return recording, err
	}
	if err := json.Unmarshal([]byte(prerequisites), &recording.ReplayPrerequisites); err != nil {
		return agentsessionreplay.Recording{}, fmt.Errorf("decode Replay prerequisites: %w", err)
	}
	return recording, nil
}

func (s *SQLiteStore) GetCassette(
	ctx context.Context,
	cassetteID string,
) (agentsessionreplay.Cassette, error) {
	if s == nil || s.readDB == nil {
		return agentsessionreplay.Cassette{}, errors.New("workspace database is not initialized")
	}
	cassette, err := scanAgentSessionCassette(s.readDB.QueryRowContext(ctx, `
SELECT cassette_id, name, source_recording_id, agent_target_id,
       root_agent_session_id, mode, total_bytes, manifest_sha256, created_at_unix_ms
FROM agent_session_cassettes
WHERE cassette_id = ?
`, strings.TrimSpace(cassetteID)))
	if errors.Is(err, sql.ErrNoRows) {
		return agentsessionreplay.Cassette{}, agentsessionreplay.ErrCassetteNotFound
	}
	return cassette, err
}

func (s *SQLiteStore) ListCassettes(
	ctx context.Context,
	workspaceID string,
) ([]agentsessionreplay.Cassette, error) {
	if s == nil || s.readDB == nil {
		return nil, errors.New("workspace database is not initialized")
	}
	workspaceID = strings.TrimSpace(workspaceID)
	query := `
SELECT c.cassette_id, c.name, c.source_recording_id, c.agent_target_id,
       c.root_agent_session_id, c.mode, c.total_bytes, c.manifest_sha256,
       c.created_at_unix_ms
FROM agent_session_cassettes c
JOIN agent_session_recordings r ON r.recording_id = c.source_recording_id`
	var args []any
	if workspaceID != "" {
		query += ` WHERE r.workspace_id = ?`
		args = append(args, workspaceID)
	}
	query += ` ORDER BY c.created_at_unix_ms DESC, c.cassette_id`
	rows, err := s.readDB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []agentsessionreplay.Cassette{}
	for rows.Next() {
		cassette, err := scanAgentSessionCassette(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, cassette)
	}
	return result, rows.Err()
}

func scanAgentSessionCassette(scanner agentSessionReplayScanner) (agentsessionreplay.Cassette, error) {
	var cassette agentsessionreplay.Cassette
	err := scanner.Scan(
		&cassette.ID,
		&cassette.Name,
		&cassette.SourceRecordingID,
		&cassette.AgentTargetID,
		&cassette.RootAgentSessionID,
		&cassette.Mode,
		&cassette.TotalBytes,
		&cassette.ManifestSHA256,
		&cassette.CreatedAtUnixMS,
	)
	return cassette, err
}
