package workspace

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	agenthost "github.com/xiaoheiCat/OpenTuttiVM/packages/agent/host"
	replaybiz "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/biz/agentsessionreplay"
)

const tuttiReplayStateSchemaVersion = replaybiz.SchemaVersion

type TuttiReplayState = replaybiz.TuttiReplayState
type TuttiReplayAgent = replaybiz.TuttiReplayAgent
type TuttiReplayTuttiMode = replaybiz.TuttiReplayTuttiMode
type TuttiReplayActivation = replaybiz.TuttiReplayActivation
type TuttiReplayTurnSnapshot = replaybiz.TuttiReplayTurnSnapshot
type TuttiReplayWorkflow = replaybiz.TuttiReplayWorkflow
type TuttiReplayIssue = replaybiz.TuttiReplayIssue
type TuttiReplayIssueTask = replaybiz.TuttiReplayIssueTask

func (s *SQLiteStore) CaptureReplayState(
	ctx context.Context,
	workspaceID string,
	rootAgentSessionID string,
) ([]byte, error) {
	agent, err := s.agentReadStore().CaptureHistoricalSessionGraph(
		ctx,
		workspaceID,
		rootAgentSessionID,
	)
	if err != nil {
		return nil, err
	}
	return s.CaptureReplayStateWithAgentGraph(
		ctx,
		workspaceID,
		agent,
	)
}

func (s *SQLiteStore) CaptureReplayStateWithAgentGraph(
	ctx context.Context,
	workspaceID string,
	agent agenthost.HistoricalSessionGraph,
) ([]byte, error) {
	state, err := s.CaptureTuttiReplayStateWithAgent(ctx, workspaceID, agent)
	if err != nil {
		return nil, err
	}
	if err := replaybiz.ValidateTuttiReplayState(state); err != nil {
		return nil, err
	}
	raw, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode semantic replay state: %w", err)
	}
	return append(raw, '\n'), nil
}

func (s *SQLiteStore) CaptureTuttiReplayStateWithAgent(
	ctx context.Context,
	workspaceID string,
	agent agenthost.HistoricalSessionGraph,
) (TuttiReplayState, error) {
	if s == nil || s.readDB == nil {
		return TuttiReplayState{}, errors.New("workspace database is not initialized")
	}
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return TuttiReplayState{}, errors.New("workspace and root Agent Session ids are required")
	}
	if err := agenthost.ValidateHistoricalSessionGraph(agent); err != nil {
		return TuttiReplayState{}, err
	}
	sessionIDs := make([]string, 0, len(agent.Sessions))
	for _, session := range agent.Sessions {
		sessionIDs = append(sessionIDs, session.ID)
	}
	state := TuttiReplayState{
		SchemaVersion: tuttiReplayStateSchemaVersion,
		Agent: replaybiz.ProjectPortableAgentState(
			agent,
			filepath.Dir(s.dbPath),
		),
		TuttiMode: TuttiReplayTuttiMode{
			Activations: []TuttiReplayActivation{}, TurnSnapshots: []TuttiReplayTurnSnapshot{},
		},
		Workflows: []TuttiReplayWorkflow{},
		Issues:    []TuttiReplayIssue{},
	}
	if err := s.captureReplayTuttiMode(ctx, workspaceID, sessionIDs, &state); err != nil {
		return TuttiReplayState{}, err
	}
	if err := s.captureReplayWorkflows(ctx, workspaceID, sessionIDs, &state); err != nil {
		return TuttiReplayState{}, err
	}
	return state, nil
}

func (s *SQLiteStore) CaptureHistoricalSessionGraph(
	ctx context.Context,
	workspaceID string,
	rootAgentSessionID string,
) (agenthost.HistoricalSessionGraph, error) {
	if s == nil || s.agentReadStore() == nil {
		return agenthost.HistoricalSessionGraph{}, errors.New("workspace database is not initialized")
	}
	return s.agentReadStore().CaptureHistoricalSessionGraph(
		ctx,
		workspaceID,
		rootAgentSessionID,
	)
}

func (s *SQLiteStore) captureReplayTuttiMode(
	ctx context.Context,
	workspaceID string,
	sessionIDs []string,
	state *TuttiReplayState,
) error {
	placeholders, args := replayIdentityArgs(workspaceID, sessionIDs)
	rows, err := s.readDB.QueryContext(ctx, `
SELECT a.activation_id, a.agent_session_id, a.current_revision_id,
       a.current_revision, r.state, r.source, r.orchestration_intensity, r.speed
FROM tutti_mode_activations a
JOIN tutti_mode_activation_revisions r
  ON r.workspace_id = a.workspace_id
 AND r.activation_id = a.activation_id
 AND r.revision_id = a.current_revision_id
WHERE a.workspace_id = ? AND a.agent_session_id IN (`+placeholders+`)
ORDER BY a.activation_id
`, args...)
	if err != nil {
		return err
	}
	for rows.Next() {
		var activation TuttiReplayActivation
		if err := rows.Scan(
			&activation.ID, &activation.SessionID, &activation.CurrentRevisionID,
			&activation.CurrentRevision, &activation.State, &activation.Source,
			&activation.Effect, &activation.Speed,
		); err != nil {
			_ = rows.Close()
			return err
		}
		state.TuttiMode.Activations = append(state.TuttiMode.Activations, activation)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	rows, err = s.readDB.QueryContext(ctx, `
SELECT agent_session_id, turn_id, activation_id, revision_id, revision,
       state, source, CASE WHEN activation_id = '' THEN 0 ELSE 1 END,
       orchestration_intensity, speed, dispatch_state
FROM tutti_mode_turn_snapshots
WHERE workspace_id = ? AND agent_session_id IN (`+placeholders+`)
ORDER BY agent_session_id, COALESCE((
  SELECT turn_sequence
  FROM workspace_agent_turn_sequences
  WHERE workspace_id = tutti_mode_turn_snapshots.workspace_id
    AND agent_session_id = tutti_mode_turn_snapshots.agent_session_id
    AND turn_id = tutti_mode_turn_snapshots.turn_id
), 9223372036854775807), turn_id
`, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var snapshot TuttiReplayTurnSnapshot
		if err := rows.Scan(
			&snapshot.SessionID, &snapshot.TurnID, &snapshot.ActivationID,
			&snapshot.RevisionID, &snapshot.Revision, &snapshot.State,
			&snapshot.Source, &snapshot.PreferenceVersion, &snapshot.Effect,
			&snapshot.Speed, &snapshot.DispatchState,
		); err != nil {
			return err
		}
		state.TuttiMode.TurnSnapshots = append(state.TuttiMode.TurnSnapshots, snapshot)
	}
	return rows.Err()
}

func (s *SQLiteStore) captureReplayWorkflows(
	ctx context.Context,
	workspaceID string,
	sessionIDs []string,
	state *TuttiReplayState,
) error {
	placeholders, args := replayIdentityArgs(workspaceID, sessionIDs)
	rows, err := s.readDB.QueryContext(ctx, `
SELECT workflow_id, workflow_type, trigger_kind, source_session_id,
       source_turn_id, source_tool_call_id, status, current_revision_id
FROM workspace_workflows
WHERE workspace_id = ? AND source_session_id IN (`+placeholders+`)
ORDER BY workflow_id
`, args...)
	if err != nil {
		return err
	}
	var workflowIDs []string
	for rows.Next() {
		var workflow TuttiReplayWorkflow
		if err := rows.Scan(
			&workflow.ID, &workflow.Type, &workflow.TriggerKind,
			&workflow.SourceSessionID, &workflow.SourceTurnID,
			&workflow.SourceToolCallID, &workflow.Status,
			&workflow.CurrentRevisionID,
		); err != nil {
			_ = rows.Close()
			return err
		}
		state.Workflows = append(state.Workflows, workflow)
		workflowIDs = append(workflowIDs, workflow.ID)
	}
	if err := rows.Close(); err != nil || len(workflowIDs) == 0 {
		return err
	}
	sort.Strings(workflowIDs)
	workflowPlaceholders, workflowArgs := replayIdentityArgs(workspaceID, workflowIDs)
	issueIDs, err := queryReplayStrings(ctx, s.readDB, `
SELECT DISTINCT issue_id
FROM workspace_workflow_operations
WHERE workspace_id = ? AND workflow_id IN (`+workflowPlaceholders+`)
  AND TRIM(issue_id) != ''
ORDER BY issue_id
`, workflowArgs...)
	if err != nil {
		return err
	}
	for _, issueID := range issueIDs {
		issue, err := s.captureReplayIssue(ctx, workspaceID, issueID)
		if err != nil {
			return err
		}
		state.Issues = append(state.Issues, issue)
	}
	for index := range state.Workflows {
		workflow := &state.Workflows[index]
		workflow.IssueIDs, err = queryReplayStrings(ctx, s.readDB, `
SELECT DISTINCT issue_id
FROM workspace_workflow_operations
WHERE workspace_id = ? AND workflow_id = ? AND TRIM(issue_id) != ''
ORDER BY issue_id
`, workspaceID, workflow.ID)
		if err != nil {
			return err
		}
		if workflow.IssueIDs == nil {
			workflow.IssueIDs = []string{}
		}
	}
	return nil
}

func (s *SQLiteStore) captureReplayIssue(
	ctx context.Context,
	workspaceID, issueID string,
) (TuttiReplayIssue, error) {
	var issue TuttiReplayIssue
	err := s.readDB.QueryRowContext(ctx, `
SELECT issue_id, title, content, status
FROM workspace_issues
WHERE workspace_id = ? AND issue_id = ?
`, workspaceID, issueID).Scan(&issue.ID, &issue.Title, &issue.Content, &issue.Status)
	if err != nil {
		return TuttiReplayIssue{}, err
	}
	issue.Tasks = []TuttiReplayIssueTask{}
	rows, err := s.readDB.QueryContext(ctx, `
SELECT task_id, title, content, status, priority, sort_index
FROM workspace_issue_tasks
WHERE workspace_id = ? AND issue_id = ?
ORDER BY sort_index, task_id
`, workspaceID, issueID)
	if err != nil {
		return TuttiReplayIssue{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var task TuttiReplayIssueTask
		if err := rows.Scan(
			&task.ID, &task.Title, &task.Content, &task.Status,
			&task.Priority, &task.Position,
		); err != nil {
			return TuttiReplayIssue{}, err
		}
		issue.Tasks = append(issue.Tasks, task)
	}
	return issue, rows.Err()
}

func replayIdentityArgs(workspaceID string, identities []string) (string, []any) {
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(identities)), ",")
	args := make([]any, 0, len(identities)+1)
	args = append(args, workspaceID)
	for _, identity := range identities {
		args = append(args, identity)
	}
	return placeholders, args
}

func queryReplayStrings(
	ctx context.Context,
	db *sql.DB,
	query string,
	args ...any,
) ([]string, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		result = append(result, strings.TrimSpace(value))
	}
	return result, rows.Err()
}
