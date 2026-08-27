package workspace

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	replaybiz "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/biz/agentsessionreplay"
)

func (s *SQLiteStore) ReplayWorkspaceExists(
	ctx context.Context,
	workspaceID string,
) (bool, error) {
	if s == nil || s.readDB == nil {
		return false, errors.New("workspace database is not initialized")
	}
	var count int
	if err := s.readDB.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM workspaces WHERE id = ?`,
		strings.TrimSpace(workspaceID),
	).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

// RestoreTuttiReplayProductState restores the Tutti-owned portions after all
// input states have already been validated and merged. Agent history is
// restored separately through Host.
func (s *SQLiteStore) RestoreTuttiReplayProductState(
	ctx context.Context,
	workspaceID string,
	state replaybiz.TuttiReplayMergedState,
) error {
	if s == nil || s.writeDB == nil {
		return errors.New("workspace database is not initialized")
	}
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return errors.New("replay workspace id is required")
	}
	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin Tutti Replay product-state restore: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	now := time.Now().UTC().UnixMilli()
	for _, activation := range state.TuttiMode.Activations {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO tutti_mode_activations (
  workspace_id, activation_id, agent_session_id, current_revision_id,
  current_revision, created_at_unix_ms, updated_at_unix_ms
) VALUES (?, ?, ?, ?, ?, ?, ?)
`, workspaceID, activation.ID, activation.SessionID,
			activation.CurrentRevisionID, activation.CurrentRevision, now, now); err != nil {
			return replayRestoreConflict("Tutti Mode activation", activation.ID, err)
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO tutti_mode_activation_revisions (
  workspace_id, activation_id, revision_id, revision, state, source,
  orchestration_intensity, speed, created_at_unix_ms
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
`, workspaceID, activation.ID, activation.CurrentRevisionID,
			activation.CurrentRevision, activation.State, activation.Source,
			activation.Effect, activation.Speed, now); err != nil {
			return replayRestoreConflict("Tutti Mode activation revision", activation.CurrentRevisionID, err)
		}
	}
	for _, snapshot := range state.TuttiMode.TurnSnapshots {
		acceptedAt := any(nil)
		if snapshot.DispatchState == "accepted" {
			acceptedAt = now
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO tutti_mode_turn_snapshots (
  workspace_id, agent_session_id, turn_id, activation_id, revision_id,
  revision, state, source, orchestration_intensity, speed,
  created_at_unix_ms, dispatch_state, accepted_at_unix_ms
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`, workspaceID, snapshot.SessionID, snapshot.TurnID, snapshot.ActivationID,
			snapshot.RevisionID, snapshot.Revision, snapshot.State, snapshot.Source,
			snapshot.Effect, snapshot.Speed, now, snapshot.DispatchState, acceptedAt); err != nil {
			return replayRestoreConflict("Tutti Mode Turn snapshot", snapshot.TurnID, err)
		}
	}
	for _, workflow := range state.Workflows {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO workspace_workflows (
  workspace_id, workflow_id, workflow_type, owner, trigger_kind,
  source_session_id, source_turn_id, source_tool_call_id, status,
  current_revision_id, created_at_unix_ms, updated_at_unix_ms
) VALUES (?, ?, ?, 'tutti', ?, ?, ?, ?, ?, ?, ?, ?)
`, workspaceID, workflow.ID, workflow.Type, workflow.TriggerKind,
			workflow.SourceSessionID, workflow.SourceTurnID,
			workflow.SourceToolCallID, workflow.Status,
			workflow.CurrentRevisionID, now, now); err != nil {
			return replayRestoreConflict("Workflow", workflow.ID, err)
		}
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO tutti_mode_plans (workspace_id, workflow_id) VALUES (?, ?)`,
			workspaceID,
			workflow.ID,
		); err != nil {
			return replayRestoreConflict("Tutti Mode plan", workflow.ID, err)
		}
		for index, issueID := range workflow.IssueIDs {
			operationID := fmt.Sprintf("replay:%s:issue:%06d", workflow.ID, index)
			if _, err := tx.ExecContext(ctx, `
INSERT INTO workspace_workflow_operations (
  workspace_id, workflow_id, operation_id, kind, status, revision_id,
  issue_id, created_at_unix_ms, updated_at_unix_ms, completed_at_unix_ms
) VALUES (?, ?, ?, 'create_issue', 'succeeded', NULL, ?, ?, ?, ?)
`, workspaceID, workflow.ID, operationID, issueID, now, now, now); err != nil {
				return replayRestoreConflict("Workflow Issue relation", issueID, err)
			}
		}
	}
	for _, issue := range state.Issues {
		counts := replayIssueStatusCounts(issue)
		if _, err := tx.ExecContext(ctx, `
INSERT INTO workspace_issues (
  issue_id, topic_id, workspace_id, title, content, search_text, status,
  task_count, not_started_count, running_count, pending_acceptance_count,
  completed_count, failed_count, canceled_count,
  created_at_unix_ms, updated_at_unix_ms
) VALUES (?, 'default', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`, issue.ID, workspaceID, issue.Title, issue.Content,
			strings.TrimSpace(issue.Title+" "+issue.Content), issue.Status,
			len(issue.Tasks), counts["not_started"], counts["running"],
			counts["pending_acceptance"], counts["completed"], counts["failed"],
			counts["canceled"], now, now); err != nil {
			return replayRestoreConflict("Issue", issue.ID, err)
		}
		for _, task := range issue.Tasks {
			if _, err := tx.ExecContext(ctx, `
INSERT INTO workspace_issue_tasks (
  task_id, issue_id, workspace_id, title, content, search_text, status,
  priority, sort_index, created_at_unix_ms, updated_at_unix_ms
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`, task.ID, issue.ID, workspaceID, task.Title, task.Content,
				strings.TrimSpace(task.Title+" "+task.Content), task.Status,
				task.Priority, task.Position, now, now); err != nil {
				return replayRestoreConflict("Issue Task", task.ID, err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit Tutti Replay product-state restore: %w", err)
	}
	return nil
}

func replayIssueStatusCounts(issue TuttiReplayIssue) map[string]int {
	counts := map[string]int{}
	for _, task := range issue.Tasks {
		counts[task.Status]++
	}
	return counts
}

func replayRestoreConflict(kind, identity string, err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return err
	}
	return fmt.Errorf("%s %q restore conflict: %w", kind, identity, err)
}
