package api

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"testing"
	"time"

	tuttigenerated "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/api/generated"
	"github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/apierrors"
	agentservice "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/agent"
)

func TestDaemonAPIGeneratedRoutesListAgentSessionsForwardsQuery(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRoutes(mux, NewRoutes(DaemonAPI{
		AgentSessionService: stubAgentSessionService{
			listPageFn: func(_ context.Context, workspaceID string, input agentservice.ListSessionsInput) (agentservice.SessionListPage, error) {
				if workspaceID != "ws-1" {
					t.Fatalf("workspaceID = %q, want ws-1", workspaceID)
				}
				if input.AgentTargetID != "local:codex" || input.Cursor != "2000|session-2" || input.SearchQuery != "mention" || input.Limit != 30 {
					t.Fatalf("list input = %#v, want target cursor searchQuery limit", input)
				}
				return agentservice.SessionListPage{
					HasMore:    true,
					NextCursor: "1000|agent-session-1",
					Sessions: []agentservice.Session{{
						ID:        "agent-session-1",
						Provider:  "codex",
						Cwd:       "/workspace",
						Visible:   true,
						CreatedAt: time.UnixMilli(1000),
					}},
				}, nil
			},
		},
	}))

	recorder := performGeneratedRouteRequest(
		t,
		mux,
		http.MethodGet,
		"/v1/workspaces/ws-1/agent-sessions?agentTargetId=local%3Acodex&cursor=2000%7Csession-2&searchQuery=mention&limit=30",
		nil,
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var response tuttigenerated.WorkspaceAgentSessionListResponse
	decodeGeneratedRouteResponse(t, recorder, &response)
	if !response.HasMore || response.NextCursor == nil || *response.NextCursor != "1000|agent-session-1" {
		t.Fatalf("response page = %#v, want hasMore and nextCursor", response)
	}
}

func TestDaemonAPIGeneratedRoutesListAgentSessionSectionsForwardsLimit(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRoutes(mux, NewRoutes(DaemonAPI{
		AgentSessionService: stubAgentSessionService{
			listSessionSectionsFn: func(_ context.Context, workspaceID string, input agentservice.ListSessionSectionsInput) (agentservice.SessionSectionsPage, error) {
				updatedAt := time.UnixMilli(1000)
				if workspaceID != "ws-1" {
					t.Fatalf("workspaceID = %q, want ws-1", workspaceID)
				}
				if input.LimitPerSection != 7 || input.AgentTargetID != "claude-target" {
					t.Fatalf("section input = %#v, want limitPerSection and agentTargetID", input)
				}
				return agentservice.SessionSectionsPage{
					WorkspaceID: workspaceID,
					Pinned: agentservice.SessionPage{
						TotalCount: 1,
						Sessions: []agentservice.Session{{
							ID:        "pinned-session",
							Provider:  "codex",
							Visible:   true,
							CreatedAt: time.UnixMilli(1000),
							UpdatedAt: &updatedAt,
						}},
					},
					Sections: []agentservice.SessionSection{{
						Kind:       "conversations",
						SectionKey: "conversations",
						HasMore:    false,
						TotalCount: 8,
					}},
				}, nil
			},
		},
	}))

	recorder := performGeneratedRouteRequest(
		t,
		mux,
		http.MethodGet,
		"/v1/workspaces/ws-1/agent-session-sections?limitPerSection=7&agentTargetId=claude-target",
		nil,
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var response tuttigenerated.WorkspaceAgentSessionSectionsResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response error = %v", err)
	}
	if got, want := len(response.Pinned.Sessions), 1; got != want {
		t.Fatalf("pinned sessions len = %d, want %d", got, want)
	}
	if response.Pinned.TotalCount != 1 || response.Sections[0].TotalCount != 8 {
		t.Fatalf("section totals = pinned %d conversations %d, want 1 and 8", response.Pinned.TotalCount, response.Sections[0].TotalCount)
	}
}

func TestDaemonAPIGeneratedRoutesListAgentSessionSectionPageForwardsCursor(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRoutes(mux, NewRoutes(DaemonAPI{
		AgentSessionService: stubAgentSessionService{
			listSessionSectionPageFn: func(_ context.Context, workspaceID string, input agentservice.ListSessionSectionPageInput) (agentservice.SessionSection, error) {
				if workspaceID != "ws-1" {
					t.Fatalf("workspaceID = %q, want ws-1", workspaceID)
				}
				if input.SectionKey != "project:/workspace/project" || input.Cursor != "1000|session-1" || input.Limit != 5 || input.AgentTargetID != "claude-target" {
					t.Fatalf("page input = %#v, want sectionKey cursor limit agentTargetID", input)
				}
				return agentservice.SessionSection{
					Kind:       "project",
					SectionKey: input.SectionKey,
					HasMore:    false,
					TotalCount: 6,
				}, nil
			},
		},
	}))

	recorder := performGeneratedRouteRequest(
		t,
		mux,
		http.MethodGet,
		"/v1/workspaces/ws-1/agent-session-sections/page?sectionKey=project:%2Fworkspace%2Fproject&cursor=1000%7Csession-1&limit=5&agentTargetId=claude-target",
		nil,
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var response tuttigenerated.WorkspaceAgentSessionSectionPageResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response error = %v", err)
	}
	if response.Section.TotalCount != 6 {
		t.Fatalf("section total count = %d, want 6", response.Section.TotalCount)
	}
}

func TestDaemonAPIGeneratedRoutesListAgentSessionSectionDeletionCandidatesForwardsScope(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRoutes(mux, NewRoutes(DaemonAPI{
		AgentSessionService: stubAgentSessionService{
			listSectionDeletionCandidatesFn: func(_ context.Context, workspaceID string, input agentservice.ListSessionSectionDeletionCandidatesInput) (agentservice.SessionSectionDeletionCandidates, error) {
				if workspaceID != "ws-1" {
					t.Fatalf("workspaceID = %q, want ws-1", workspaceID)
				}
				if input.SectionKey != "project:/workspace/project" || input.AgentTargetID != "claude-target" || !input.ExcludePinned {
					t.Fatalf("candidate input = %#v, want sectionKey agentTargetID excludePinned", input)
				}
				return agentservice.SessionSectionDeletionCandidates{
					WorkspaceID:   workspaceID,
					SectionKey:    input.SectionKey,
					AgentTargetID: input.AgentTargetID,
					ExcludePinned: input.ExcludePinned,
					SessionIDs:    []string{"session-1", "session-2"},
				}, nil
			},
		},
	}))

	recorder := performGeneratedRouteRequest(
		t,
		mux,
		http.MethodGet,
		"/v1/workspaces/ws-1/agent-session-sections/deletion-candidates?sectionKey=project:%2Fworkspace%2Fproject&agentTargetId=claude-target&excludePinned=true",
		nil,
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var response tuttigenerated.WorkspaceAgentSessionSectionDeletionCandidatesResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response error = %v", err)
	}
	if !response.ExcludePinned || !slices.Equal(response.SessionIds, []string{"session-1", "session-2"}) || response.AgentTargetId == nil || *response.AgentTargetId != "claude-target" {
		t.Fatalf("response = %#v", response)
	}
}

func TestDaemonAPIGeneratedRoutesDeleteAgentSessionsBatchForwardsExactIDs(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRoutes(mux, NewRoutes(DaemonAPI{
		AgentSessionService: stubAgentSessionService{
			deleteSessionsBatchFn: func(_ context.Context, workspaceID string, input agentservice.DeleteSessionsBatchInput) (agentservice.DeleteSessionsBatchResult, error) {
				if workspaceID != "ws-1" {
					t.Fatalf("workspaceID = %q, want ws-1", workspaceID)
				}
				if !slices.Equal(input.SessionIDs, []string{"session-1", "session-2"}) {
					t.Fatalf("delete input = %#v, want exact session IDs", input)
				}
				return agentservice.DeleteSessionsBatchResult{
					RemovedMessages:         5,
					RemovedSessions:         2,
					RemovedSessionIDs:       []string{"session-1", "session-2"},
					CleanupFailedSessionIDs: []string{"session-2"},
				}, nil
			},
		},
	}))

	recorder := performGeneratedRouteRequest(
		t,
		mux,
		http.MethodDelete,
		"/v1/workspaces/ws-1/agent-sessions/batch",
		tuttigenerated.DeleteWorkspaceAgentSessionsBatchRequest{SessionIds: []string{"session-1", "session-2"}},
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var response tuttigenerated.DeleteWorkspaceAgentSessionsBatchResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response error = %v", err)
	}
	if response.RemovedMessages != 5 || response.RemovedSessions != 2 ||
		!slices.Equal(response.RemovedSessionIds, []string{"session-1", "session-2"}) ||
		!slices.Equal(response.CleanupFailedSessionIds, []string{"session-2"}) {
		t.Fatalf("response = %#v", response)
	}
	if response.RemovedSessions != 2 || !slices.Equal(response.RemovedSessionIds, []string{"session-1", "session-2"}) {
		t.Fatalf("response = %#v", response)
	}
}

func TestDaemonAPIGeneratedRoutesDeleteAgentSessionsBatchKeepsEmptyIDArrays(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRoutes(mux, NewRoutes(DaemonAPI{
		AgentSessionService: stubAgentSessionService{
			deleteSessionsBatchFn: func(context.Context, string, agentservice.DeleteSessionsBatchInput) (agentservice.DeleteSessionsBatchResult, error) {
				return agentservice.DeleteSessionsBatchResult{}, nil
			},
		},
	}))

	recorder := performGeneratedRouteRequest(
		t,
		mux,
		http.MethodDelete,
		"/v1/workspaces/ws-1/agent-sessions/batch",
		tuttigenerated.DeleteWorkspaceAgentSessionsBatchRequest{SessionIds: []string{"already-absent"}},
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var response tuttigenerated.DeleteWorkspaceAgentSessionsBatchResponse
	decodeGeneratedRouteResponse(t, recorder, &response)
	if response.RemovedSessionIds == nil || response.CleanupFailedSessionIds == nil {
		t.Fatalf("response = %#v, want empty arrays", response)
	}
	if len(response.RemovedSessionIds) != 0 || len(response.CleanupFailedSessionIds) != 0 {
		t.Fatalf("response = %#v, want empty arrays", response)
	}
}

func TestDaemonAPIGeneratedRoutesDeleteAgentSessionsBatchRejectsInvalidIDs(t *testing.T) {
	deleteCalls := 0
	mux := http.NewServeMux()
	RegisterRoutes(mux, NewRoutes(DaemonAPI{
		AgentSessionService: stubAgentSessionService{
			deleteSessionsBatchFn: func(_ context.Context, _ string, _ agentservice.DeleteSessionsBatchInput) (agentservice.DeleteSessionsBatchResult, error) {
				deleteCalls++
				return agentservice.DeleteSessionsBatchResult{}, nil
			},
		},
	}))
	tests := []struct {
		name string
		body any
	}{
		{name: "missing body", body: nil},
		{name: "empty ids", body: tuttigenerated.DeleteWorkspaceAgentSessionsBatchRequest{SessionIds: []string{}}},
		{name: "blank id", body: tuttigenerated.DeleteWorkspaceAgentSessionsBatchRequest{SessionIds: []string{" "}}},
		{name: "duplicate ids", body: tuttigenerated.DeleteWorkspaceAgentSessionsBatchRequest{SessionIds: []string{"session-1", " session-1 "}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := performGeneratedRouteRequest(t, mux, http.MethodDelete, "/v1/workspaces/ws-1/agent-sessions/batch", test.body)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d; body: %s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
			}
		})
	}
	if deleteCalls != 0 {
		t.Fatalf("delete calls = %d, want 0", deleteCalls)
	}
}

func TestDaemonAPIGeneratedRoutesListAgentPinnedSessionPageForwardsCursor(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRoutes(mux, NewRoutes(DaemonAPI{
		AgentSessionService: stubAgentSessionService{
			listPinnedSessionPageFn: func(_ context.Context, workspaceID string, input agentservice.ListPinnedSessionPageInput) (agentservice.SessionPage, error) {
				if workspaceID != "ws-1" {
					t.Fatalf("workspaceID = %q, want ws-1", workspaceID)
				}
				if input.Cursor != "1000|session-1" || input.Limit != 5 || input.AgentTargetID != "claude-target" {
					t.Fatalf("pinned page input = %#v, want cursor limit agentTargetID", input)
				}
				return agentservice.SessionPage{HasMore: false, TotalCount: 4}, nil
			},
		},
	}))

	recorder := performGeneratedRouteRequest(
		t,
		mux,
		http.MethodGet,
		"/v1/workspaces/ws-1/agent-session-sections/pinned-page?cursor=1000%7Csession-1&limit=5&agentTargetId=claude-target",
		nil,
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var response tuttigenerated.WorkspaceAgentSessionPageResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response error = %v", err)
	}
	if response.Page.TotalCount != 4 {
		t.Fatalf("pinned page total count = %d, want 4", response.Page.TotalCount)
	}
}

func TestDaemonAPIGeneratedRoutesListAgentSessionsRejectsLimitAboveContractMaximum(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRoutes(mux, NewRoutes(DaemonAPI{
		AgentSessionService: stubAgentSessionService{
			listGeneratedFilesFn: func(_ context.Context, workspaceID string, input agentservice.ListGeneratedFilesInput) (agentservice.GeneratedFileList, error) {
				if workspaceID != "ws-1" {
					t.Fatalf("workspaceID = %q, want ws-1", workspaceID)
				}
				if input.Query != "report" {
					t.Fatalf("query = %q, want report", input.Query)
				}
				if input.SectionKey != "project:/workspace" {
					t.Fatalf("sectionKey = %q, want project:/workspace", input.SectionKey)
				}
				if !slices.Equal(input.AgentTargetIDs, []string{"local:codex", "local:claude-code"}) {
					t.Fatalf("agentTargetIDs = %#v, want selected targets", input.AgentTargetIDs)
				}
				if input.Limit != 25 {
					t.Fatalf("limit = %d, want 25", input.Limit)
				}
				return agentservice.GeneratedFileList{
					WorkspaceID: workspaceID,
					Files: []agentservice.GeneratedFile{
						{
							Label: "report.md",
							Path:  "/workspace/report.md",
						},
					},
				}, nil
			},
		},
	}))

	recorder := performGeneratedRouteRequest(
		t,
		mux,
		http.MethodGet,
		"/v1/workspaces/ws-1/agent-sessions?limit=101",
		nil,
	)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body: %s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
	}

	assertGeneratedRouteError(
		t,
		recorder,
		tuttigenerated.InvalidRequest,
		apierrors.ReasonMalformedRequest,
		"invalid agent session request",
	)
}
