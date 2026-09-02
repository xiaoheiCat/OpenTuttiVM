package api

import (
	"net/http"
	"testing"

	tuttigenerated "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/api/generated"
	"github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/apierrors"
)

func TestDaemonAPIGeneratedRoutesWorkspaceTerminalsReturnServiceUnavailable(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRoutes(mux, NewRoutes(DaemonAPI{}))

	recorder := performGeneratedRouteRequest(t, mux, http.MethodGet, "/v1/workspaces/ws-1/terminals", nil)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d; body: %s", recorder.Code, http.StatusServiceUnavailable, recorder.Body.String())
	}

	assertGeneratedRouteError(
		t,
		recorder,
		tuttigenerated.ServiceUnavailable,
		apierrors.ReasonWorkspaceTerminalUnavailable,
		"workspace terminal service is unavailable",
	)
}

func TestDaemonAPIGeneratedRoutesSearchWorkspaceIssueReferencesReturnServiceUnavailable(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRoutes(mux, NewRoutes(DaemonAPI{}))

	recorder := performGeneratedRouteRequest(t, mux, http.MethodPost, "/v1/workspaces/ws-1/issue-references/search", map[string]any{"query": "login"})
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d; body: %s", recorder.Code, http.StatusServiceUnavailable, recorder.Body.String())
	}

	assertGeneratedRouteError(
		t,
		recorder,
		tuttigenerated.ServiceUnavailable,
		apierrors.ReasonWorkspaceIssueServiceUnavailable,
		"workspace issue-manager service is unavailable",
	)
}

func TestDaemonAPIGeneratedRoutesAgentSessionsReturnServiceUnavailable(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRoutes(mux, NewRoutes(DaemonAPI{}))

	recorder := performGeneratedRouteRequest(t, mux, http.MethodGet, "/v1/workspaces/ws-1/agent-sessions", nil)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d; body: %s", recorder.Code, http.StatusServiceUnavailable, recorder.Body.String())
	}

	assertGeneratedRouteError(
		t,
		recorder,
		tuttigenerated.ServiceUnavailable,
		apierrors.ReasonWorkspaceAgentSessionUnavailable,
		"workspace agent session service is unavailable",
	)
}
