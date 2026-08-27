package api

import (
	"context"
	"net/http"
	"testing"

	tuttigenerated "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/api/generated"
	"github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/apierrors"
	workspacebiz "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/biz/workspace"
	workspaceservice "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/workspace"
)

func TestDaemonAPIGeneratedRoutesReplaceWorkspaceAppIconReturnServiceUnavailable(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRoutes(mux, NewRoutes(DaemonAPI{}))

	recorder := performGeneratedRouteRequest(
		t,
		mux,
		http.MethodPost,
		"/v1/workspaces/ws-1/apps/app-1/icon",
		map[string]any{"sourcePath": "/tmp/icon.png"},
	)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d; body: %s", recorder.Code, http.StatusServiceUnavailable, recorder.Body.String())
	}

	assertGeneratedRouteError(
		t,
		recorder,
		tuttigenerated.ServiceUnavailable,
		apierrors.ReasonWorkspaceAppUnavailable,
		"workspace app service is unavailable",
	)
}

func TestDaemonAPIGeneratedRoutesLaunchWorkspaceApp(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRoutes(mux, NewRoutes(DaemonAPI{
		AppCenterService: stubAppCenterService{
			launchFn: func(_ context.Context, workspaceID string, appID string) (workspacebiz.WorkspaceApp, error) {
				if workspaceID != "ws-1" {
					t.Fatalf("workspaceID = %q, want ws-1", workspaceID)
				}
				if appID != "app-1" {
					t.Fatalf("appID = %q, want app-1", appID)
				}
				return workspaceAppForRouteTest(appID, workspacebiz.AppRuntimeStatusRunning), nil
			},
		},
	}))

	recorder := performGeneratedRouteRequest(
		t,
		mux,
		http.MethodPost,
		"/v1/workspaces/ws-1/apps/app-1/launch",
		nil,
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}

	var response tuttigenerated.WorkspaceAppResponse
	decodeGeneratedRouteResponse(t, recorder, &response)
	if response.App.AppId != "app-1" || response.App.Status != tuttigenerated.WorkspaceAppRuntimeStatusRunning {
		t.Fatalf("response app = %#v", response.App)
	}
}

func TestDaemonAPIGeneratedRoutesRetryWorkspaceAppMapsInvalidRuntimeState(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRoutes(mux, NewRoutes(DaemonAPI{
		AppCenterService: stubAppCenterService{
			retryFn: func(context.Context, string, string) (workspacebiz.WorkspaceApp, error) {
				return workspacebiz.WorkspaceApp{}, workspaceservice.ErrInvalidWorkspaceAppRuntimeState
			},
		},
	}))

	recorder := performGeneratedRouteRequest(
		t,
		mux,
		http.MethodPost,
		"/v1/workspaces/ws-1/apps/app-1/retry",
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
		workspaceservice.ErrInvalidWorkspaceAppRuntimeState.Error(),
	)
}
