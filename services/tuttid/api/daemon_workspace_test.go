package api

import (
	"context"
	"net/http"
	"testing"
	"time"

	tuttigenerated "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/api/generated"
	workspacebiz "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/biz/workspace"
	workspacedata "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/data/workspace"
	workspaceservice "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/workspace"
)

type stubCatalogService struct {
	createFn func(context.Context, workspaceservice.CreateInput) (workspacebiz.Summary, error)
	deleteFn func(context.Context, string) (workspaceservice.DeleteResult, error)
	getFn    func(context.Context, string) (workspacebiz.Summary, error)
	listFn   func(context.Context) ([]workspacebiz.Summary, error)
	openFn   func(context.Context, string) (workspacebiz.Summary, error)
	startFn  func(context.Context) (*workspacebiz.Summary, error)
	updateFn func(context.Context, string, workspaceservice.UpdateInput) (workspacebiz.Summary, error)
}

func (s stubCatalogService) Create(ctx context.Context, input workspaceservice.CreateInput) (workspacebiz.Summary, error) {
	if s.createFn == nil {
		return workspacebiz.Summary{}, nil
	}
	return s.createFn(ctx, input)
}

func (s stubCatalogService) Delete(ctx context.Context, workspaceID string) (workspaceservice.DeleteResult, error) {
	if s.deleteFn == nil {
		return workspaceservice.DeleteResult{}, nil
	}
	return s.deleteFn(ctx, workspaceID)
}

func (s stubCatalogService) Get(ctx context.Context, workspaceID string) (workspacebiz.Summary, error) {
	if s.getFn == nil {
		return workspacebiz.Summary{}, nil
	}
	return s.getFn(ctx, workspaceID)
}

func (s stubCatalogService) List(ctx context.Context) ([]workspacebiz.Summary, error) {
	if s.listFn == nil {
		return nil, nil
	}
	return s.listFn(ctx)
}

func (s stubCatalogService) Open(ctx context.Context, workspaceID string) (workspacebiz.Summary, error) {
	if s.openFn == nil {
		return workspacebiz.Summary{}, nil
	}
	return s.openFn(ctx, workspaceID)
}

func (s stubCatalogService) Startup(ctx context.Context) (*workspacebiz.Summary, error) {
	if s.startFn == nil {
		return nil, nil
	}
	return s.startFn(ctx)
}

func (s stubCatalogService) Update(
	ctx context.Context,
	workspaceID string,
	input workspaceservice.UpdateInput,
) (workspacebiz.Summary, error) {
	if s.updateFn == nil {
		return workspacebiz.Summary{}, nil
	}
	return s.updateFn(ctx, workspaceID, input)
}

func TestDaemonAPIGeneratedRoutesListWorkspaces(t *testing.T) {
	lastOpenedAt := time.Date(2026, 5, 21, 8, 0, 0, 0, time.UTC)
	handler := generatedRouteHandler(stubCatalogService{
		listFn: func(context.Context) ([]workspacebiz.Summary, error) {
			return []workspacebiz.Summary{
				{
					ID:           "ws-1",
					Name:         "Workspace One",
					LastOpenedAt: &lastOpenedAt,
				},
			}, nil
		},
	})

	recorder := performGeneratedRouteRequest(t, handler, http.MethodGet, "/v1/workspaces", nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}

	var response tuttigenerated.ListWorkspacesResponse
	decodeGeneratedRouteResponse(t, recorder, &response)
	if response.TotalCount != 1 {
		t.Fatalf("totalCount = %d, want 1", response.TotalCount)
	}
	if len(response.Workspaces) != 1 {
		t.Fatalf("workspaces len = %d, want 1", len(response.Workspaces))
	}
	workspace := response.Workspaces[0]
	if workspace.Id != "ws-1" || workspace.Name != "Workspace One" {
		t.Fatalf("workspace = %#v", workspace)
	}
	if workspace.LastOpenedAt == nil || !workspace.LastOpenedAt.Equal(lastOpenedAt) {
		t.Fatalf("lastOpenedAt = %#v, want %s", workspace.LastOpenedAt, lastOpenedAt.Format(time.RFC3339))
	}
}

func TestDaemonAPIGeneratedRoutesCreateValidatesBody(t *testing.T) {
	handler := generatedRouteHandler(stubCatalogService{})

	recorder := performGeneratedRouteRequest(t, handler, http.MethodPost, "/v1/workspaces", map[string]string{
		"name": " ",
	})
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}

	assertGeneratedRouteError(
		t,
		recorder,
		tuttigenerated.InvalidRequest,
		"missing_workspace_name",
		"workspace name is required",
	)
}

func TestDaemonAPIGeneratedRoutesMapWorkspaceNotFound(t *testing.T) {
	handler := generatedRouteHandler(stubCatalogService{
		getFn: func(context.Context, string) (workspacebiz.Summary, error) {
			return workspacebiz.Summary{}, workspacedata.ErrWorkspaceNotFound
		},
	})

	recorder := performGeneratedRouteRequest(t, handler, http.MethodGet, "/v1/workspaces/ws-missing", nil)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNotFound)
	}

	assertGeneratedRouteError(
		t,
		recorder,
		tuttigenerated.WorkspaceNotFound,
		"workspace_not_found",
		workspacedata.ErrWorkspaceNotFound.Error(),
	)
}

func generatedRouteHandler(service stubCatalogService) http.Handler {
	mux := http.NewServeMux()
	RegisterRoutes(mux, NewRoutes(DaemonAPI{WorkspaceService: service}))
	return mux
}
