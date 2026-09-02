package api

import (
	"context"
	"net/http"
	"testing"

	tuttigenerated "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/api/generated"
	workspacebiz "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/biz/workspace"
	workspaceservice "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/workspace"
)

type rejectingWorkbenchStore struct {
	t *testing.T
}

func (s rejectingWorkbenchStore) GetWorkbenchSnapshot(context.Context, string) (workspacebiz.WorkbenchSnapshot, error) {
	s.t.Fatal("GetWorkbenchSnapshot should not be called")
	return workspacebiz.WorkbenchSnapshot{}, nil
}

func (s rejectingWorkbenchStore) PutWorkbenchSnapshot(context.Context, workspacebiz.WorkbenchSnapshot) error {
	s.t.Fatal("PutWorkbenchSnapshot should not be called for invalid workbench snapshots")
	return nil
}

func TestDaemonAPIWorkbenchRejectsInvalidSnapshotBeforeStore(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRoutes(
		mux,
		NewRoutes(DaemonAPI{
			WorkbenchService: workspaceservice.WorkbenchService{
				Store: rejectingWorkbenchStore{t: t},
			},
		},
		),
	)

	recorder := performGeneratedRouteRequest(t, mux, http.MethodPut, "/v1/workspaces/ws-1/workbench", map[string]any{
		"snapshot": map[string]any{
			"schemaVersion": 1,
			"nodes": []map[string]any{
				{
					"id":    "node-1",
					"kind":  "terminal",
					"title": "Terminal",
					"frame": map[string]any{
						"x":      10,
						"y":      20,
						"width":  120,
						"height": 240,
					},
				},
			},
		},
	})
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body: %s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
	}
}

func TestDaemonAPIWorkbenchRejectsUnknownSnapshotFields(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRoutes(
		mux,
		NewRoutes(DaemonAPI{
			WorkbenchService: workspaceservice.WorkbenchService{
				Store: rejectingWorkbenchStore{t: t},
			},
		},
		),
	)

	recorder := performGeneratedRouteRequest(t, mux, http.MethodPut, "/v1/workspaces/ws-1/workbench", map[string]any{
		"snapshot": map[string]any{
			"schemaVersion": 1,
			"nodes": []map[string]any{
				{
					"id":       "node-1",
					"kind":     "terminal",
					"title":    "Terminal",
					"position": map[string]any{"x": 10, "y": 20},
					"frame": map[string]any{
						"x":      10,
						"y":      20,
						"width":  320,
						"height": 240,
					},
				},
			},
		},
	})
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body: %s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
	}
	assertGeneratedRouteError(
		t,
		recorder,
		tuttigenerated.InvalidRequest,
		"malformed_request",
		"can't decode JSON body: json: unknown field \"position\"",
	)
}
