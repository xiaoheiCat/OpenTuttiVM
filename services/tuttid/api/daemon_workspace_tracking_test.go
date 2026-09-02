package api

import (
	"context"
	"testing"

	tuttigenerated "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/api/generated"
	workspacebiz "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/biz/workspace"
	workspaceservice "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/workspace"
)

func TestUpdateWorkspaceDoesNotTrackWorkspaceRenamedAfterSuccessfulUpdate(t *testing.T) {
	reporter := &recordingAnalyticsReporter{}
	api := DaemonAPI{
		AnalyticsReporter: reporter,
		WorkspaceService: stubCatalogService{
			updateFn: func(_ context.Context, workspaceID string, input workspaceservice.UpdateInput) (workspacebiz.Summary, error) {
				return workspacebiz.Summary{
					ID:   workspaceID,
					Name: input.Name,
				}, nil
			},
		},
	}

	response, err := api.UpdateWorkspace(context.Background(), tuttigenerated.UpdateWorkspaceRequestObject{
		WorkspaceID: tuttigenerated.WorkspaceID("ws-1"),
		Body: &tuttigenerated.UpdateWorkspaceRequest{
			Name: "Renamed Workspace",
		},
	})
	if err != nil {
		t.Fatalf("UpdateWorkspace() error = %v", err)
	}
	if _, ok := response.(tuttigenerated.UpdateWorkspace200JSONResponse); !ok {
		t.Fatalf("response = %T, want %T", response, tuttigenerated.UpdateWorkspace200JSONResponse{})
	}
	if len(reporter.events) != 0 {
		t.Fatalf("analytics events = %d, want 0", len(reporter.events))
	}
}
