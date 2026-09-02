package api

import (
	"context"
	"strings"

	tuttigenerated "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/api/generated"
	workspaceapi "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/api/workspace"
	"github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/apierrors"
)

func (api DaemonAPI) GetWorkspaceWorkbench(ctx context.Context, request tuttigenerated.GetWorkspaceWorkbenchRequestObject) (tuttigenerated.GetWorkspaceWorkbenchResponseObject, error) {
	if api.WorkbenchService == nil {
		return tuttigenerated.GetWorkspaceWorkbench503JSONResponse{
			ServiceUnavailableErrorJSONResponse: serviceUnavailableError(
				apierrors.WorkspaceWorkbenchUnavailable(apierrors.WithDeveloperMessage("workspace workbench service is unavailable")),
			),
		}, nil
	}

	workspaceID := strings.TrimSpace(string(request.WorkspaceID))
	if workspaceID == "" {
		return tuttigenerated.GetWorkspaceWorkbench400JSONResponse{
			InvalidRequestErrorJSONResponse: invalidRequestError(
				apierrors.MissingWorkspaceID(
					apierrors.WithDeveloperMessage("workspace id is required"),
					apierrors.WithParams(map[string]any{"field": "workspaceId"}),
				),
			),
		}, nil
	}

	snapshot, err := api.WorkbenchService.GetSnapshot(ctx, workspaceID)
	if err != nil {
		return writeGetWorkspaceWorkbenchError(err), nil
	}

	response, err := workspaceapi.GeneratedWorkbenchResponseFromBiz(snapshot)
	if err != nil {
		return tuttigenerated.GetWorkspaceWorkbench502JSONResponse{
			WorkspaceOperationErrorJSONResponse: workspaceOperationError(
				apierrors.WorkspaceOperationFailed(apierrors.WithCause(err)),
			),
		}, nil
	}

	return tuttigenerated.GetWorkspaceWorkbench200JSONResponse(response), nil
}

func (api DaemonAPI) PutWorkspaceWorkbench(ctx context.Context, request tuttigenerated.PutWorkspaceWorkbenchRequestObject) (tuttigenerated.PutWorkspaceWorkbenchResponseObject, error) {
	if api.WorkbenchService == nil {
		return tuttigenerated.PutWorkspaceWorkbench503JSONResponse{
			ServiceUnavailableErrorJSONResponse: serviceUnavailableError(
				apierrors.WorkspaceWorkbenchUnavailable(apierrors.WithDeveloperMessage("workspace workbench service is unavailable")),
			),
		}, nil
	}

	workspaceID := strings.TrimSpace(string(request.WorkspaceID))
	if workspaceID == "" {
		return tuttigenerated.PutWorkspaceWorkbench400JSONResponse{
			InvalidRequestErrorJSONResponse: invalidRequestError(
				apierrors.MissingWorkspaceID(
					apierrors.WithDeveloperMessage("workspace id is required"),
					apierrors.WithParams(map[string]any{"field": "workspaceId"}),
				),
			),
		}, nil
	}

	if request.Body == nil {
		return tuttigenerated.PutWorkspaceWorkbench400JSONResponse{
			InvalidRequestErrorJSONResponse: invalidRequestError(
				apierrors.EmptyBody(apierrors.WithDeveloperMessage("empty body")),
			),
		}, nil
	}

	snapshotInput := workspaceapi.WorkbenchSnapshotFromGenerated(request.Body.Snapshot)
	snapshot, err := api.WorkbenchService.PutSnapshot(ctx, workspaceID, snapshotInput)
	if err != nil {
		return writePutWorkspaceWorkbenchError(err), nil
	}

	response, err := workspaceapi.GeneratedWorkbenchResponseFromBiz(snapshot)
	if err != nil {
		return tuttigenerated.PutWorkspaceWorkbench502JSONResponse{
			WorkspaceOperationErrorJSONResponse: workspaceOperationError(
				apierrors.WorkspaceOperationFailed(apierrors.WithCause(err)),
			),
		}, nil
	}

	return tuttigenerated.PutWorkspaceWorkbench200JSONResponse(response), nil
}
