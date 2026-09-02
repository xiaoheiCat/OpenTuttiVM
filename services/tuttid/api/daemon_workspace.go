package api

import (
	"context"
	"strings"

	tuttigenerated "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/api/generated"
	workspaceapi "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/api/workspace"
	"github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/apierrors"
	workspaceservice "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/workspace"
)

func (api DaemonAPI) ListWorkspaces(ctx context.Context, _ tuttigenerated.ListWorkspacesRequestObject) (tuttigenerated.ListWorkspacesResponseObject, error) {
	if api.WorkspaceService == nil {
		return tuttigenerated.ListWorkspaces503JSONResponse{
			ServiceUnavailableErrorJSONResponse: serviceUnavailableError(
				apierrors.WorkspaceServiceUnavailable(apierrors.WithDeveloperMessage("workspace service is unavailable")),
			),
		}, nil
	}

	workspaces, err := api.WorkspaceService.List(ctx)
	if err != nil {
		return tuttigenerated.ListWorkspaces502JSONResponse{
			WorkspaceOperationErrorJSONResponse: workspaceOperationError(
				apierrors.WorkspaceOperationFailed(apierrors.WithCause(err)),
			),
		}, nil
	}

	return tuttigenerated.ListWorkspaces200JSONResponse{
		Workspaces: workspaceapi.GeneratedSummariesFromBiz(workspaces),
		TotalCount: len(workspaces),
	}, nil
}

func (api DaemonAPI) CreateWorkspace(ctx context.Context, request tuttigenerated.CreateWorkspaceRequestObject) (tuttigenerated.CreateWorkspaceResponseObject, error) {
	if api.WorkspaceService == nil {
		return tuttigenerated.CreateWorkspace503JSONResponse{
			ServiceUnavailableErrorJSONResponse: serviceUnavailableError(
				apierrors.WorkspaceServiceUnavailable(apierrors.WithDeveloperMessage("workspace service is unavailable")),
			),
		}, nil
	}

	if request.Body == nil {
		return tuttigenerated.CreateWorkspace400JSONResponse{
			InvalidRequestErrorJSONResponse: invalidRequestError(
				apierrors.EmptyBody(apierrors.WithDeveloperMessage("empty body")),
			),
		}, nil
	}

	name := strings.TrimSpace(request.Body.Name)
	if name == "" {
		return tuttigenerated.CreateWorkspace400JSONResponse{
			InvalidRequestErrorJSONResponse: invalidRequestError(
				apierrors.MissingWorkspaceName(
					apierrors.WithDeveloperMessage("workspace name is required"),
					apierrors.WithParams(map[string]any{"field": "name"}),
				),
			),
		}, nil
	}

	workspace, err := api.WorkspaceService.Create(ctx, workspaceservice.CreateInput{
		Name: name,
	})
	if err != nil {
		return writeCreateWorkspaceError(err), nil
	}

	return tuttigenerated.CreateWorkspace201JSONResponse(workspaceapi.GeneratedEnvelopeResponseFromBiz(workspace)), nil
}

func (api DaemonAPI) GetStartupWorkspace(ctx context.Context, _ tuttigenerated.GetStartupWorkspaceRequestObject) (tuttigenerated.GetStartupWorkspaceResponseObject, error) {
	if api.WorkspaceService == nil {
		return tuttigenerated.GetStartupWorkspace503JSONResponse{
			ServiceUnavailableErrorJSONResponse: serviceUnavailableError(
				apierrors.WorkspaceServiceUnavailable(apierrors.WithDeveloperMessage("workspace service is unavailable")),
			),
		}, nil
	}

	response, err := api.WorkspaceService.Startup(ctx)
	if err != nil {
		return tuttigenerated.GetStartupWorkspace502JSONResponse{
			WorkspaceOperationErrorJSONResponse: workspaceOperationError(
				apierrors.WorkspaceOperationFailed(apierrors.WithCause(err)),
			),
		}, nil
	}

	return tuttigenerated.GetStartupWorkspace200JSONResponse(workspaceapi.GeneratedStartupResponseFromBiz(response)), nil
}

func (api DaemonAPI) DeleteWorkspace(ctx context.Context, request tuttigenerated.DeleteWorkspaceRequestObject) (tuttigenerated.DeleteWorkspaceResponseObject, error) {
	if api.WorkspaceService == nil {
		return tuttigenerated.DeleteWorkspace503JSONResponse{
			ServiceUnavailableErrorJSONResponse: serviceUnavailableError(
				apierrors.WorkspaceServiceUnavailable(apierrors.WithDeveloperMessage("workspace service is unavailable")),
			),
		}, nil
	}

	workspaceID := strings.TrimSpace(string(request.WorkspaceID))
	if workspaceID == "" {
		return tuttigenerated.DeleteWorkspace400JSONResponse{
			InvalidRequestErrorJSONResponse: invalidRequestError(
				apierrors.MissingWorkspaceID(
					apierrors.WithDeveloperMessage("workspace id is required"),
					apierrors.WithParams(map[string]any{"field": "workspaceId"}),
				),
			),
		}, nil
	}

	response, err := api.WorkspaceService.Delete(ctx, workspaceID)
	if err != nil {
		return writeDeleteWorkspaceError(err), nil
	}

	return tuttigenerated.DeleteWorkspace200JSONResponse{
		WorkspaceId: response.WorkspaceID,
	}, nil
}

func (api DaemonAPI) GetWorkspace(ctx context.Context, request tuttigenerated.GetWorkspaceRequestObject) (tuttigenerated.GetWorkspaceResponseObject, error) {
	if api.WorkspaceService == nil {
		return tuttigenerated.GetWorkspace503JSONResponse{
			ServiceUnavailableErrorJSONResponse: serviceUnavailableError(
				apierrors.WorkspaceServiceUnavailable(apierrors.WithDeveloperMessage("workspace service is unavailable")),
			),
		}, nil
	}

	workspaceID := strings.TrimSpace(string(request.WorkspaceID))
	if workspaceID == "" {
		return tuttigenerated.GetWorkspace400JSONResponse{
			InvalidRequestErrorJSONResponse: invalidRequestError(
				apierrors.MissingWorkspaceID(
					apierrors.WithDeveloperMessage("workspace id is required"),
					apierrors.WithParams(map[string]any{"field": "workspaceId"}),
				),
			),
		}, nil
	}

	workspace, err := api.WorkspaceService.Get(ctx, workspaceID)
	if err != nil {
		return writeGetWorkspaceError(err), nil
	}

	return tuttigenerated.GetWorkspace200JSONResponse(workspaceapi.GeneratedEnvelopeResponseFromBiz(workspace)), nil
}

func (api DaemonAPI) UpdateWorkspace(ctx context.Context, request tuttigenerated.UpdateWorkspaceRequestObject) (tuttigenerated.UpdateWorkspaceResponseObject, error) {
	if api.WorkspaceService == nil {
		return tuttigenerated.UpdateWorkspace503JSONResponse{
			ServiceUnavailableErrorJSONResponse: serviceUnavailableError(
				apierrors.WorkspaceServiceUnavailable(apierrors.WithDeveloperMessage("workspace service is unavailable")),
			),
		}, nil
	}

	workspaceID := strings.TrimSpace(string(request.WorkspaceID))
	if workspaceID == "" {
		return tuttigenerated.UpdateWorkspace400JSONResponse{
			InvalidRequestErrorJSONResponse: invalidRequestError(
				apierrors.MissingWorkspaceID(
					apierrors.WithDeveloperMessage("workspace id is required"),
					apierrors.WithParams(map[string]any{"field": "workspaceId"}),
				),
			),
		}, nil
	}

	if request.Body == nil {
		return tuttigenerated.UpdateWorkspace400JSONResponse{
			InvalidRequestErrorJSONResponse: invalidRequestError(
				apierrors.EmptyBody(apierrors.WithDeveloperMessage("empty body")),
			),
		}, nil
	}

	name := strings.TrimSpace(request.Body.Name)
	if name == "" {
		return tuttigenerated.UpdateWorkspace400JSONResponse{
			InvalidRequestErrorJSONResponse: invalidRequestError(
				apierrors.MissingWorkspaceName(
					apierrors.WithDeveloperMessage("workspace name is required"),
					apierrors.WithParams(map[string]any{"field": "name"}),
				),
			),
		}, nil
	}

	workspace, err := api.WorkspaceService.Update(ctx, workspaceID, workspaceservice.UpdateInput{
		Name: name,
	})
	if err != nil {
		return writeUpdateWorkspaceError(err), nil
	}

	return tuttigenerated.UpdateWorkspace200JSONResponse(workspaceapi.GeneratedEnvelopeResponseFromBiz(workspace)), nil
}

func (api DaemonAPI) OpenWorkspace(ctx context.Context, request tuttigenerated.OpenWorkspaceRequestObject) (tuttigenerated.OpenWorkspaceResponseObject, error) {
	if api.WorkspaceService == nil {
		return tuttigenerated.OpenWorkspace503JSONResponse{
			ServiceUnavailableErrorJSONResponse: serviceUnavailableError(
				apierrors.WorkspaceServiceUnavailable(apierrors.WithDeveloperMessage("workspace service is unavailable")),
			),
		}, nil
	}

	workspaceID := strings.TrimSpace(string(request.WorkspaceID))
	if workspaceID == "" {
		return tuttigenerated.OpenWorkspace400JSONResponse{
			InvalidRequestErrorJSONResponse: invalidRequestError(
				apierrors.MissingWorkspaceID(
					apierrors.WithDeveloperMessage("workspace id is required"),
					apierrors.WithParams(map[string]any{"field": "workspaceId"}),
				),
			),
		}, nil
	}

	workspace, err := api.WorkspaceService.Open(ctx, workspaceID)
	if err != nil {
		return writeOpenWorkspaceError(err), nil
	}

	return tuttigenerated.OpenWorkspace200JSONResponse(workspaceapi.GeneratedEnvelopeResponseFromBiz(workspace)), nil
}
