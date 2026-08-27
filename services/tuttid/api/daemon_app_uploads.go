package api

import (
	"context"
	"net/http"
	"time"

	tuttigenerated "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/api/generated"
	"github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/apierrors"
	workspaceservice "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/workspace"
	tuttitypes "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/types"
)

func registerWorkspaceAppUploadRoutes(mux *http.ServeMux, wrapper *tuttigenerated.ServerInterfaceWrapper) {
	mux.HandleFunc("/v1/workspaces/{workspaceID}/apps/{appID}/uploads", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			tuttitypes.WriteMethodNotAllowed(w)
			return
		}
		wrapper.PrepareWorkspaceAppUpload(w, r)
	})

	mux.HandleFunc("/v1/workspaces/{workspaceID}/apps/{appID}/uploads/{uploadID}/content", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			tuttitypes.WriteMethodNotAllowed(w)
			return
		}
		wrapper.PutWorkspaceAppUploadContent(w, r)
	})

	mux.HandleFunc("/v1/workspaces/{workspaceID}/apps/{appID}/uploads/{uploadID}", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			tuttitypes.WriteMethodNotAllowed(w)
			return
		}
		wrapper.CancelWorkspaceAppUpload(w, r)
	})

	mux.HandleFunc("/v1/workspaces/{workspaceID}/apps/{appID}/uploads/{uploadID}/complete", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			tuttitypes.WriteMethodNotAllowed(w)
			return
		}
		wrapper.CompleteWorkspaceAppUpload(w, r)
	})
}

func (api DaemonAPI) PrepareWorkspaceAppUpload(
	ctx context.Context,
	request tuttigenerated.PrepareWorkspaceAppUploadRequestObject,
) (tuttigenerated.PrepareWorkspaceAppUploadResponseObject, error) {
	if api.AppCenterService == nil {
		return tuttigenerated.PrepareWorkspaceAppUpload503JSONResponse{
			ServiceUnavailableErrorJSONResponse: workspaceAppServiceUnavailableError(),
		}, nil
	}
	workspaceID, appID, errResponse := validateWorkspaceAppPath(request.WorkspaceID, request.AppID)
	if errResponse != nil {
		return tuttigenerated.PrepareWorkspaceAppUpload400JSONResponse{InvalidRequestErrorJSONResponse: *errResponse}, nil
	}
	if request.Body == nil {
		return tuttigenerated.PrepareWorkspaceAppUpload400JSONResponse{
			InvalidRequestErrorJSONResponse: invalidRequestError(
				apierrors.EmptyBody(apierrors.WithDeveloperMessage("empty body")),
			),
		}, nil
	}

	session, err := api.AppCenterService.PrepareWorkspaceAppUpload(ctx, workspaceID, appID, workspaceservice.PrepareWorkspaceAppUploadInput{
		MimeType:  request.Body.MimeType,
		Name:      request.Body.Name,
		Purpose:   string(request.Body.Purpose),
		SizeBytes: request.Body.SizeBytes,
	})
	if err != nil {
		return writePrepareWorkspaceAppUploadError(err), nil
	}

	return tuttigenerated.PrepareWorkspaceAppUpload201JSONResponse{
		ExpiresAt: session.ExpiresAt,
		UploadId:  session.UploadID,
	}, nil
}

func (api DaemonAPI) PutWorkspaceAppUploadContent(
	ctx context.Context,
	request tuttigenerated.PutWorkspaceAppUploadContentRequestObject,
) (tuttigenerated.PutWorkspaceAppUploadContentResponseObject, error) {
	if api.AppCenterService == nil {
		return tuttigenerated.PutWorkspaceAppUploadContent503JSONResponse{
			ServiceUnavailableErrorJSONResponse: workspaceAppServiceUnavailableError(),
		}, nil
	}
	workspaceID, appID, errResponse := validateWorkspaceAppPath(request.WorkspaceID, request.AppID)
	if errResponse != nil {
		return tuttigenerated.PutWorkspaceAppUploadContent400JSONResponse{InvalidRequestErrorJSONResponse: *errResponse}, nil
	}

	if err := api.AppCenterService.PutWorkspaceAppUploadContent(ctx, workspaceID, appID, request.UploadID, workspaceservice.PutWorkspaceAppUploadContentInput{
		Body: request.Body,
	}); err != nil {
		return writePutWorkspaceAppUploadContentError(err), nil
	}

	return tuttigenerated.PutWorkspaceAppUploadContent204Response{}, nil
}

func (api DaemonAPI) CompleteWorkspaceAppUpload(
	ctx context.Context,
	request tuttigenerated.CompleteWorkspaceAppUploadRequestObject,
) (tuttigenerated.CompleteWorkspaceAppUploadResponseObject, error) {
	if api.AppCenterService == nil {
		return tuttigenerated.CompleteWorkspaceAppUpload503JSONResponse{
			ServiceUnavailableErrorJSONResponse: workspaceAppServiceUnavailableError(),
		}, nil
	}
	workspaceID, appID, errResponse := validateWorkspaceAppPath(request.WorkspaceID, request.AppID)
	if errResponse != nil {
		return tuttigenerated.CompleteWorkspaceAppUpload400JSONResponse{InvalidRequestErrorJSONResponse: *errResponse}, nil
	}

	file, err := api.AppCenterService.CompleteWorkspaceAppUpload(ctx, workspaceID, appID, request.UploadID, time.Now().UTC())
	if err != nil {
		return writeCompleteWorkspaceAppUploadError(err), nil
	}

	return tuttigenerated.CompleteWorkspaceAppUpload200JSONResponse{
		File: generatedWorkspaceAppUploadedFile(file),
	}, nil
}

func (api DaemonAPI) CancelWorkspaceAppUpload(
	ctx context.Context,
	request tuttigenerated.CancelWorkspaceAppUploadRequestObject,
) (tuttigenerated.CancelWorkspaceAppUploadResponseObject, error) {
	if api.AppCenterService == nil {
		return tuttigenerated.CancelWorkspaceAppUpload503JSONResponse{
			ServiceUnavailableErrorJSONResponse: workspaceAppServiceUnavailableError(),
		}, nil
	}
	workspaceID, appID, errResponse := validateWorkspaceAppPath(request.WorkspaceID, request.AppID)
	if errResponse != nil {
		return tuttigenerated.CancelWorkspaceAppUpload400JSONResponse{InvalidRequestErrorJSONResponse: *errResponse}, nil
	}

	if err := api.AppCenterService.CancelWorkspaceAppUpload(ctx, workspaceID, appID, request.UploadID); err != nil {
		return writeCancelWorkspaceAppUploadError(err), nil
	}

	return tuttigenerated.CancelWorkspaceAppUpload204Response{}, nil
}

func generatedWorkspaceAppUploadedFile(file workspaceservice.WorkspaceAppUploadedFile) tuttigenerated.WorkspaceAppUploadedFile {
	return tuttigenerated.WorkspaceAppUploadedFile{
		MimeType:  file.MimeType,
		Name:      file.Name,
		Path:      file.Path,
		Sha256:    file.SHA256,
		SizeBytes: file.SizeBytes,
	}
}

func writeCancelWorkspaceAppUploadError(err error) tuttigenerated.CancelWorkspaceAppUploadResponseObject {
	protocolErr := apierrors.Classify(err)
	switch protocolErr.Code {
	case tuttigenerated.WorkspaceNotFound, tuttigenerated.WorkspaceAppNotFound:
		return tuttigenerated.CancelWorkspaceAppUpload404JSONResponse{
			WorkspaceAppNotFoundErrorJSONResponse: workspaceAppNotFoundError(protocolErr),
		}
	case tuttigenerated.InvalidRequest:
		return tuttigenerated.CancelWorkspaceAppUpload400JSONResponse{
			InvalidRequestErrorJSONResponse: invalidRequestError(protocolErr),
		}
	default:
		return tuttigenerated.CancelWorkspaceAppUpload502JSONResponse{
			WorkspaceOperationErrorJSONResponse: workspaceOperationError(protocolErr),
		}
	}
}

func writePrepareWorkspaceAppUploadError(err error) tuttigenerated.PrepareWorkspaceAppUploadResponseObject {
	protocolErr := apierrors.Classify(err)
	switch protocolErr.Code {
	case tuttigenerated.WorkspaceNotFound, tuttigenerated.WorkspaceAppNotFound:
		return tuttigenerated.PrepareWorkspaceAppUpload404JSONResponse{
			WorkspaceAppNotFoundErrorJSONResponse: workspaceAppNotFoundError(protocolErr),
		}
	case tuttigenerated.InvalidRequest:
		return tuttigenerated.PrepareWorkspaceAppUpload400JSONResponse{
			InvalidRequestErrorJSONResponse: invalidRequestError(protocolErr),
		}
	default:
		return tuttigenerated.PrepareWorkspaceAppUpload502JSONResponse{
			WorkspaceOperationErrorJSONResponse: workspaceOperationError(protocolErr),
		}
	}
}

func writePutWorkspaceAppUploadContentError(err error) tuttigenerated.PutWorkspaceAppUploadContentResponseObject {
	protocolErr := apierrors.Classify(err)
	switch protocolErr.Code {
	case tuttigenerated.WorkspaceNotFound, tuttigenerated.WorkspaceAppNotFound:
		return tuttigenerated.PutWorkspaceAppUploadContent404JSONResponse{
			WorkspaceAppNotFoundErrorJSONResponse: workspaceAppNotFoundError(protocolErr),
		}
	case tuttigenerated.InvalidRequest:
		return tuttigenerated.PutWorkspaceAppUploadContent400JSONResponse{
			InvalidRequestErrorJSONResponse: invalidRequestError(protocolErr),
		}
	default:
		return tuttigenerated.PutWorkspaceAppUploadContent502JSONResponse{
			WorkspaceOperationErrorJSONResponse: workspaceOperationError(protocolErr),
		}
	}
}

func writeCompleteWorkspaceAppUploadError(err error) tuttigenerated.CompleteWorkspaceAppUploadResponseObject {
	protocolErr := apierrors.Classify(err)
	switch protocolErr.Code {
	case tuttigenerated.WorkspaceNotFound, tuttigenerated.WorkspaceAppNotFound:
		return tuttigenerated.CompleteWorkspaceAppUpload404JSONResponse{
			WorkspaceAppNotFoundErrorJSONResponse: workspaceAppNotFoundError(protocolErr),
		}
	case tuttigenerated.InvalidRequest:
		return tuttigenerated.CompleteWorkspaceAppUpload400JSONResponse{
			InvalidRequestErrorJSONResponse: invalidRequestError(protocolErr),
		}
	default:
		return tuttigenerated.CompleteWorkspaceAppUpload502JSONResponse{
			WorkspaceOperationErrorJSONResponse: workspaceOperationError(protocolErr),
		}
	}
}
