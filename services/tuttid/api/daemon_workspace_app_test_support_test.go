package api

import (
	"context"
	"time"

	workspacebiz "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/biz/workspace"
	workspaceservice "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/workspace"
)

type stubAppCenterService struct {
	launchFn func(context.Context, string, string) (workspacebiz.WorkspaceApp, error)
	retryFn  func(context.Context, string, string) (workspacebiz.WorkspaceApp, error)
}

func (stubAppCenterService) Add(context.Context, string, string) (workspacebiz.WorkspaceApp, error) {
	return workspacebiz.WorkspaceApp{}, nil
}

func (stubAppCenterService) DeletePackage(context.Context, string, string) error {
	return nil
}

func (stubAppCenterService) ExportPackage(context.Context, string, string, string) (workspaceservice.AppPackageArchiveResult, error) {
	return workspaceservice.AppPackageArchiveResult{}, nil
}

func (stubAppCenterService) ImportPackage(context.Context, string) (workspacebiz.WorkspaceApp, error) {
	return workspacebiz.WorkspaceApp{}, nil
}

func (stubAppCenterService) Install(context.Context, string, string) (workspacebiz.WorkspaceApp, error) {
	return workspacebiz.WorkspaceApp{}, nil
}

func (s stubAppCenterService) InstallWithOptions(ctx context.Context, workspaceID string, appID string, _ workspaceservice.InstallOptions) (workspacebiz.WorkspaceApp, error) {
	return s.Install(ctx, workspaceID, appID)
}

func (s stubAppCenterService) Launch(ctx context.Context, workspaceID string, appID string) (workspacebiz.WorkspaceApp, error) {
	if s.launchFn == nil {
		return workspacebiz.WorkspaceApp{}, nil
	}
	return s.launchFn(ctx, workspaceID, appID)
}

func (stubAppCenterService) LoadLocalPackage(context.Context, string, string, workspaceservice.InstallOptions) (workspacebiz.WorkspaceApp, error) {
	return workspacebiz.WorkspaceApp{}, nil
}

func (stubAppCenterService) ListReferences(context.Context, string, string, workspacebiz.AppReferenceListInput) (workspacebiz.AppReferenceListResult, error) {
	return workspacebiz.AppReferenceListResult{}, nil
}

func (stubAppCenterService) SearchReferences(context.Context, string, string, workspacebiz.AppReferenceSearchInput) (workspacebiz.AppReferenceListResult, error) {
	return workspacebiz.AppReferenceListResult{}, nil
}

func (stubAppCenterService) PrepareWorkspaceAppUpload(context.Context, string, string, workspaceservice.PrepareWorkspaceAppUploadInput) (workspaceservice.WorkspaceAppUploadSession, error) {
	return workspaceservice.WorkspaceAppUploadSession{}, nil
}

func (stubAppCenterService) PutWorkspaceAppUploadContent(context.Context, string, string, string, workspaceservice.PutWorkspaceAppUploadContentInput) error {
	return nil
}

func (stubAppCenterService) CompleteWorkspaceAppUpload(context.Context, string, string, string, time.Time) (workspaceservice.WorkspaceAppUploadedFile, error) {
	return workspaceservice.WorkspaceAppUploadedFile{}, nil
}

func (stubAppCenterService) CancelWorkspaceAppUpload(context.Context, string, string, string) error {
	return nil
}

func (stubAppCenterService) List(context.Context, string) ([]workspacebiz.WorkspaceApp, error) {
	return nil, nil
}

func (stubAppCenterService) CatalogLoadState() workspacebiz.AppCatalogLoadState {
	return workspacebiz.AppCatalogLoadState{}
}

func (stubAppCenterService) RefreshCatalog(context.Context, string) ([]workspacebiz.WorkspaceApp, error) {
	return nil, nil
}

func (stubAppCenterService) Remove(context.Context, string, string) (workspacebiz.WorkspaceApp, error) {
	return workspacebiz.WorkspaceApp{}, nil
}

func (stubAppCenterService) ReplaceIcon(context.Context, string, string, string) (workspacebiz.WorkspaceApp, error) {
	return workspacebiz.WorkspaceApp{}, nil
}

func (stubAppCenterService) ReloadLocalPackage(context.Context, string, string, workspaceservice.InstallOptions) (workspacebiz.WorkspaceApp, error) {
	return workspacebiz.WorkspaceApp{}, nil
}

func (s stubAppCenterService) Retry(ctx context.Context, workspaceID string, appID string) (workspacebiz.WorkspaceApp, error) {
	if s.retryFn == nil {
		return workspacebiz.WorkspaceApp{}, nil
	}
	return s.retryFn(ctx, workspaceID, appID)
}

func (stubAppCenterService) Rollback(context.Context, string, string, string) (workspacebiz.WorkspaceApp, error) {
	return workspacebiz.WorkspaceApp{}, nil
}

func (stubAppCenterService) StartEnabled(context.Context, string) ([]workspacebiz.WorkspaceApp, error) {
	return nil, nil
}

func (stubAppCenterService) StopAll(context.Context, string) ([]workspacebiz.WorkspaceApp, error) {
	return nil, nil
}

func (stubAppCenterService) Uninstall(context.Context, string, string) (workspacebiz.WorkspaceApp, error) {
	return workspacebiz.WorkspaceApp{}, nil
}

func workspaceAppForRouteTest(appID string, status workspacebiz.AppRuntimeStatus) workspacebiz.WorkspaceApp {
	launchURL := "http://127.0.0.1:3000"
	port := 3000
	return workspacebiz.WorkspaceApp{
		Package: workspacebiz.AppPackage{
			AppID:   appID,
			Version: "0.1.0",
			Manifest: workspacebiz.AppManifest{
				AppID:       appID,
				Name:        "Test App",
				Description: "Test app",
			},
			Source: workspacebiz.AppPackageSourceImported,
		},
		Installation: &workspacebiz.AppInstallation{
			WorkspaceID: "ws-1",
			AppID:       appID,
			Enabled:     true,
		},
		Runtime: workspacebiz.AppRuntimeState{
			Status:    status,
			LaunchURL: &launchURL,
			Port:      &port,
		},
	}
}
