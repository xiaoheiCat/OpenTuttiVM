package workspace

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	workspacebiz "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/biz/workspace"
	appcliservice "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/cli/appcli"
)

func (s *AppCenterService) activateAppCLI(ctx context.Context, workspaceID string, appPackage workspacebiz.AppPackage, baseURL string) workspacebiz.AppCLIState {
	if s.AppCLIRegistry == nil {
		if appPackage.Manifest.CLI != nil {
			return workspacebiz.AppCLIState{Status: workspacebiz.AppCLIStatusPending}
		}
		return workspacebiz.AppCLIState{Status: workspacebiz.AppCLIStatusNone}
	}
	return s.AppCLIRegistry.Activate(ctx, appcliservice.Activation{
		WorkspaceID: workspaceID,
		AppPackage:  appPackage,
		BaseURL:     baseURL,
	})
}

func (s *AppCenterService) appCLIState(workspaceID string, app workspacebiz.WorkspaceApp) workspacebiz.AppCLIState {
	if s.AppCLIRegistry == nil {
		if app.Installation != nil && app.Package.Manifest.CLI != nil {
			return workspacebiz.AppCLIState{Status: workspacebiz.AppCLIStatusPending}
		}
		return workspacebiz.AppCLIState{Status: workspacebiz.AppCLIStatusNone}
	}
	return s.AppCLIRegistry.Status(workspaceID, app)
}

func (s *AppCenterService) deactivateAppCLI(workspaceID string, appID string) {
	if s.AppCLIRegistry != nil {
		s.AppCLIRegistry.Deactivate(workspaceID, appID)
	}
}

func (s *AppCenterService) deactivateAppCLIForApp(appID string) {
	if s.AppCLIRegistry != nil {
		s.AppCLIRegistry.DeactivateApp(appID)
	}
}

func (s *AppCenterService) deactivateWorkspaceAppCLI(ctx context.Context, workspaceID string) {
	if s.AppCLIRegistry == nil || s.Store == nil {
		return
	}
	installations, err := s.Store.ListWorkspaceAppInstallations(ctx, workspaceID)
	if err != nil {
		slog.Warn("workspace app cli deactivate skipped; installation lookup failed", "workspaceId", workspaceID, "error", err)
		return
	}
	for _, installation := range installations {
		s.AppCLIRegistry.Deactivate(workspaceID, installation.AppID)
	}
}

func (s *AppCenterService) EnsureAppRunningForCLI(ctx context.Context, workspaceID string, appID string) (string, error) {
	appPackage, installation, err := s.installedPackage(ctx, workspaceID, appID)
	if err != nil {
		return "", err
	}
	if !installation.Enabled {
		return "", errors.New("workspace app is disabled")
	}
	state := s.runner().State(workspaceID, appPackage.AppID)
	if state.Status != workspacebiz.AppRuntimeStatusRunning || state.LaunchURL == nil {
		if _, err := s.Launch(ctx, workspaceID, appID); err != nil {
			return "", err
		}
		state, err = s.waitForAppRunning(ctx, workspaceID, appPackage.AppID)
		if err != nil {
			return "", err
		}
	}
	if state.LaunchURL == nil || strings.TrimSpace(*state.LaunchURL) == "" {
		return "", errors.New("workspace app runtime launch url is unavailable")
	}
	return *state.LaunchURL, nil
}

func (s *AppCenterService) waitForAppRunning(ctx context.Context, workspaceID string, appID string) (workspacebiz.AppRuntimeState, error) {
	waitCtx := ctx
	cancel := func() {}
	if _, ok := ctx.Deadline(); !ok {
		waitCtx, cancel = context.WithTimeout(ctx, defaultAppHealthcheckTimeout+5*time.Second)
	}
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		state := s.runner().State(workspaceID, appID)
		switch state.Status {
		case workspacebiz.AppRuntimeStatusRunning:
			return state, nil
		case workspacebiz.AppRuntimeStatusFailed:
			if state.LastError != nil {
				return state, errors.New(*state.LastError)
			}
			return state, errors.New("workspace app runtime failed")
		}
		select {
		case <-waitCtx.Done():
			return state, waitCtx.Err()
		case <-ticker.C:
		}
	}
}
