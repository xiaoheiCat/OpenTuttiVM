package workspace

import (
	"context"
	"errors"
	"strings"

	"github.com/google/uuid"
	preferencesbiz "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/biz/preferences"
	workspacebiz "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/biz/workspace"
	workspacedata "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/data/workspace"
)

type CatalogService struct {
	Store            workspacedata.CatalogStore
	PreferencesStore workspacedata.PreferencesStore
}

type CreateInput struct {
	Name string
}

type UpdateInput struct {
	Name string
}

type DeleteResult struct {
	WorkspaceID string
}

func (s CatalogService) Startup(ctx context.Context) (*workspacebiz.Summary, error) {
	if s.Store == nil {
		return nil, errors.New("workspace catalog store is not configured")
	}

	workspace, err := s.Store.GetStartup(ctx)
	if err != nil {
		return nil, err
	}
	if workspace != nil {
		return workspace, nil
	}

	workspaces, err := s.Store.List(ctx)
	if err != nil {
		return nil, err
	}
	if len(workspaces) > 0 {
		opened, err := s.Open(ctx, workspaces[0].ID)
		if err != nil {
			return nil, err
		}
		return &opened, nil
	}

	created, err := s.Create(ctx, CreateInput{
		Name: s.defaultWorkspaceName(ctx),
	})
	if err != nil {
		return nil, err
	}

	opened, err := s.Open(ctx, created.ID)
	if err != nil {
		return nil, err
	}
	return &opened, nil
}

func (s CatalogService) Create(ctx context.Context, request CreateInput) (workspacebiz.Summary, error) {
	if s.Store == nil {
		return workspacebiz.Summary{}, errors.New("workspace catalog store is not configured")
	}

	name := strings.TrimSpace(request.Name)
	if name == "" {
		return workspacebiz.Summary{}, errors.New("workspace name is required")
	}

	workspace := workspacebiz.Summary{
		ID:   uuid.NewString(),
		Name: name,
	}

	if err := s.Store.Create(ctx, workspace); err != nil {
		return workspacebiz.Summary{}, err
	}

	return workspace, nil
}

func (s CatalogService) Open(ctx context.Context, workspaceID string) (workspacebiz.Summary, error) {
	if s.Store == nil {
		return workspacebiz.Summary{}, errors.New("workspace catalog store is not configured")
	}

	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return workspacebiz.Summary{}, errors.New("workspace id is required")
	}

	workspace, err := s.Store.Open(ctx, workspaceID)
	if err != nil {
		return workspacebiz.Summary{}, err
	}

	return workspace, nil
}

func (s CatalogService) Get(ctx context.Context, workspaceID string) (workspacebiz.Summary, error) {
	if s.Store == nil {
		return workspacebiz.Summary{}, errors.New("workspace catalog store is not configured")
	}

	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return workspacebiz.Summary{}, errors.New("workspace id is required")
	}

	workspace, err := s.Store.Get(ctx, workspaceID)
	if err != nil {
		return workspacebiz.Summary{}, err
	}

	return workspace, nil
}

func (s CatalogService) Update(ctx context.Context, workspaceID string, request UpdateInput) (workspacebiz.Summary, error) {
	if s.Store == nil {
		return workspacebiz.Summary{}, errors.New("workspace catalog store is not configured")
	}

	workspaceID = strings.TrimSpace(workspaceID)
	name := strings.TrimSpace(request.Name)
	if workspaceID == "" {
		return workspacebiz.Summary{}, errors.New("workspace id is required")
	}
	if name == "" {
		return workspacebiz.Summary{}, errors.New("workspace name is required")
	}

	workspace := workspacebiz.Summary{
		ID:   workspaceID,
		Name: name,
	}
	if err := s.Store.Update(ctx, workspace); err != nil {
		return workspacebiz.Summary{}, err
	}

	return s.Store.Get(ctx, workspaceID)
}

func (s CatalogService) Delete(ctx context.Context, workspaceID string) (DeleteResult, error) {
	if s.Store == nil {
		return DeleteResult{}, errors.New("workspace catalog store is not configured")
	}

	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return DeleteResult{}, errors.New("workspace id is required")
	}

	if err := s.Store.Delete(ctx, workspaceID); err != nil {
		return DeleteResult{}, err
	}

	return DeleteResult{
		WorkspaceID: workspaceID,
	}, nil
}

func (s CatalogService) List(ctx context.Context) ([]workspacebiz.Summary, error) {
	if s.Store == nil {
		return nil, errors.New("workspace catalog store is not configured")
	}

	workspaces, err := s.Store.List(ctx)
	if err != nil {
		return nil, err
	}

	return workspaces, nil
}

func (s CatalogService) defaultWorkspaceName(ctx context.Context) string {
	locale := preferencesbiz.DefaultDesktopLocale
	if s.PreferencesStore != nil {
		preferences, err := s.PreferencesStore.GetDesktopPreferences(ctx)
		if err == nil && preferencesbiz.IsDesktopLocale(preferences.Locale) {
			locale = preferences.Locale
		}
	}

	switch locale {
	case "zh-CN":
		return "默认空间"
	default:
		return "default"
	}
}
