package api

import (
	"context"
	"sort"
	"strings"

	tuttigenerated "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/api/generated"
	workspaceapi "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/api/workspace"
	"github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/apierrors"
	workspacebiz "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/biz/workspace"
	cliservice "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/cli"
)

func (api DaemonAPI) ListWorkspaceAppMentionCandidates(ctx context.Context, request tuttigenerated.ListWorkspaceAppMentionCandidatesRequestObject) (tuttigenerated.ListWorkspaceAppMentionCandidatesResponseObject, error) {
	if api.AppCenterService == nil {
		return tuttigenerated.ListWorkspaceAppMentionCandidates503JSONResponse{
			ServiceUnavailableErrorJSONResponse: workspaceAppServiceUnavailableError(),
		}, nil
	}

	workspaceID := strings.TrimSpace(string(request.WorkspaceID))
	if workspaceID == "" {
		return tuttigenerated.ListWorkspaceAppMentionCandidates400JSONResponse{
			InvalidRequestErrorJSONResponse: invalidRequestError(
				apierrors.MissingWorkspaceID(
					apierrors.WithDeveloperMessage("workspace id is required"),
					apierrors.WithParams(map[string]any{"field": "workspaceId"}),
				),
			),
		}, nil
	}

	apps, err := api.AppCenterService.List(ctx, workspaceID)
	if err != nil {
		return writeListWorkspaceAppMentionCandidatesError(err), nil
	}

	cliAppsByID := workspaceAppMentionCLIAppsByID(ctx, api.CLIRegistry, workspaceID)
	candidates := make([]tuttigenerated.WorkspaceAppMentionCandidate, 0, len(apps)+len(cliAppsByID))
	knownAppIDs := make(map[string]struct{}, len(apps))
	for _, app := range apps {
		appID := normalizedWorkspaceAppMentionID(app.Package.AppID)
		if appID == "" {
			continue
		}
		knownAppIDs[appID] = struct{}{}
		if app.Installation == nil || !app.Installation.Enabled {
			continue
		}
		cliApp, hasCLIApp := cliAppsByID[appID]
		if !hasCLIApp || cliApp.metadata.CommandCount == 0 {
			continue
		}
		candidates = append(
			candidates,
			workspaceAppMentionCandidateFromApp(app, cliApp),
		)
	}
	for appID, cliApp := range cliAppsByID {
		if _, exists := knownAppIDs[appID]; exists {
			continue
		}
		candidate, ok := workspaceAppMentionCandidateFromCLIApp(cliApp)
		if ok {
			candidates = append(candidates, candidate)
		}
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		return strings.ToLower(candidates[i].AppId) < strings.ToLower(candidates[j].AppId)
	})

	return tuttigenerated.ListWorkspaceAppMentionCandidates200JSONResponse{
		WorkspaceId: workspaceID,
		Apps:        candidates,
	}, nil
}

type workspaceAppMentionCLIApp struct {
	appID       string
	displayName string
	description string
	iconURL     string
	metadata    tuttigenerated.WorkspaceAppMentionCliMetadata
}

func workspaceAppMentionCLIAppsByID(ctx context.Context, registry *cliservice.Registry, workspaceID string) map[string]workspaceAppMentionCLIApp {
	if registry == nil {
		return map[string]workspaceAppMentionCLIApp{}
	}
	capabilities := registry.Capabilities(ctx, cliservice.InvokeContext{
		Source:                "agent-context",
		WorkspaceID:           workspaceID,
		SkipCapabilityFilters: true,
	})
	appsByID := make(map[string]workspaceAppMentionCLIApp)
	for _, capability := range capabilities {
		if capability.Source.Kind != cliservice.CapabilitySourceApp {
			continue
		}
		appID := strings.TrimSpace(capability.Source.AppID)
		normalizedAppID := normalizedWorkspaceAppMentionID(appID)
		if normalizedAppID == "" {
			continue
		}
		cliApp := appsByID[normalizedAppID]
		if cliApp.appID == "" {
			cliApp = workspaceAppMentionCLIApp{
				appID:       appID,
				displayName: firstNonBlank(capability.Source.AppName, appID),
				metadata:    emptyWorkspaceAppMentionCliMetadata(),
			}
		}
		if iconURL := strings.TrimSpace(capability.Source.IconURL); iconURL != "" {
			cliApp.iconURL = iconURL
		}
		if cliApp.description == "" {
			cliApp.description = firstNonBlank(
				capability.Source.CLIDescription,
				capability.Source.AppDescription,
				capability.Description,
			)
		}
		cliApp.metadata.CommandCount++
		cliApp.metadata.Scopes = appendUniqueNonBlank(
			cliApp.metadata.Scopes,
			firstCapabilityPathSegment(capability),
		)
		cliApp.metadata.CommandPaths = appendUniqueNonBlank(
			cliApp.metadata.CommandPaths,
			strings.Join(capability.Path, " "),
		)
		cliApp.metadata.CommandSummaries = appendUniqueNonBlank(
			cliApp.metadata.CommandSummaries,
			capability.Summary,
		)
		cliApp.metadata.CommandDescriptions = appendUniqueNonBlank(
			cliApp.metadata.CommandDescriptions,
			capability.Description,
		)
		appsByID[normalizedAppID] = cliApp
	}
	return appsByID
}

func workspaceAppMentionCandidateFromApp(app workspacebiz.WorkspaceApp, cliApp workspaceAppMentionCLIApp) tuttigenerated.WorkspaceAppMentionCandidate {
	cliMetadata := cliApp.metadata
	if cliMetadata.CommandCount == 0 {
		cliMetadata = emptyWorkspaceAppMentionCliMetadata()
	}
	return tuttigenerated.WorkspaceAppMentionCandidate{
		AppId:            app.Package.AppID,
		DisplayName:      app.Package.DisplayName(),
		Description:      app.Package.Description(),
		IconUrl:          app.ResolvedIconURL(),
		AvailableIconUrl: app.AvailableIconURL,
		Installed:        true,
		Enabled:          true,
		Source:           tuttigenerated.WorkspaceAppMentionCandidateSourceWorkspaceApp,
		Localizations:    workspaceapi.GeneratedAppLocalizationsFromBiz(app.Package.Localizations()),
		References: tuttigenerated.WorkspaceAppReferencesState{
			ListSupported:   app.Package.ReferenceListSupported(),
			SearchSupported: app.Package.ReferenceSearchSupported(),
		},
		Cli: cliMetadata,
	}
}

func workspaceAppMentionCandidateFromCLIApp(cliApp workspaceAppMentionCLIApp) (tuttigenerated.WorkspaceAppMentionCandidate, bool) {
	appID := strings.TrimSpace(cliApp.appID)
	if appID == "" {
		return tuttigenerated.WorkspaceAppMentionCandidate{}, false
	}
	displayName := firstNonBlank(cliApp.displayName, appID)
	return tuttigenerated.WorkspaceAppMentionCandidate{
		AppId:            appID,
		DisplayName:      displayName,
		Description:      strings.TrimSpace(cliApp.description),
		IconUrl:          stringPointerIfNotBlank(strings.TrimSpace(cliApp.iconURL)),
		AvailableIconUrl: nil,
		Installed:        true,
		Enabled:          true,
		Source:           tuttigenerated.WorkspaceAppMentionCandidateSourceCliApp,
		Localizations:    []tuttigenerated.WorkspaceAppLocalization{},
		References:       tuttigenerated.WorkspaceAppReferencesState{},
		Cli:              cliApp.metadata,
	}, true
}

func emptyWorkspaceAppMentionCliMetadata() tuttigenerated.WorkspaceAppMentionCliMetadata {
	return tuttigenerated.WorkspaceAppMentionCliMetadata{
		CommandDescriptions: []string{},
		CommandPaths:        []string{},
		CommandSummaries:    []string{},
		Scopes:              []string{},
	}
}

func normalizedWorkspaceAppMentionID(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func firstCapabilityPathSegment(capability cliservice.Capability) string {
	if len(capability.Path) == 0 {
		return ""
	}
	return capability.Path[0]
}

func firstNonBlank(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func appendUniqueNonBlank(values []string, value string) []string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return values
	}
	for _, existing := range values {
		if existing == trimmed {
			return values
		}
	}
	return append(values, trimmed)
}

func writeListWorkspaceAppMentionCandidatesError(err error) tuttigenerated.ListWorkspaceAppMentionCandidatesResponseObject {
	protocolErr := apierrors.Classify(err)
	switch protocolErr.Code {
	case tuttigenerated.WorkspaceNotFound:
		return tuttigenerated.ListWorkspaceAppMentionCandidates404JSONResponse{
			WorkspaceNotFoundErrorJSONResponse: workspaceNotFoundError(protocolErr),
		}
	case tuttigenerated.InvalidRequest:
		return tuttigenerated.ListWorkspaceAppMentionCandidates400JSONResponse{
			InvalidRequestErrorJSONResponse: invalidRequestError(protocolErr),
		}
	default:
		return tuttigenerated.ListWorkspaceAppMentionCandidates502JSONResponse{
			WorkspaceOperationErrorJSONResponse: workspaceOperationError(protocolErr),
		}
	}
}
