package api

import (
	"context"
	"net/http"
	"testing"

	tuttigenerated "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/api/generated"
	workspacebiz "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/biz/workspace"
	cliservice "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/cli"
)

func TestDaemonAPIGeneratedRoutesListWorkspaceAppMentionCandidates(t *testing.T) {
	cliProvider := &workspaceAppMentionTestCLIProvider{
		capabilities: []cliservice.Capability{
			workspaceAppMentionTestCapability("installed-app.open", "installed-app", "Installed CLI", "https://cli.example/installed.png", "Installed CLI description"),
			workspaceAppMentionTestCapability("disabled-app.open", "disabled-app", "Disabled CLI", "", "Disabled CLI description"),
			workspaceAppMentionTestCapability("agent-codex.open", "agent-codex", "Codex", "", "Codex CLI description"),
		},
	}
	registry, err := cliservice.NewRegistryFromProviders(cliProvider)
	if err != nil {
		t.Fatalf("NewRegistryFromProviders() error = %v", err)
	}

	mux := http.NewServeMux()
	var gotWorkspaceID string
	RegisterRoutes(
		mux,
		NewRoutes(DaemonAPI{
			AppCenterService: stubWorkspaceAppCenterService{
				listFn: func(_ context.Context, workspaceID string) ([]workspacebiz.WorkspaceApp, error) {
					gotWorkspaceID = workspaceID
					return []workspacebiz.WorkspaceApp{
						workspaceAppMentionTestApp("installed-app", "Installed App", "Installed App description", true, true),
						workspaceAppMentionTestApp("installed-no-cli", "Installed No CLI", "Installed without CLI description", true, true),
						workspaceAppMentionTestApp("disabled-app", "Disabled App", "Disabled App description", true, false),
						workspaceAppMentionTestApp("uninstalled-app", "Uninstalled App", "Uninstalled App description", false, false),
					}, nil
				},
			},
			CLIRegistry: registry,
		}),
	)

	recorder := performGeneratedRouteRequest(
		t,
		mux,
		http.MethodGet,
		"/v1/workspaces/workspace-1/agent-context/workspace-app-mentions",
		nil,
	)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", recorder.Code, recorder.Body.String())
	}
	if gotWorkspaceID != "workspace-1" {
		t.Fatalf("workspace id = %q, want workspace-1", gotWorkspaceID)
	}
	if cliProvider.filterCalls != 0 {
		t.Fatalf("FilterCapabilities called %d times, want 0", cliProvider.filterCalls)
	}

	var response tuttigenerated.WorkspaceAppMentionCandidatesResponse
	decodeGeneratedRouteResponse(t, recorder, &response)
	if response.WorkspaceId != "workspace-1" {
		t.Fatalf("response.workspaceId = %q, want workspace-1", response.WorkspaceId)
	}
	if len(response.Apps) != 2 {
		t.Fatalf("apps len = %d, want 2: %#v", len(response.Apps), response.Apps)
	}

	codex := response.Apps[0]
	if codex.AppId != "agent-codex" || codex.Source != tuttigenerated.WorkspaceAppMentionCandidateSourceCliApp {
		t.Fatalf("first candidate = %#v, want agent-codex cli app", codex)
	}
	if codex.Description != "Codex CLI description" || codex.Cli.CommandCount != 1 {
		t.Fatalf("codex candidate metadata = %#v, want CLI description and one command", codex)
	}

	installed := response.Apps[1]
	if installed.AppId != "installed-app" || installed.Source != tuttigenerated.WorkspaceAppMentionCandidateSourceWorkspaceApp {
		t.Fatalf("second candidate = %#v, want installed workspace app", installed)
	}
	if installed.DisplayName != "Installed App" || installed.Description != "Installed App description" {
		t.Fatalf("installed candidate name/description = %#v", installed)
	}
	if installed.Cli.CommandCount != 1 {
		t.Fatalf("installed command count = %d, want 1", installed.Cli.CommandCount)
	}
	if len(installed.Cli.CommandPaths) != 1 || installed.Cli.CommandPaths[0] != "installed-app open" {
		t.Fatalf("installed command paths = %#v, want installed-app open", installed.Cli.CommandPaths)
	}
	if !installed.References.ListSupported || !installed.References.SearchSupported {
		t.Fatalf("installed references = %#v, want list and search support", installed.References)
	}
}

func TestWorkspaceAppMentionCLIAppsExcludeIntegrationCapabilities(t *testing.T) {
	appCommands := &workspaceAppMentionDynamicCommands{}
	registry := &cliservice.Registry{AppCommands: appCommands}

	appsByID := workspaceAppMentionCLIAppsByID(context.Background(), registry, "workspace-1")

	if len(appCommands.contexts) != 1 {
		t.Fatalf("contexts = %#v, want one call", appCommands.contexts)
	}
	contextValue := appCommands.contexts[0]
	if !contextValue.SkipCapabilityFilters {
		t.Fatalf("SkipCapabilityFilters = false, want true for metadata path")
	}
	if contextValue.IncludeIntegrationCapabilities {
		t.Fatalf("IncludeIntegrationCapabilities = true, want false for mention metadata")
	}

	app := appsByID["automation-app"]
	if app.metadata.CommandCount != 1 {
		t.Fatalf("command count = %d, want only public command", app.metadata.CommandCount)
	}
	if len(app.metadata.CommandPaths) != 1 || app.metadata.CommandPaths[0] != "automation list" {
		t.Fatalf("command paths = %#v, want only public list command", app.metadata.CommandPaths)
	}
}

type workspaceAppMentionTestCLIProvider struct {
	capabilities []cliservice.Capability
	filterCalls  int
}

func (workspaceAppMentionTestCLIProvider) AppID() string {
	return "workspace-app-mention-test"
}

func (p *workspaceAppMentionTestCLIProvider) Commands() []cliservice.Command {
	commands := make([]cliservice.Command, 0, len(p.capabilities))
	for _, capability := range p.capabilities {
		commands = append(commands, cliservice.Command{
			Capability: capability,
			Handler: func(context.Context, cliservice.InvokeRequest) (cliservice.CommandOutput, error) {
				return cliservice.CommandOutput{}, nil
			},
		})
	}
	return commands
}

func (p *workspaceAppMentionTestCLIProvider) FilterCapabilities(context.Context, cliservice.InvokeContext, []cliservice.Capability) []cliservice.Capability {
	p.filterCalls++
	return nil
}

func workspaceAppMentionTestCapability(id string, appID string, appName string, iconURL string, description string) cliservice.Capability {
	return cliservice.Capability{
		ID:          id,
		Path:        []string{appID, "open"},
		Summary:     "Open " + appName,
		Description: "Search " + appName,
		Source: cliservice.CapabilitySource{
			Kind:           cliservice.CapabilitySourceApp,
			AppID:          appID,
			AppName:        appName,
			IconURL:        iconURL,
			CLIDescription: description,
		},
	}
}

func workspaceAppMentionTestApp(appID string, name string, description string, installed bool, enabled bool) workspacebiz.WorkspaceApp {
	iconURL := "https://app.example/" + appID + ".png"
	var installation *workspacebiz.AppInstallation
	if installed {
		installation = &workspacebiz.AppInstallation{
			WorkspaceID: "workspace-1",
			AppID:       appID,
			Enabled:     enabled,
		}
	}
	return workspacebiz.WorkspaceApp{
		Package: workspacebiz.AppPackage{
			AppID:   appID,
			Version: "1.0.0",
			Manifest: workspacebiz.AppManifest{
				AppID:       appID,
				Name:        name,
				Description: description,
				References: &workspacebiz.AppManifestReferences{
					ListEndpoint:   "/references/list",
					SearchEndpoint: "/references/search",
				},
			},
			Source: workspacebiz.AppPackageSourceImported,
		},
		Installation: installation,
		IconURL:      &iconURL,
		Runtime:      workspacebiz.AppRuntimeState{Status: workspacebiz.AppRuntimeStatusIdle},
	}
}

type workspaceAppMentionDynamicCommands struct {
	contexts []cliservice.InvokeContext
}

func (f *workspaceAppMentionDynamicCommands) Capabilities(_ context.Context, contextValue cliservice.InvokeContext) []cliservice.Capability {
	f.contexts = append(f.contexts, contextValue)
	capabilities := []cliservice.Capability{
		workspaceAppMentionAppCapability("app.automation-app.automation.list", "automation-app", []string{"automation", "list"}, "List automations"),
	}
	if contextValue.IncludeIntegrationCapabilities {
		capabilities = append(capabilities,
			workspaceAppMentionAppCapability("app.automation-app.automation.internal-sync", "automation-app", []string{"automation", "internal-sync"}, "Sync internal automation state"),
		)
	}
	return capabilities
}

func (*workspaceAppMentionDynamicCommands) Invoke(context.Context, cliservice.InvokeRequest) (cliservice.CommandOutput, error) {
	return cliservice.CommandOutput{}, cliservice.ErrCommandNotFound
}

func workspaceAppMentionAppCapability(id string, appID string, path []string, summary string) cliservice.Capability {
	return cliservice.Capability{
		ID:      id,
		Path:    path,
		Summary: summary,
		Source: cliservice.CapabilitySource{
			Kind:    cliservice.CapabilitySourceApp,
			AppID:   appID,
			AppName: "Automation",
		},
	}
}
