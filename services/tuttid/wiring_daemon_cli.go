package main

import (
	agentservice "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/agent"
	agentextensionservice "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/agentextension"
	agenttargetservice "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/agenttarget"
	browsersvc "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/browser"
	cliservice "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/cli"
	appclicli "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/cli/appcli"
	agentcontextcli "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/cli/providers/agentcontext"
	browsercli "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/cli/providers/browser"
	computercli "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/cli/providers/computer"
	diagnosticscli "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/cli/providers/diagnostics"
	issuemanagercli "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/cli/providers/issuemanager"
	managedmodelscli "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/cli/providers/managedmodels"
	referencescli "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/cli/providers/references"
	tuttigoalreviewcli "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/cli/providers/tuttigoalreview"
	tuttimodeplancli "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/cli/providers/tuttimodeplan"
	workbenchappscli "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/cli/providers/workbenchapps"
	computersvc "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/computer"
	eventstreamservice "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/eventstream"
	managedcredentialsservice "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/managedcredentials"
	preferencesservice "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/preferences"
	tuttimodeactivationservice "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/tuttimodeactivation"
	tuttimodeexecutionservice "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/tuttimodeexecution"
	tuttimodeplanservice "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/tuttimodeplan"
	workspaceservice "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/workspace"
)

type daemonCLIRegistryInput struct {
	Workspaces           workspaceservice.CatalogService
	Issues               *workspaceservice.IssueManagerService
	Apps                 *workspaceservice.AppCenterService
	Events               *eventstreamservice.Service
	ManagedCredentials   *managedcredentialsservice.Service
	AgentSessions        *agentservice.Service
	AgentTargets         agenttargetservice.Service
	AgentTargetSetup     *agentextensionservice.SetupService
	Preferences          *preferencesservice.Service
	TuttiModePlans       *tuttimodeplanservice.Service
	TuttiModeExecutions  *tuttimodeexecutionservice.Service
	TuttiModeActivations *tuttimodeactivationservice.Service
	Browser              *browsersvc.Service
	Computer             *computersvc.Service
	AppCommands          *appclicli.Registry
}

func buildDaemonCLIRegistry(
	input daemonCLIRegistryInput,
) (*cliservice.Registry, error) {
	providers := []cliservice.Provider{
		diagnosticscli.NewProvider(),
		managedmodelscli.NewProvider(input.ManagedCredentials),
		issuemanagercli.NewProvider(input.Workspaces, input.Issues, input.Apps),
		referencescli.NewProvider(input.Workspaces, input.Apps, input.Issues),
		workbenchappscli.NewProvider(
			input.Workspaces,
			input.Apps,
			eventstreamservice.WorkbenchNodeLaunchPublisher{
				Service: input.Events,
			},
		),
		agentcontextcli.NewProviderWithAgentTargets(
			input.Workspaces,
			input.AgentSessions,
			eventstreamservice.AgentGUILaunchPublisher{
				Service: input.Events,
			},
			input.AgentTargets,
			input.Preferences,
		).WithAgentTargetSetup(input.AgentTargetSetup),
		tuttimodeplancli.NewProviderWithExecutionSnapshot(
			input.Workspaces,
			input.TuttiModePlans,
			input.AgentSessions,
			input.Issues,
			input.Issues,
			input.TuttiModeExecutions,
			input.Issues,
			input.TuttiModeExecutions,
			input.TuttiModeExecutions,
		).WithTuttiModeActivations(input.TuttiModeActivations),
		tuttigoalreviewcli.NewProvider(
			input.TuttiModeExecutions,
			input.AgentSessions,
		),
	}
	if input.Browser != nil {
		providers = append(
			providers,
			browsercli.NewProvider(input.Workspaces, input.Browser, input.AgentSessions),
		)
	}
	if input.Computer != nil {
		providers = append(
			providers,
			computercli.NewProvider(input.Workspaces, input.Computer),
		)
	}
	registry, err := cliservice.NewRegistryFromProviders(providers...)
	if err != nil {
		return nil, err
	}
	registry.AgentSessionCapabilities = agentSessionCLIProjectionResolver{
		Sessions: input.AgentSessions,
	}
	registry.AppCommands = input.AppCommands
	return registry, nil
}
