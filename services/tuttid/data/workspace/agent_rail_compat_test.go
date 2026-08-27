package workspace

import (
	agentstore "github.com/xiaoheiCat/OpenTuttiVM/packages/agent/store-sqlite"
)

// Test-only shims for rail classification helpers that moved into the
// embedded agent store; they keep the pre-extraction test bodies unchanged.

const (
	agentSessionRailSectionKindConversations = agentstore.RailSectionKindConversations
	agentSessionRailSectionKindProject       = agentstore.RailSectionKindProject
	agentSessionRailSectionKeyConversations  = agentstore.RailSectionKeyConversations
)

func normalizeAgentSessionRailPath(path string) string {
	return agentstore.NormalizeProjectPath(path)
}

func agentSessionRailSectionKeyForProject(projectPath string) string {
	return agentstore.RailSectionKeyForProject(projectPath)
}

func classifyAgentSessionRailSection(
	cwd string,
	runtimeContext map[string]any,
	projectPaths []string,
) agentstore.RailSection {
	return agentstore.ClassifyRailSection(cwd, runtimeContext, projectPaths)
}
