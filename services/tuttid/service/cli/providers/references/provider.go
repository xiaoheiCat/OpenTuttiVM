package references

import (
	cliservice "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/cli"
	"github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/cli/providers/refresolve"
)

const appID = "references"

// AppReferences / IssueOutputs are the unified egresses to app / task produced artifacts.
// Defined in refresolve (shared with inline issue-detail reference expansion); aliased here so
// the provider constructor signature stays stable.
type (
	AppReferences = refresolve.AppReferences
	IssueOutputs  = refresolve.IssueOutputs
)

// Provider backs the `reference list` CLI command. It resolves a workspace-reference
// mention handle (app+group / topic+issue) into a flat list of artifact files, so the
// agent can read them on demand instead of receiving every path pre-expanded.
type Provider struct {
	workspaces cliservice.WorkspaceCatalog
	apps       AppReferences
	issues     IssueOutputs
}

func NewProvider(workspaces cliservice.WorkspaceCatalog, apps AppReferences, issues IssueOutputs) Provider {
	return Provider{workspaces: workspaces, apps: apps, issues: issues}
}

func (Provider) AppID() string {
	return appID
}

func (p Provider) Commands() []cliservice.Command {
	return []cliservice.Command{p.newReferenceListCommand()}
}
