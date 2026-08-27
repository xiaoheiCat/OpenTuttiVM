package eventstream

import (
	"encoding/json"
	"fmt"
	"strings"

	eventprotocol "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/api/events/generated"
	workspacebiz "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/biz/workspace"
)

func validateWorkspaceAppUpdatedPayload(payload []byte) error {
	var raw struct {
		App map[string]json.RawMessage `json:"app"`
	}
	if err := json.Unmarshal(payload, &raw); err != nil {
		return fmt.Errorf("decode payload: %w", err)
	}
	referencesRaw, ok := raw.App["references"]
	if !ok {
		return fmt.Errorf("app.references is required")
	}
	var references struct {
		ListSupported *bool `json:"listSupported"`
	}
	if err := json.Unmarshal(referencesRaw, &references); err != nil {
		return fmt.Errorf("decode app.references: %w", err)
	}
	if references.ListSupported == nil {
		return fmt.Errorf("app.references.listSupported is required")
	}

	var decoded eventprotocol.WorkspaceAppUpdatedPayload
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return fmt.Errorf("decode payload: %w", err)
	}
	app := decoded.App
	if strings.TrimSpace(app.AppId) == "" {
		return fmt.Errorf("app.appId is required")
	}
	if strings.TrimSpace(app.DisplayName) == "" {
		return fmt.Errorf("app.displayName is required")
	}
	if strings.TrimSpace(app.Version) == "" {
		return fmt.Errorf("app.version is required")
	}
	if app.StateRevision < 0 {
		return fmt.Errorf("app.stateRevision must not be negative")
	}
	switch app.Status {
	case "idle", "preparing", "starting", "running", "installed_pending_restart", "failed", "stopping":
	default:
		return fmt.Errorf("app.status is unsupported")
	}
	switch app.MinimizeBehavior {
	case "hibernate", "keep-mounted":
	default:
		return fmt.Errorf("app.minimizeBehavior is unsupported")
	}
	if app.WindowMinWidth != nil && (*app.WindowMinWidth < workspacebiz.MinAppWindowWidth || *app.WindowMinWidth > workspacebiz.MaxAppWindowWidth) {
		return fmt.Errorf("app.windowMinWidth is unsupported")
	}
	if app.WindowMinHeight != nil && (*app.WindowMinHeight < workspacebiz.MinAppWindowHeight || *app.WindowMinHeight > workspacebiz.MaxAppWindowHeight) {
		return fmt.Errorf("app.windowMinHeight is unsupported")
	}
	return nil
}

func validateWorkspaceAppFactoryJobUpdatedPayload(payload []byte) error {
	var decoded eventprotocol.WorkspaceAppfactoryJobUpdatedPayload
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return fmt.Errorf("decode payload: %w", err)
	}
	job := decoded.Job
	if strings.TrimSpace(job.JobId) == "" {
		return fmt.Errorf("job.jobId is required")
	}
	if strings.TrimSpace(job.WorkspaceId) == "" {
		return fmt.Errorf("job.workspaceId is required")
	}
	switch job.Status {
	case "queued", "generating", "preparing", "validating", "ready", "published", "failed", "canceled":
		return nil
	default:
		return fmt.Errorf("job.status is unsupported")
	}
}
