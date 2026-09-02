package workspace

import (
	"errors"
	"fmt"
	"strings"

	workspacebiz "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/biz/workspace"
)

func validateAppFactoryManifestMetadata(job workspacebiz.AppFactoryJob, manifest workspacebiz.AppManifest) error {
	if manifest.AppID != job.AppID {
		return fmt.Errorf("app manifest appId must be the generated id %q", job.AppID)
	}
	if !strings.HasPrefix(manifest.AppID, "app_") {
		return errors.New("app manifest appId must use the generated app_ prefix")
	}
	if manifest.Version != defaultFactoryAppVersion {
		return fmt.Errorf("app manifest version must be %q", defaultFactoryAppVersion)
	}

	requestedName := strings.TrimSpace(job.DisplayName)
	if requestedName == "" {
		return errors.New("app factory display name is required")
	}
	if manifest.Name != requestedName {
		return fmt.Errorf("app manifest name must match requested display name %q", requestedName)
	}

	requestedDescription := strings.TrimSpace(job.Description)
	if requestedDescription != "" {
		if manifest.Description != requestedDescription {
			return fmt.Errorf("app manifest description must match requested description %q", requestedDescription)
		}
	} else if isGenericFactoryGeneratedDescription(manifest.Description, manifest.Name) {
		return errors.New("app manifest description must be a generated user-facing description")
	}
	return nil
}

func isGenericFactoryGeneratedDescription(description string, appName string) bool {
	normalized := strings.ToLower(strings.TrimSpace(description))
	if normalized == "" {
		return true
	}
	for _, placeholder := range []string{
		"generated app workspace app.",
		"workspace app workspace app.",
		strings.ToLower(strings.TrimSpace(appName)) + " workspace app.",
	} {
		if normalized == strings.TrimSpace(placeholder) {
			return true
		}
	}
	return false
}
