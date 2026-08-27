package workspace

import (
	"strings"

	workspacebiz "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/biz/workspace"
	builtinapps "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/builtin-apps"
)

func (s *AppCenterService) visibleAppPackagesForCatalog(packages []workspacebiz.AppPackage, builtins []builtinapps.App, installationsByAppID map[string]workspacebiz.AppInstallation) []workspacebiz.AppPackage {
	if !s.shouldHideStaleBuiltinPackages() {
		return packages
	}
	currentBuiltinAppIDs := builtinAppIDSet(builtins)
	result := make([]workspacebiz.AppPackage, 0, len(packages))
	for _, appPackage := range packages {
		if staleUninstalledBuiltinPackage(appPackage, currentBuiltinAppIDs, installationsByAppID) {
			continue
		}
		result = append(result, appPackage)
	}
	return result
}

func (s *AppCenterService) shouldHideStaleBuiltinPackages() bool {
	return s.CatalogLoadState().Status == workspacebiz.AppCatalogLoadStatusReady
}

func staleUninstalledBuiltinPackage(appPackage workspacebiz.AppPackage, currentBuiltinAppIDs map[string]struct{}, installationsByAppID map[string]workspacebiz.AppInstallation) bool {
	if appPackage.Source != workspacebiz.AppPackageSourceBuiltin {
		return false
	}
	if _, installed := installationsByAppID[appPackage.AppID]; installed {
		return false
	}
	_, current := currentBuiltinAppIDs[appPackage.AppID]
	return !current
}

func builtinAppIDSet(builtins []builtinapps.App) map[string]struct{} {
	result := make(map[string]struct{}, len(builtins))
	for _, builtin := range builtins {
		appID := strings.TrimSpace(builtin.Manifest.AppID)
		if appID == "" {
			continue
		}
		result[appID] = struct{}{}
	}
	return result
}
