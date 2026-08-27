package workspace

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	workspacebiz "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/biz/workspace"
)

var (
	ErrAppPackageIconInvalid          = errors.New("workspace app icon is invalid")
	ErrAppPackageIconReplaceForbidden = errors.New("workspace app icon cannot be replaced")
)

func (s *AppCenterService) ReplaceIcon(ctx context.Context, workspaceID string, appID string, sourcePath string) (workspacebiz.WorkspaceApp, error) {
	if _, err := s.workspaceSummary(ctx, workspaceID); err != nil {
		return workspacebiz.WorkspaceApp{}, err
	}

	appPackage, err := s.Store.GetAppPackage(ctx, appID)
	if err != nil {
		return workspacebiz.WorkspaceApp{}, err
	}
	if appPackage.Source != workspacebiz.AppPackageSourceGenerated {
		return workspacebiz.WorkspaceApp{}, fmt.Errorf("%w: app %q has source %q", ErrAppPackageIconReplaceForbidden, appPackage.AppID, appPackage.Source)
	}
	if strings.TrimSpace(appPackage.PackageDir) == "" {
		return workspacebiz.WorkspaceApp{}, errors.New("workspace app package directory is missing")
	}
	packageInfo, err := os.Stat(appPackage.PackageDir)
	if err != nil {
		return workspacebiz.WorkspaceApp{}, fmt.Errorf("stat workspace app package directory: %w", err)
	}
	if !packageInfo.IsDir() {
		return workspacebiz.WorkspaceApp{}, errors.New("workspace app package directory must be a directory")
	}

	iconData, iconExt, err := readWorkspaceAppIconSource(sourcePath)
	if err != nil {
		return workspacebiz.WorkspaceApp{}, err
	}

	iconRelativePath, iconManifestChanged, err := resolveReplacementIconPath(appPackage, iconExt)
	if err != nil {
		return workspacebiz.WorkspaceApp{}, err
	}
	iconPath := filepath.Join(appPackage.PackageDir, filepath.FromSlash(iconRelativePath))
	if err := os.MkdirAll(filepath.Dir(iconPath), 0o755); err != nil {
		return workspacebiz.WorkspaceApp{}, fmt.Errorf("create workspace app icon directory: %w", err)
	}
	if err := os.WriteFile(iconPath, iconData, 0o644); err != nil {
		return workspacebiz.WorkspaceApp{}, fmt.Errorf("write workspace app icon: %w", err)
	}

	if iconManifestChanged {
		appPackage.Manifest.Icon = workspacebiz.AppManifestIcon{
			Type: "asset",
			Src:  iconRelativePath,
		}
	}
	manifestData, err := json.MarshalIndent(appPackage.Manifest, "", "  ")
	if err != nil {
		return workspacebiz.WorkspaceApp{}, fmt.Errorf("serialize workspace app manifest: %w", err)
	}
	manifestData = append(manifestData, '\n')
	manifest, manifestJSON, err := workspacebiz.ParseAppManifestJSON(manifestData)
	if err != nil {
		return workspacebiz.WorkspaceApp{}, err
	}
	if err := os.WriteFile(filepath.Join(appPackage.PackageDir, "tutti.app.json"), manifestData, 0o644); err != nil {
		return workspacebiz.WorkspaceApp{}, fmt.Errorf("write workspace app manifest: %w", err)
	}
	appPackage.Manifest = manifest
	appPackage.ManifestJSON = manifestJSON
	if err := s.Store.PutAppPackage(ctx, appPackage); err != nil {
		return workspacebiz.WorkspaceApp{}, err
	}

	app, err := s.workspaceAppForPackage(ctx, workspaceID, appPackage)
	if err != nil {
		return workspacebiz.WorkspaceApp{}, err
	}
	return s.publishAppIfChanged(ctx, workspaceID, appPackage.AppID, app), nil
}

func resolveReplacementIconPath(appPackage workspacebiz.AppPackage, iconExt string) (string, bool, error) {
	iconRelativePath := strings.TrimSpace(appPackage.Manifest.Icon.Src)
	if isReplaceableExistingIconPath(iconRelativePath, iconExt) {
		iconRelativePath = strings.ReplaceAll(iconRelativePath, `\`, `/`)
		iconPath := filepath.Join(appPackage.PackageDir, filepath.FromSlash(iconRelativePath))
		info, err := os.Stat(iconPath)
		if err == nil {
			if info.IsDir() {
				return "", false, fmt.Errorf("%w: existing icon path is a directory", ErrAppPackageIconInvalid)
			}
			return iconRelativePath, false, nil
		}
		if err != nil && !os.IsNotExist(err) {
			return "", false, fmt.Errorf("stat existing workspace app icon: %w", err)
		}
	}
	return filepath.ToSlash(filepath.Join("assets", "icon-custom"+iconExt)), true, nil
}

func isReplaceableExistingIconPath(iconRelativePath string, iconExt string) bool {
	iconRelativePath = strings.TrimSpace(iconRelativePath)
	if iconRelativePath == "" || filepath.IsAbs(iconRelativePath) || strings.HasPrefix(iconRelativePath, `\`) {
		return false
	}
	lowerPath := strings.ToLower(iconRelativePath)
	if strings.Contains(lowerPath, "://") || strings.HasPrefix(lowerPath, "data:") || strings.Contains(iconRelativePath, ":") {
		return false
	}
	if !strings.EqualFold(filepath.Ext(iconRelativePath), iconExt) {
		return false
	}
	for _, part := range strings.FieldsFunc(iconRelativePath, func(char rune) bool {
		return char == '/' || char == '\\'
	}) {
		if part == ".." {
			return false
		}
	}
	return true
}

func readWorkspaceAppIconSource(sourcePath string) ([]byte, string, error) {
	sourcePath = strings.TrimSpace(sourcePath)
	if sourcePath == "" {
		return nil, "", fmt.Errorf("%w: source path is required", ErrAppPackageIconInvalid)
	}
	ext := strings.ToLower(filepath.Ext(sourcePath))
	if !isSupportedWorkspaceAppIconExtension(ext) {
		return nil, "", fmt.Errorf("%w: must be a png, jpg, jpeg, or webp file", ErrAppPackageIconInvalid)
	}
	info, err := os.Stat(sourcePath)
	if err != nil {
		return nil, "", fmt.Errorf("%w: stat source: %v", ErrAppPackageIconInvalid, err)
	}
	if info.IsDir() {
		return nil, "", fmt.Errorf("%w: source must be a file", ErrAppPackageIconInvalid)
	}
	if info.Size() <= 0 || info.Size() > workspacebiz.MaxAppPackageIconBytes {
		return nil, "", fmt.Errorf("%w: must be between 1 and %d bytes", ErrAppPackageIconInvalid, workspacebiz.MaxAppPackageIconBytes)
	}
	data, err := os.ReadFile(sourcePath)
	if err != nil {
		return nil, "", fmt.Errorf("%w: read source: %v", ErrAppPackageIconInvalid, err)
	}
	if !matchesWorkspaceAppIconSignature(ext, data) {
		return nil, "", fmt.Errorf("%w: file type does not match its extension", ErrAppPackageIconInvalid)
	}
	return data, ext, nil
}

func isSupportedWorkspaceAppIconExtension(ext string) bool {
	switch ext {
	case ".png", ".jpg", ".jpeg", ".webp":
		return true
	default:
		return false
	}
}

func matchesWorkspaceAppIconSignature(ext string, data []byte) bool {
	switch ext {
	case ".png":
		return len(data) >= 8 &&
			data[0] == 0x89 &&
			data[1] == 'P' &&
			data[2] == 'N' &&
			data[3] == 'G' &&
			data[4] == '\r' &&
			data[5] == '\n' &&
			data[6] == 0x1a &&
			data[7] == '\n'
	case ".jpg", ".jpeg":
		return len(data) >= 3 &&
			data[0] == 0xff &&
			data[1] == 0xd8 &&
			data[2] == 0xff
	case ".webp":
		return len(data) >= 12 &&
			string(data[0:4]) == "RIFF" &&
			string(data[8:12]) == "WEBP"
	default:
		return false
	}
}

func (s *AppCenterService) workspaceAppForPackage(ctx context.Context, workspaceID string, appPackage workspacebiz.AppPackage) (workspacebiz.WorkspaceApp, error) {
	var installationPtr *workspacebiz.AppInstallation
	installations, err := s.Store.ListWorkspaceAppInstallations(ctx, workspaceID)
	if err != nil {
		return workspacebiz.WorkspaceApp{}, err
	}
	for _, installation := range installations {
		if installation.AppID == appPackage.AppID {
			installationCopy := installation
			installationPtr = &installationCopy
			break
		}
	}

	runtimeState := workspacebiz.AppRuntimeState{Status: workspacebiz.AppRuntimeStatusIdle}
	if installationPtr != nil {
		runtimeState = s.runner().State(workspaceID, appPackage.AppID)
	}

	app := workspacebiz.WorkspaceApp{
		Package:      appPackage,
		Installation: installationPtr,
		Runtime:      runtimeState,
	}
	app.CLI = s.appCLIState(workspaceID, app)
	return app, nil
}
