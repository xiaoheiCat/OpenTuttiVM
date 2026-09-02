package workspace

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	workspacebiz "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/biz/workspace"
)

func TestAppCenterServiceReplaceIconUpdatesGeneratedPackage(t *testing.T) {
	t.Parallel()

	store := newAppStoreStub()
	packageDir := createWorkspaceAppPackageForTest(t, t.TempDir(), workspacebiz.AppManifest{
		SchemaVersion: workspacebiz.AppManifestSchemaVersionV1,
		AppID:         "local-app",
		Version:       "0.1.0",
		Name:          "Local App",
		Description:   "Local app",
		Icon: workspacebiz.AppManifestIcon{
			Type: "asset",
			Src:  "icon.png",
		},
		Runtime: workspacebiz.AppManifestRuntime{
			Bootstrap:       "bootstrap.sh",
			HealthcheckPath: "/ready",
		},
	})
	appPackage := workspacebiz.AppPackage{
		AppID:      "local-app",
		Version:    "0.1.0",
		PackageDir: packageDir,
		Manifest:   mustReadManifestForTest(t, packageDir),
		Source:     workspacebiz.AppPackageSourceGenerated,
	}
	if err := store.PutAppPackage(context.Background(), appPackage); err != nil {
		t.Fatalf("PutAppPackage() error = %v", err)
	}
	if err := store.PutWorkspaceAppInstallation(context.Background(), workspacebiz.AppInstallation{
		WorkspaceID: "ws-1",
		AppID:       "local-app",
		Enabled:     true,
	}); err != nil {
		t.Fatalf("PutWorkspaceAppInstallation() error = %v", err)
	}
	sourceIconPath := filepath.Join(t.TempDir(), "replacement.png")
	if err := os.WriteFile(sourceIconPath, []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'}, 0o644); err != nil {
		t.Fatalf("write source icon: %v", err)
	}
	publisher := &workspaceAppPublisherStub{}
	service := AppCenterService{
		Store:          store,
		WorkspaceStore: &catalogStoreStub{getWorkspace: workspacebiz.Summary{ID: "ws-1", Name: "Workspace"}},
		Runner:         &AppRunner{},
		Publisher:      publisher,
		StateDir:       t.TempDir(),
	}

	app, err := service.ReplaceIcon(context.Background(), "ws-1", "local-app", sourceIconPath)
	if err != nil {
		t.Fatalf("ReplaceIcon() error = %v", err)
	}
	if app.Package.Manifest.Icon.Src != "icon.png" {
		t.Fatalf("replaced icon src = %q", app.Package.Manifest.Icon.Src)
	}
	replacedIconData, err := os.ReadFile(filepath.Join(packageDir, "icon.png"))
	if err != nil {
		t.Fatalf("read replaced icon: %v", err)
	}
	if string(replacedIconData) != string([]byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'}) {
		t.Fatalf("replaced icon data = %v", replacedIconData)
	}
	manifest := mustReadManifestForTest(t, packageDir)
	if manifest.Icon.Src != "icon.png" {
		t.Fatalf("manifest icon src = %q", manifest.Icon.Src)
	}
	stored, err := store.GetAppPackage(context.Background(), "local-app")
	if err != nil {
		t.Fatalf("GetAppPackage() error = %v", err)
	}
	if stored.Manifest.Icon.Src != "icon.png" {
		t.Fatalf("stored icon src = %q", stored.Manifest.Icon.Src)
	}
	if len(publisher.published) != 1 || publisher.published[0].Package.Manifest.Icon.Src != "icon.png" {
		t.Fatalf("published updates = %#v", publisher.published)
	}
}

func TestAppCenterServiceReplaceIconUsesCustomPathWhenExistingIconExtensionDiffers(t *testing.T) {
	t.Parallel()

	store := newAppStoreStub()
	packageDir := createWorkspaceAppPackageForTest(t, t.TempDir(), workspacebiz.AppManifest{
		SchemaVersion: workspacebiz.AppManifestSchemaVersionV1,
		AppID:         "local-app",
		Version:       "0.1.0",
		Name:          "Local App",
		Description:   "Local app",
		Icon: workspacebiz.AppManifestIcon{
			Type: "asset",
			Src:  "icon.webp",
		},
		Runtime: workspacebiz.AppManifestRuntime{
			Bootstrap:       "bootstrap.sh",
			HealthcheckPath: "/ready",
		},
	})
	appPackage := workspacebiz.AppPackage{
		AppID:      "local-app",
		Version:    "0.1.0",
		PackageDir: packageDir,
		Manifest:   mustReadManifestForTest(t, packageDir),
		Source:     workspacebiz.AppPackageSourceGenerated,
	}
	if err := store.PutAppPackage(context.Background(), appPackage); err != nil {
		t.Fatalf("PutAppPackage() error = %v", err)
	}
	sourceIconPath := filepath.Join(t.TempDir(), "replacement.png")
	if err := os.WriteFile(sourceIconPath, []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'}, 0o644); err != nil {
		t.Fatalf("write source icon: %v", err)
	}
	service := AppCenterService{
		Store:          store,
		WorkspaceStore: &catalogStoreStub{getWorkspace: workspacebiz.Summary{ID: "ws-1", Name: "Workspace"}},
		Runner:         &AppRunner{},
		StateDir:       t.TempDir(),
	}

	app, err := service.ReplaceIcon(context.Background(), "ws-1", "local-app", sourceIconPath)
	if err != nil {
		t.Fatalf("ReplaceIcon() error = %v", err)
	}
	if app.Package.Manifest.Icon.Src != "assets/icon-custom.png" {
		t.Fatalf("replaced icon src = %q", app.Package.Manifest.Icon.Src)
	}
	if _, err := os.Stat(filepath.Join(packageDir, "assets", "icon-custom.png")); err != nil {
		t.Fatalf("custom icon stat error = %v", err)
	}
}

func TestAppCenterServiceReplaceIconRejectsBuiltinApp(t *testing.T) {
	t.Parallel()

	store := newAppStoreStub()
	if err := store.PutAppPackage(context.Background(), workspacebiz.AppPackage{
		AppID:   "builtin-app",
		Version: "0.1.0",
		Manifest: workspacebiz.AppManifest{
			AppID: "builtin-app",
			Name:  "Builtin App",
		},
		Source: workspacebiz.AppPackageSourceBuiltin,
	}); err != nil {
		t.Fatalf("PutAppPackage() error = %v", err)
	}
	service := AppCenterService{
		Store:          store,
		WorkspaceStore: &catalogStoreStub{getWorkspace: workspacebiz.Summary{ID: "ws-1", Name: "Workspace"}},
		Runner:         &AppRunner{},
		StateDir:       t.TempDir(),
	}

	if _, err := service.ReplaceIcon(context.Background(), "ws-1", "builtin-app", "/tmp/icon.png"); !errors.Is(err, ErrAppPackageIconReplaceForbidden) {
		t.Fatalf("ReplaceIcon() error = %v, want ErrAppPackageIconReplaceForbidden", err)
	}
}

func TestAppCenterServiceReplaceIconRejectsInvalidIcon(t *testing.T) {
	t.Parallel()

	store := newAppStoreStub()
	packageDir := createWorkspaceAppPackageForTest(t, t.TempDir(), workspacebiz.AppManifest{
		SchemaVersion: workspacebiz.AppManifestSchemaVersionV1,
		AppID:         "local-app",
		Version:       "0.1.0",
		Name:          "Local App",
		Description:   "Local app",
		Icon: workspacebiz.AppManifestIcon{
			Type: "asset",
			Src:  "icon.png",
		},
		Runtime: workspacebiz.AppManifestRuntime{
			Bootstrap:       "bootstrap.sh",
			HealthcheckPath: "/ready",
		},
	})
	if err := store.PutAppPackage(context.Background(), workspacebiz.AppPackage{
		AppID:      "local-app",
		Version:    "0.1.0",
		PackageDir: packageDir,
		Manifest:   mustReadManifestForTest(t, packageDir),
		Source:     workspacebiz.AppPackageSourceGenerated,
	}); err != nil {
		t.Fatalf("PutAppPackage() error = %v", err)
	}
	sourceIconPath := filepath.Join(t.TempDir(), "replacement.png")
	if err := os.WriteFile(sourceIconPath, []byte("not png data"), 0o644); err != nil {
		t.Fatalf("write source icon: %v", err)
	}
	service := AppCenterService{
		Store:          store,
		WorkspaceStore: &catalogStoreStub{getWorkspace: workspacebiz.Summary{ID: "ws-1", Name: "Workspace"}},
		Runner:         &AppRunner{},
		StateDir:       t.TempDir(),
	}

	if _, err := service.ReplaceIcon(context.Background(), "ws-1", "local-app", sourceIconPath); !errors.Is(err, ErrAppPackageIconInvalid) {
		t.Fatalf("ReplaceIcon() error = %v, want ErrAppPackageIconInvalid", err)
	}
}
