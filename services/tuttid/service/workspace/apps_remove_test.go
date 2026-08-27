package workspace

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	workspacebiz "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/biz/workspace"
	builtinapps "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/builtin-apps"
	workspacedata "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/data/workspace"
)

func TestAppCenterServiceRemovePublishesUninstalledAppUpdate(t *testing.T) {
	t.Parallel()

	store := newAppStoreStub()
	appPackage := workspacebiz.AppPackage{
		AppID:   "sample-app",
		Version: "0.1.0",
		Manifest: workspacebiz.AppManifest{
			AppID:       "sample-app",
			Name:        "Sample App",
			Description: "Sample app",
		},
	}
	if err := store.PutAppPackage(context.Background(), appPackage); err != nil {
		t.Fatalf("PutAppPackage() error = %v", err)
	}
	if err := store.PutWorkspaceAppInstallation(context.Background(), workspacebiz.AppInstallation{
		WorkspaceID: "ws-1",
		AppID:       "sample-app",
		Enabled:     true,
	}); err != nil {
		t.Fatalf("PutWorkspaceAppInstallation() error = %v", err)
	}
	publisher := &workspaceAppPublisherStub{}
	service := AppCenterService{
		Store:          store,
		WorkspaceStore: &catalogStoreStub{getWorkspace: workspacebiz.Summary{ID: "ws-1", Name: "Workspace"}},
		Runner:         &AppRunner{},
		Publisher:      publisher,
		StateDir:       t.TempDir(),
	}

	removed, err := service.Remove(context.Background(), "ws-1", "sample-app")
	if err != nil {
		t.Fatalf("Remove() error = %v", err)
	}
	if removed.Installation != nil || removed.Runtime.Status != workspacebiz.AppRuntimeStatusIdle {
		t.Fatalf("Remove() = %#v", removed)
	}
	if removed.StateRevision != 1 {
		t.Fatalf("removed state revision = %d, want 1", removed.StateRevision)
	}
	if len(publisher.published) != 1 {
		t.Fatalf("published updates = %d, want 1", len(publisher.published))
	}
	published := publisher.published[0]
	if publisher.workspaces[0] != "ws-1" || published.Installation != nil || published.StateRevision != 1 || published.Runtime.Status != workspacebiz.AppRuntimeStatusIdle {
		t.Fatalf("published update = workspace %q app %#v", publisher.workspaces[0], published)
	}
}

func TestAppCenterServiceRemoveDeletesWorkspaceAppState(t *testing.T) {
	t.Parallel()

	store := newAppStoreStub()
	appPackage := workspacebiz.AppPackage{
		AppID:   "sample-app",
		Version: "0.1.0",
		Manifest: workspacebiz.AppManifest{
			AppID:       "sample-app",
			Name:        "Sample App",
			Description: "Sample app",
		},
	}
	if err := store.PutAppPackage(context.Background(), appPackage); err != nil {
		t.Fatalf("PutAppPackage() error = %v", err)
	}
	if err := store.PutWorkspaceAppInstallation(context.Background(), workspacebiz.AppInstallation{
		WorkspaceID: "ws-1",
		AppID:       "sample-app",
		Enabled:     true,
	}); err != nil {
		t.Fatalf("PutWorkspaceAppInstallation() error = %v", err)
	}
	service := AppCenterService{
		Store:          store,
		WorkspaceStore: &catalogStoreStub{getWorkspace: workspacebiz.Summary{ID: "ws-1", Name: "Workspace"}},
		Runner:         &AppRunner{},
		StateDir:       t.TempDir(),
	}
	stateRoot := service.workspaceAppStateRoot("ws-1", "sample-app")
	if err := os.MkdirAll(filepath.Join(stateRoot, "database"), 0o755); err != nil {
		t.Fatalf("create app database dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(stateRoot, "database", "app.sqlite"), []byte("sqlite data"), 0o644); err != nil {
		t.Fatalf("write app database: %v", err)
	}

	if _, err := service.Remove(context.Background(), "ws-1", "sample-app"); err != nil {
		t.Fatalf("Remove() error = %v", err)
	}
	if _, err := os.Stat(stateRoot); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("removed app state root stat error = %v, want not exist", err)
	}
}

func TestWorkspaceAppStateRootUsesHashedInstallationScope(t *testing.T) {
	t.Parallel()

	stateDir := t.TempDir()
	service := AppCenterService{StateDir: stateDir}

	first := service.workspaceAppStateRoot("workspace-1", "sample-app")
	second := service.workspaceAppStateRoot("workspace-2", "sample-app")

	if strings.Contains(first, "workspace-1") || strings.Contains(first, "workspace-2") {
		t.Fatalf("workspace app state root leaks workspace id: %q", first)
	}
	if first == second {
		t.Fatalf("workspace app state roots collide: %q", first)
	}
	wantPrefix := filepath.Join(stateDir, "apps", "installations", "sample-app") + string(os.PathSeparator)
	if !strings.HasPrefix(first, wantPrefix) {
		t.Fatalf("workspace app state root = %q, want prefix %q", first, wantPrefix)
	}
}

func TestAppCenterServiceRemoveDeletesUnusedRemoteBuiltinPackage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newAppStoreStub()
	packageDir := filepath.Join(t.TempDir(), "packages", "remote-builtin", "1.0.0")
	if err := os.MkdirAll(packageDir, 0o755); err != nil {
		t.Fatalf("create package dir: %v", err)
	}
	appPackage := workspacebiz.AppPackage{
		AppID:      "remote-builtin",
		Version:    "1.0.0",
		PackageDir: packageDir,
		Source:     workspacebiz.AppPackageSourceBuiltin,
		Manifest: workspacebiz.AppManifest{
			AppID:       "remote-builtin",
			Version:     "1.0.0",
			Name:        "Remote Builtin",
			Description: "Remote builtin",
		},
	}
	if err := store.PutAppPackage(ctx, appPackage); err != nil {
		t.Fatalf("PutAppPackage() error = %v", err)
	}
	if err := store.PutWorkspaceAppInstallation(ctx, workspacebiz.AppInstallation{
		WorkspaceID: "ws-1",
		AppID:       "remote-builtin",
		Enabled:     true,
	}); err != nil {
		t.Fatalf("PutWorkspaceAppInstallation() error = %v", err)
	}
	service := AppCenterService{
		Store:          store,
		WorkspaceStore: &catalogStoreStub{getWorkspace: workspacebiz.Summary{ID: "ws-1", Name: "Workspace"}},
		Runner:         &AppRunner{},
		StateDir:       t.TempDir(),
		BuiltinCatalog: func() ([]builtinapps.App, error) {
			return []builtinapps.App{
				{
					Manifest: appPackage.Manifest,
					Distribution: builtinapps.Distribution{
						Kind: builtinapps.DistributionRemote,
					},
				},
			}, nil
		},
	}

	if _, err := service.Remove(ctx, "ws-1", "remote-builtin"); err != nil {
		t.Fatalf("Remove() error = %v", err)
	}
	if _, err := store.GetAppPackage(ctx, "remote-builtin"); !errors.Is(err, workspacedata.ErrWorkspaceAppNotFound) {
		t.Fatalf("GetAppPackage() after remove error = %v, want ErrWorkspaceAppNotFound", err)
	}
	if _, err := os.Stat(packageDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("removed package dir stat error = %v, want not exist", err)
	}
	installations, err := store.ListWorkspaceAppInstallationsByApp(ctx, "remote-builtin")
	if err != nil {
		t.Fatalf("ListWorkspaceAppInstallationsByApp() error = %v", err)
	}
	if len(installations) != 0 {
		t.Fatalf("installations after remove = %#v, want empty", installations)
	}
}

func TestAppCenterServiceRemoveDeletesUnusedRemoteBuiltinPackageWithInvalidCachedManifest(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newAppStoreStub()
	stateDir := t.TempDir()
	activePackageDir := filepath.Join(stateDir, "packages", "remote-builtin", "1.0.0")
	stalePackageDir := filepath.Join(stateDir, "packages", "remote-builtin", "0.9.0")
	for _, packageDir := range []string{activePackageDir, stalePackageDir} {
		if err := os.MkdirAll(packageDir, 0o755); err != nil {
			t.Fatalf("create package dir %q: %v", packageDir, err)
		}
	}
	appPackage := workspacebiz.AppPackage{
		AppID:      "remote-builtin",
		Version:    "1.0.0",
		PackageDir: activePackageDir,
		Source:     workspacebiz.AppPackageSourceBuiltin,
		Manifest: workspacebiz.AppManifest{
			AppID:       "remote-builtin",
			Version:     "1.0.0",
			Name:        "Remote Builtin",
			Description: "Remote builtin",
		},
	}
	if err := store.PutAppPackage(ctx, appPackage); err != nil {
		t.Fatalf("PutAppPackage() error = %v", err)
	}
	if err := store.PutAppPackageVersion(ctx, workspacebiz.AppPackage{
		AppID:      "remote-builtin",
		Version:    "0.9.0",
		PackageDir: stalePackageDir,
		Source:     workspacebiz.AppPackageSourceBuiltin,
	}); err != nil {
		t.Fatalf("PutAppPackageVersion(stale) error = %v", err)
	}
	store.listPackageVersionsErr = errors.New("scan workspace app package version: app manifest references.listEndpoint is required when references is provided")
	if err := store.PutWorkspaceAppInstallation(ctx, workspacebiz.AppInstallation{
		WorkspaceID: "ws-1",
		AppID:       "remote-builtin",
		Enabled:     true,
	}); err != nil {
		t.Fatalf("PutWorkspaceAppInstallation() error = %v", err)
	}
	service := AppCenterService{
		Store:          store,
		WorkspaceStore: &catalogStoreStub{getWorkspace: workspacebiz.Summary{ID: "ws-1", Name: "Workspace"}},
		Runner:         &AppRunner{},
		StateDir:       t.TempDir(),
		BuiltinCatalog: func() ([]builtinapps.App, error) {
			return []builtinapps.App{
				{
					Manifest: appPackage.Manifest,
					Distribution: builtinapps.Distribution{
						Kind: builtinapps.DistributionRemote,
					},
				},
			}, nil
		},
	}

	if _, err := service.Remove(ctx, "ws-1", "remote-builtin"); err != nil {
		t.Fatalf("Remove() error = %v", err)
	}
	for _, packageDir := range []string{activePackageDir, stalePackageDir} {
		if _, err := os.Stat(packageDir); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("removed package dir %q stat error = %v, want not exist", packageDir, err)
		}
	}
}

func TestAppCenterServiceRemoveRemoteBuiltinKeepsLocalDevPackageDir(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newAppStoreStub()
	stateDir := t.TempDir()
	builtinPackageDir := filepath.Join(stateDir, "packages", "remote-builtin", "1.0.0")
	importedPackageDir := filepath.Join(stateDir, "packages", "remote-builtin", "imported")
	localDevPackageDir := filepath.Join(stateDir, "local-dev", "remote-builtin")
	for _, packageDir := range []string{builtinPackageDir, importedPackageDir, localDevPackageDir} {
		if err := os.MkdirAll(packageDir, 0o755); err != nil {
			t.Fatalf("create package dir %q: %v", packageDir, err)
		}
	}
	appPackage := workspacebiz.AppPackage{
		AppID:      "remote-builtin",
		Version:    "1.0.0",
		PackageDir: builtinPackageDir,
		Source:     workspacebiz.AppPackageSourceBuiltin,
		Manifest: workspacebiz.AppManifest{
			AppID:       "remote-builtin",
			Version:     "1.0.0",
			Name:        "Remote Builtin",
			Description: "Remote builtin",
		},
	}
	if err := store.PutAppPackage(ctx, appPackage); err != nil {
		t.Fatalf("PutAppPackage() error = %v", err)
	}
	if err := store.PutAppPackageVersion(ctx, workspacebiz.AppPackage{
		AppID:      "remote-builtin",
		Version:    "imported",
		PackageDir: importedPackageDir,
		Source:     workspacebiz.AppPackageSourceImported,
	}); err != nil {
		t.Fatalf("PutAppPackageVersion(imported) error = %v", err)
	}
	if err := store.PutAppPackageVersion(ctx, workspacebiz.AppPackage{
		AppID:      "remote-builtin",
		Version:    "local-dev",
		PackageDir: localDevPackageDir,
		Source:     workspacebiz.AppPackageSourceLocalDev,
	}); err != nil {
		t.Fatalf("PutAppPackageVersion(local-dev) error = %v", err)
	}
	if err := store.PutWorkspaceAppInstallation(ctx, workspacebiz.AppInstallation{
		WorkspaceID: "ws-1",
		AppID:       "remote-builtin",
		Enabled:     true,
	}); err != nil {
		t.Fatalf("PutWorkspaceAppInstallation() error = %v", err)
	}
	service := AppCenterService{
		Store:          store,
		WorkspaceStore: &catalogStoreStub{getWorkspace: workspacebiz.Summary{ID: "ws-1", Name: "Workspace"}},
		Runner:         &AppRunner{},
		StateDir:       t.TempDir(),
		BuiltinCatalog: func() ([]builtinapps.App, error) {
			return []builtinapps.App{
				{
					Manifest: appPackage.Manifest,
					Distribution: builtinapps.Distribution{
						Kind: builtinapps.DistributionRemote,
					},
				},
			}, nil
		},
	}

	if _, err := service.Remove(ctx, "ws-1", "remote-builtin"); err != nil {
		t.Fatalf("Remove() error = %v", err)
	}
	if _, err := os.Stat(builtinPackageDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("removed builtin package dir stat error = %v, want not exist", err)
	}
	if _, err := os.Stat(importedPackageDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("removed imported package dir stat error = %v, want not exist", err)
	}
	if info, err := os.Stat(localDevPackageDir); err != nil || !info.IsDir() {
		t.Fatalf("local-dev package dir stat = %#v, %v, want existing dir", info, err)
	}
}

func TestAppCenterServiceRemoveKeepsRemoteBuiltinPackageUsedByAnotherWorkspace(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newAppStoreStub()
	packageDir := filepath.Join(t.TempDir(), "packages", "remote-builtin", "1.0.0")
	if err := os.MkdirAll(packageDir, 0o755); err != nil {
		t.Fatalf("create package dir: %v", err)
	}
	appPackage := workspacebiz.AppPackage{
		AppID:      "remote-builtin",
		Version:    "1.0.0",
		PackageDir: packageDir,
		Source:     workspacebiz.AppPackageSourceBuiltin,
		Manifest: workspacebiz.AppManifest{
			AppID:       "remote-builtin",
			Version:     "1.0.0",
			Name:        "Remote Builtin",
			Description: "Remote builtin",
		},
	}
	if err := store.PutAppPackage(ctx, appPackage); err != nil {
		t.Fatalf("PutAppPackage() error = %v", err)
	}
	for _, workspaceID := range []string{"ws-1", "ws-2"} {
		if err := store.PutWorkspaceAppInstallation(ctx, workspacebiz.AppInstallation{
			WorkspaceID: workspaceID,
			AppID:       "remote-builtin",
			Enabled:     true,
		}); err != nil {
			t.Fatalf("PutWorkspaceAppInstallation(%s) error = %v", workspaceID, err)
		}
	}
	service := AppCenterService{
		Store:          store,
		WorkspaceStore: &catalogStoreStub{getWorkspace: workspacebiz.Summary{ID: "ws-1", Name: "Workspace"}},
		Runner:         &AppRunner{},
		StateDir:       t.TempDir(),
		BuiltinCatalog: func() ([]builtinapps.App, error) {
			return []builtinapps.App{
				{
					Manifest: appPackage.Manifest,
					Distribution: builtinapps.Distribution{
						Kind: builtinapps.DistributionRemote,
					},
				},
			}, nil
		},
	}

	if _, err := service.Remove(ctx, "ws-1", "remote-builtin"); err != nil {
		t.Fatalf("Remove() error = %v", err)
	}
	if _, err := store.GetAppPackage(ctx, "remote-builtin"); err != nil {
		t.Fatalf("GetAppPackage() after remove error = %v", err)
	}
	if _, err := os.Stat(packageDir); err != nil {
		t.Fatalf("package dir after remove stat error = %v", err)
	}
	installations, err := store.ListWorkspaceAppInstallationsByApp(ctx, "remote-builtin")
	if err != nil {
		t.Fatalf("ListWorkspaceAppInstallationsByApp() error = %v", err)
	}
	if len(installations) != 1 || installations[0].WorkspaceID != "ws-2" {
		t.Fatalf("installations after remove = %#v, want ws-2 only", installations)
	}
}
