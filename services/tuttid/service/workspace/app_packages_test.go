package workspace

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	workspacebiz "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/biz/workspace"
	workspacedata "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/data/workspace"
)

func TestAppCenterServiceImportsAndExportsUserPackageArchives(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	stateDir := t.TempDir()
	sourceDir := createWorkspaceAppPackageForTest(t, t.TempDir(), workspacebiz.AppManifest{
		SchemaVersion: workspacebiz.AppManifestSchemaVersionV1,
		AppID:         "imported-app",
		Version:       "0.2.0",
		Name:          "Imported App",
		Description:   "Imported app",
		Runtime: workspacebiz.AppManifestRuntime{
			Bootstrap:       "bootstrap.sh",
			HealthcheckPath: "/",
		},
	})
	archivePath := filepath.Join(t.TempDir(), "imported-app.zip")
	if err := createAppPackageZip(sourceDir, archivePath); err != nil {
		t.Fatalf("createAppPackageZip() error = %v", err)
	}
	store := newAppStoreStub()
	service := AppCenterService{
		Store:    store,
		StateDir: stateDir,
	}

	imported, err := service.ImportPackage(ctx, archivePath)
	if err != nil {
		t.Fatalf("ImportPackage() error = %v", err)
	}
	if imported.Package.Source != workspacebiz.AppPackageSourceImported || imported.Package.PackageDir == "" {
		t.Fatalf("imported app = %#v", imported)
	}
	if _, err := service.ImportPackage(ctx, archivePath); !errors.Is(err, ErrAppPackageAlreadyExists) {
		t.Fatalf("ImportPackage() duplicate error = %v, want ErrAppPackageAlreadyExists", err)
	}
	exportPath := filepath.Join(t.TempDir(), "exported.zip")
	exported, err := service.ExportPackage(ctx, "imported-app", "", exportPath)
	if err != nil {
		t.Fatalf("ExportPackage() error = %v", err)
	}
	if exported.Path != exportPath || exported.ArtifactSHA256 == "" || exported.ArtifactSizeBytes <= 0 {
		t.Fatalf("ExportPackage() = %#v", exported)
	}
	if _, err := os.Stat(exportPath); err != nil {
		t.Fatalf("export archive missing: %v", err)
	}
}

func TestAppCenterServiceDeletePackageRemovesLocalApp(t *testing.T) {
	t.Parallel()

	store := newAppStoreStub()
	packageDir := createWorkspaceAppPackageForTest(t, t.TempDir(), workspacebiz.AppManifest{
		SchemaVersion: workspacebiz.AppManifestSchemaVersionV1,
		AppID:         "local-app",
		Version:       "0.1.0",
		Name:          "Local App",
		Description:   "Local app",
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
	service := AppCenterService{
		Store:          store,
		WorkspaceStore: &catalogStoreStub{getWorkspace: workspacebiz.Summary{ID: "ws-1", Name: "Workspace"}},
		Runner:         &AppRunner{},
		StateDir:       t.TempDir(),
	}

	if err := service.DeletePackage(context.Background(), "ws-1", "local-app"); err != nil {
		t.Fatalf("DeletePackage() error = %v", err)
	}
	if _, err := os.Stat(packageDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("deleted package dir stat error = %v, want not exist", err)
	}
	if _, err := store.GetAppPackage(context.Background(), "local-app"); !errors.Is(err, workspacedata.ErrWorkspaceAppNotFound) {
		t.Fatalf("GetAppPackage() after delete error = %v, want ErrWorkspaceAppNotFound", err)
	}
	if installations, err := store.ListWorkspaceAppInstallations(context.Background(), "ws-1"); err != nil || len(installations) != 0 {
		t.Fatalf("installations after delete = %#v error = %v, want empty", installations, err)
	}
}

func TestAppCenterServiceDeletePackageRemovesFactoryJobFiles(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	stateDir := t.TempDir()
	store := newAppStoreStub()
	factoryStore := newAppFactoryStoreStub()
	packageDir := createWorkspaceAppPackageForTest(t, t.TempDir(), workspacebiz.AppManifest{
		SchemaVersion: workspacebiz.AppManifestSchemaVersionV1,
		AppID:         "local-app",
		Version:       "0.1.0",
		Name:          "Local App",
		Description:   "Local app",
		Runtime: workspacebiz.AppManifestRuntime{
			Bootstrap:       "bootstrap.sh",
			HealthcheckPath: "/ready",
		},
	})
	appPackage := workspacebiz.AppPackage{
		AppID:                "local-app",
		Version:              "0.1.0",
		PackageDir:           packageDir,
		Manifest:             mustReadManifestForTest(t, packageDir),
		Source:               workspacebiz.AppPackageSourceGenerated,
		FactoryJobID:         "job-1",
		CreatedInWorkspaceID: "ws-1",
	}
	if err := store.PutAppPackage(ctx, appPackage); err != nil {
		t.Fatalf("PutAppPackage() error = %v", err)
	}
	jobRoot := filepath.Join(stateDir, "apps", "factory", "jobs", "job-1")
	if err := os.MkdirAll(filepath.Join(jobRoot, "draft"), 0o755); err != nil {
		t.Fatalf("create factory job draft: %v", err)
	}
	if err := factoryStore.PutAppFactoryJob(ctx, workspacebiz.AppFactoryJob{
		WorkspaceID: "ws-1",
		JobID:       "job-1",
		Status:      workspacebiz.AppFactoryJobStatusPublished,
		DraftDir:    filepath.Join(jobRoot, "draft"),
	}); err != nil {
		t.Fatalf("PutAppFactoryJob() error = %v", err)
	}
	service := AppCenterService{
		Store:           store,
		AppFactoryStore: factoryStore,
		WorkspaceStore:  &catalogStoreStub{getWorkspace: workspacebiz.Summary{ID: "ws-1", Name: "Workspace"}},
		Runner:          &AppRunner{},
		StateDir:        stateDir,
	}

	if err := service.DeletePackage(ctx, "ws-1", "local-app"); err != nil {
		t.Fatalf("DeletePackage() error = %v", err)
	}
	if _, err := os.Stat(jobRoot); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("factory job root stat error = %v, want not exist", err)
	}
	if _, err := factoryStore.GetAppFactoryJob(ctx, "ws-1", "job-1"); err != nil {
		t.Fatalf("factory job record error = %v, want retained", err)
	}
}

func TestAppCenterServiceDeletePackagePrunesEmptyPackageCacheParent(t *testing.T) {
	t.Parallel()

	store := newAppStoreStub()
	stateDir := t.TempDir()
	service := AppCenterService{
		Store:          store,
		WorkspaceStore: &catalogStoreStub{getWorkspace: workspacebiz.Summary{ID: "ws-1", Name: "Workspace"}},
		Runner:         &AppRunner{},
		StateDir:       stateDir,
	}
	appID := "local-app"
	for _, version := range []string{"0.1.0", "0.2.0"} {
		packageDir := service.packageCacheDir(appID, version)
		if err := os.MkdirAll(packageDir, 0o755); err != nil {
			t.Fatalf("create package dir: %v", err)
		}
		createWorkspaceAppPackageForTest(t, packageDir, workspacebiz.AppManifest{
			SchemaVersion: workspacebiz.AppManifestSchemaVersionV1,
			AppID:         appID,
			Version:       version,
			Name:          "Local App",
			Description:   "Local app",
			Runtime: workspacebiz.AppManifestRuntime{
				Bootstrap:       "bootstrap.sh",
				HealthcheckPath: "/ready",
			},
		})
		if err := store.PutAppPackage(context.Background(), workspacebiz.AppPackage{
			AppID:      appID,
			Version:    version,
			PackageDir: packageDir,
			Manifest:   mustReadManifestForTest(t, packageDir),
			Source:     workspacebiz.AppPackageSourceGenerated,
		}); err != nil {
			t.Fatalf("PutAppPackage(%s) error = %v", version, err)
		}
	}
	appPackageParent := filepath.Join(service.packageCacheRoot(), appID)

	if err := service.DeletePackage(context.Background(), "ws-1", appID); err != nil {
		t.Fatalf("DeletePackage() error = %v", err)
	}
	if _, err := os.Stat(appPackageParent); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("deleted app package parent stat error = %v, want not exist", err)
	}
	if _, err := os.Stat(service.packageCacheRoot()); err != nil {
		t.Fatalf("package cache root stat error = %v, want exists", err)
	}
}

func TestAppCenterServiceDeletePackageKeepsNonEmptyPackageCacheParent(t *testing.T) {
	t.Parallel()

	store := newAppStoreStub()
	stateDir := t.TempDir()
	service := AppCenterService{
		Store:          store,
		WorkspaceStore: &catalogStoreStub{getWorkspace: workspacebiz.Summary{ID: "ws-1", Name: "Workspace"}},
		Runner:         &AppRunner{},
		StateDir:       stateDir,
	}
	appID := "local-app"
	packageDir := service.packageCacheDir(appID, "0.1.0")
	if err := os.MkdirAll(packageDir, 0o755); err != nil {
		t.Fatalf("create package dir: %v", err)
	}
	createWorkspaceAppPackageForTest(t, packageDir, workspacebiz.AppManifest{
		SchemaVersion: workspacebiz.AppManifestSchemaVersionV1,
		AppID:         appID,
		Version:       "0.1.0",
		Name:          "Local App",
		Description:   "Local app",
		Runtime: workspacebiz.AppManifestRuntime{
			Bootstrap:       "bootstrap.sh",
			HealthcheckPath: "/ready",
		},
	})
	if err := store.PutAppPackage(context.Background(), workspacebiz.AppPackage{
		AppID:      appID,
		Version:    "0.1.0",
		PackageDir: packageDir,
		Manifest:   mustReadManifestForTest(t, packageDir),
		Source:     workspacebiz.AppPackageSourceGenerated,
	}); err != nil {
		t.Fatalf("PutAppPackage() error = %v", err)
	}
	appPackageParent := filepath.Join(service.packageCacheRoot(), appID)
	keptDir := filepath.Join(appPackageParent, "manual-cache")
	if err := os.MkdirAll(keptDir, 0o755); err != nil {
		t.Fatalf("create kept package dir: %v", err)
	}

	if err := service.DeletePackage(context.Background(), "ws-1", appID); err != nil {
		t.Fatalf("DeletePackage() error = %v", err)
	}
	if _, err := os.Stat(packageDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("deleted package dir stat error = %v, want not exist", err)
	}
	if _, err := os.Stat(keptDir); err != nil {
		t.Fatalf("kept package dir stat error = %v, want exists", err)
	}
	if _, err := os.Stat(appPackageParent); err != nil {
		t.Fatalf("non-empty app package parent stat error = %v, want exists", err)
	}
}

func TestAppCenterServiceDeletePackageRemovesWorkspaceAppState(t *testing.T) {
	t.Parallel()

	store := newAppStoreStub()
	packageDir := createWorkspaceAppPackageForTest(t, t.TempDir(), workspacebiz.AppManifest{
		SchemaVersion: workspacebiz.AppManifestSchemaVersionV1,
		AppID:         "local-app",
		Version:       "0.1.0",
		Name:          "Local App",
		Description:   "Local app",
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
	for _, workspaceID := range []string{"ws-1", "ws-2"} {
		if err := store.PutWorkspaceAppInstallation(context.Background(), workspacebiz.AppInstallation{
			WorkspaceID: workspaceID,
			AppID:       "local-app",
			Enabled:     true,
		}); err != nil {
			t.Fatalf("PutWorkspaceAppInstallation() error = %v", err)
		}
	}
	service := AppCenterService{
		Store:          store,
		WorkspaceStore: &catalogStoreStub{getWorkspace: workspacebiz.Summary{ID: "ws-1", Name: "Workspace"}},
		Runner:         &AppRunner{},
		StateDir:       t.TempDir(),
	}
	stateRoots := []string{
		service.workspaceAppStateRoot("ws-1", "local-app"),
		service.workspaceAppStateRoot("ws-2", "local-app"),
	}
	for _, stateRoot := range stateRoots {
		if err := os.MkdirAll(filepath.Join(stateRoot, "data"), 0o755); err != nil {
			t.Fatalf("create app data dir: %v", err)
		}
		if err := os.WriteFile(filepath.Join(stateRoot, "data", "app.sqlite"), []byte("sqlite data"), 0o644); err != nil {
			t.Fatalf("write app data: %v", err)
		}
	}

	if err := service.DeletePackage(context.Background(), "ws-1", "local-app"); err != nil {
		t.Fatalf("DeletePackage() error = %v", err)
	}
	for _, stateRoot := range stateRoots {
		if _, err := os.Stat(stateRoot); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("deleted app state root stat error = %v, want not exist", err)
		}
	}
}

func TestAppCenterServiceDeletePackageRejectsBuiltinApp(t *testing.T) {
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

	if err := service.DeletePackage(context.Background(), "ws-1", "builtin-app"); !errors.Is(err, ErrAppPackageDeleteForbidden) {
		t.Fatalf("DeletePackage() error = %v, want ErrAppPackageDeleteForbidden", err)
	}
}
