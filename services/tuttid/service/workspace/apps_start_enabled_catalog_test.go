package workspace

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	preferencesbiz "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/biz/preferences"
	workspacebiz "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/biz/workspace"
	builtinapps "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/builtin-apps"
)

func TestAppCenterServiceStartEnabledSkipsUninstalledRemoteBuiltinUpdate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	oldDir := createWorkspaceAppPackageForTest(t, t.TempDir(), workspacebiz.AppManifest{
		SchemaVersion: workspacebiz.AppManifestSchemaVersionV1,
		AppID:         "large-builtin",
		Version:       "1.0.0",
		Name:          "Large Builtin",
		Description:   "Old large app",
		Icon: workspacebiz.AppManifestIcon{
			Type: "asset",
			Src:  "icon.png",
		},
		Runtime: workspacebiz.AppManifestRuntime{
			Bootstrap:       "bootstrap.sh",
			HealthcheckPath: "/",
		},
	})
	remoteManifest := mustReadManifestForTest(t, oldDir)
	remoteManifest.Version = "1.1.0"

	store := newAppStoreStub()
	if err := store.PutAppPackage(ctx, workspacebiz.AppPackage{
		AppID:      "large-builtin",
		Version:    "1.0.0",
		PackageDir: oldDir,
		Manifest:   mustReadManifestForTest(t, oldDir),
		Source:     workspacebiz.AppPackageSourceBuiltin,
	}); err != nil {
		t.Fatalf("PutAppPackage() error = %v", err)
	}
	fetcher := &appArtifactFetcherStub{}
	service := AppCenterService{
		Store:           store,
		WorkspaceStore:  &catalogStoreStub{getWorkspace: workspacebiz.Summary{ID: "ws-1", Name: "Workspace"}},
		Runner:          &AppRunner{},
		StateDir:        t.TempDir(),
		ArtifactFetcher: fetcher,
		BuiltinCatalog: func() ([]builtinapps.App, error) {
			return []builtinapps.App{{
				Manifest: remoteManifest,
				Distribution: builtinapps.Distribution{
					Kind:           builtinapps.DistributionRemote,
					ArtifactURL:    "https://cdn.example.test/large-builtin.zip",
					ArtifactSHA256: "sha256",
					IconURL:        "https://cdn.example.test/large-builtin.png",
				},
			}}, nil
		},
	}

	apps, err := service.StartEnabled(ctx, "ws-1")
	if err != nil {
		t.Fatalf("StartEnabled() error = %v", err)
	}
	if len(fetcher.calls) != 0 {
		t.Fatalf("artifact fetch calls = %#v, want none", fetcher.calls)
	}
	stored, err := store.GetAppPackage(ctx, "large-builtin")
	if err != nil {
		t.Fatalf("GetAppPackage() error = %v", err)
	}
	if stored.Version != "1.0.0" {
		t.Fatalf("stored package version = %q, want old cached version", stored.Version)
	}
	app := findWorkspaceAppForTest(apps, "large-builtin")
	if app == nil || app.Installation != nil || app.UpdateAvailable || app.Package.Version != "1.1.0" {
		t.Fatalf("remote builtin projection = %#v", app)
	}
}

func TestAppCenterServiceStartEnabledDoesNotBlockOnRemoteBuiltinUpdate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	oldDir := createWorkspaceAppPackageForTest(t, t.TempDir(), workspacebiz.AppManifest{
		SchemaVersion: workspacebiz.AppManifestSchemaVersionV1,
		AppID:         "large-builtin",
		Version:       "1.0.0",
		Name:          "Large Builtin",
		Description:   "Old large app",
		Runtime: workspacebiz.AppManifestRuntime{
			Bootstrap:       "bootstrap.sh",
			HealthcheckPath: "/",
		},
	})
	remoteManifest := mustReadManifestForTest(t, oldDir)
	remoteManifest.Version = "1.1.0"

	store := newAppStoreStub()
	if err := store.PutAppPackage(ctx, workspacebiz.AppPackage{
		AppID:      "large-builtin",
		Version:    "1.0.0",
		PackageDir: oldDir,
		Manifest:   mustReadManifestForTest(t, oldDir),
		Source:     workspacebiz.AppPackageSourceBuiltin,
	}); err != nil {
		t.Fatalf("PutAppPackage() error = %v", err)
	}
	if err := store.PutWorkspaceAppInstallation(ctx, workspacebiz.AppInstallation{
		WorkspaceID: "ws-1",
		AppID:       "large-builtin",
		Enabled:     true,
	}); err != nil {
		t.Fatalf("PutWorkspaceAppInstallation() error = %v", err)
	}
	fetcher := newBlockingArtifactFetcher()
	resolver := &preloadThenFailRuntimeResolver{called: make(chan struct{}), startErr: errors.New("skip runtime")}
	service := AppCenterService{
		Store:           store,
		WorkspaceStore:  &catalogStoreStub{getWorkspace: workspacebiz.Summary{ID: "ws-1", Name: "Workspace"}},
		Runner:          &AppRunner{RuntimeResolver: resolver},
		StateDir:        t.TempDir(),
		ArtifactFetcher: fetcher,
		BuiltinCatalog: func() ([]builtinapps.App, error) {
			return []builtinapps.App{{
				Manifest: remoteManifest,
				Distribution: builtinapps.Distribution{
					Kind:           builtinapps.DistributionRemote,
					ArtifactURL:    "https://cdn.example.test/large-builtin.zip",
					ArtifactSHA256: "sha256",
					IconURL:        "https://cdn.example.test/large-builtin.png",
				},
			}}, nil
		},
	}

	resultCh := make(chan struct {
		apps []workspacebiz.WorkspaceApp
		err  error
	}, 1)
	go func() {
		apps, err := service.StartEnabled(ctx, "ws-1")
		resultCh <- struct {
			apps []workspacebiz.WorkspaceApp
			err  error
		}{apps: apps, err: err}
	}()

	select {
	case result := <-resultCh:
		if result.err != nil {
			t.Fatalf("StartEnabled() error = %v", result.err)
		}
		app := findWorkspaceAppForTest(result.apps, "large-builtin")
		if app == nil || app.Installation == nil || app.Package.Version != "1.0.0" {
			t.Fatalf("StartEnabled() app = %#v", app)
		}
	case <-fetcher.started:
		select {
		case result := <-resultCh:
			if result.err != nil {
				t.Fatalf("StartEnabled() error = %v", result.err)
			}
		case <-time.After(100 * time.Millisecond):
			close(fetcher.release)
			t.Fatal("StartEnabled() blocked on remote builtin artifact download")
		}
	case <-time.After(time.Second):
		close(fetcher.release)
		t.Fatal("StartEnabled() did not return")
	}

	select {
	case <-fetcher.started:
	case <-time.After(time.Second):
		close(fetcher.release)
		t.Fatal("background remote builtin sync did not start")
	}
	progress := waitForInstallJobProgressForTest(t, &service, "ws-1", "large-builtin")
	if progress.UserPhase != workspacebiz.AppInstallUserPhaseDownloading {
		close(fetcher.release)
		t.Fatalf("auto update install progress user phase = %q, want downloading", progress.UserPhase)
	}
	close(fetcher.release)
	select {
	case <-fetcher.done:
	case <-time.After(time.Second):
		t.Fatal("background remote builtin sync did not finish")
	}
}

func TestAppCenterServiceStartEnabledWaitsForRemoteCatalogRefresh(t *testing.T) {
	ctx := context.Background()
	t.Setenv("TUTTI_APP_CATALOG_FILE", "")
	oldDir := createWorkspaceAppPackageForTest(t, t.TempDir(), workspacebiz.AppManifest{
		SchemaVersion: workspacebiz.AppManifestSchemaVersionV1,
		AppID:         "large-builtin",
		Version:       "1.0.0",
		Name:          "Large Builtin",
		Description:   "Old large app",
		Runtime: workspacebiz.AppManifestRuntime{
			Bootstrap:       "bootstrap.sh",
			HealthcheckPath: "/",
		},
	})

	store := newAppStoreStub()
	if err := store.PutAppPackage(ctx, workspacebiz.AppPackage{
		AppID:      "large-builtin",
		Version:    "1.0.0",
		PackageDir: oldDir,
		Manifest:   mustReadManifestForTest(t, oldDir),
		Source:     workspacebiz.AppPackageSourceBuiltin,
	}); err != nil {
		t.Fatalf("PutAppPackage() error = %v", err)
	}
	if err := store.PutWorkspaceAppInstallation(ctx, workspacebiz.AppInstallation{
		WorkspaceID: "ws-1",
		AppID:       "large-builtin",
		Enabled:     true,
	}); err != nil {
		t.Fatalf("PutWorkspaceAppInstallation() error = %v", err)
	}

	releaseCatalog := make(chan struct{})
	releaseCatalogOnce := sync.Once{}
	releaseCatalogResponse := func() {
		releaseCatalogOnce.Do(func() {
			close(releaseCatalog)
		})
	}
	t.Cleanup(releaseCatalogResponse)
	catalogRequested := make(chan struct{})
	var catalogRequestedOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		catalogRequestedOnce.Do(func() {
			close(catalogRequested)
		})
		<-releaseCatalog
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{
			"schemaVersion": "tutti.app.catalog.v1",
			"apps": [
				{
					"manifest": {
						"schemaVersion": "tutti.app.manifest.v1",
						"appId": "large-builtin",
						"version": "1.1.0",
						"name": "Large Builtin",
						"description": "New large app",
						"icon": {"type": "asset", "src": "icon.png"},
						"runtime": {"bootstrap": "bootstrap.sh", "healthcheckPath": "/"}
					},
					"distribution": {
						"kind": "remote",
						"artifactUrl": "https://cdn.example.test/large-builtin.zip",
						"artifactSha256": "sha256",
						"iconUrl": "https://cdn.example.test/large-builtin.png"
					}
				}
			]
		}`))
	}))
	t.Cleanup(server.Close)
	t.Setenv("TUTTI_APP_CATALOG_URL", server.URL+"/catalog.json")

	fetcher := newBlockingArtifactFetcher()
	releaseFetchOnce := sync.Once{}
	releaseFetch := func() {
		releaseFetchOnce.Do(func() {
			close(fetcher.release)
		})
	}
	t.Cleanup(releaseFetch)
	runner := &AppRunner{RuntimeResolver: &preloadThenFailRuntimeResolver{called: make(chan struct{}), startErr: errors.New("skip runtime")}}
	service := AppCenterService{
		Store:           store,
		WorkspaceStore:  &catalogStoreStub{getWorkspace: workspacebiz.Summary{ID: "ws-1", Name: "Workspace"}},
		Runner:          runner,
		StateDir:        t.TempDir(),
		ArtifactFetcher: fetcher,
	}

	resultCh := make(chan struct {
		apps []workspacebiz.WorkspaceApp
		err  error
	}, 1)
	go func() {
		apps, err := service.StartEnabled(ctx, "ws-1")
		resultCh <- struct {
			apps []workspacebiz.WorkspaceApp
			err  error
		}{apps: apps, err: err}
	}()

	select {
	case <-catalogRequested:
	case result := <-resultCh:
		t.Fatalf("StartEnabled() returned before catalog request completed: %#v", result)
	case <-time.After(time.Second):
		t.Fatal("remote catalog was not requested")
	}
	select {
	case result := <-resultCh:
		t.Fatalf("StartEnabled() returned before catalog refresh result: %#v", result)
	case <-time.After(50 * time.Millisecond):
	}

	releaseCatalogResponse()
	var result struct {
		apps []workspacebiz.WorkspaceApp
		err  error
	}
	select {
	case result = <-resultCh:
		if result.err != nil {
			t.Fatalf("StartEnabled() error = %v", result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("StartEnabled() did not return after catalog refresh completed")
	}
	app := findWorkspaceAppForTest(result.apps, "large-builtin")
	if app == nil || app.Installation == nil || app.Package.Version != "1.0.0" || !app.UpdateAvailable || app.AvailableVersion == nil || *app.AvailableVersion != "1.1.0" {
		t.Fatalf("StartEnabled() app = %#v", app)
	}
	if state := runner.State("ws-1", "large-builtin"); state.Status != workspacebiz.AppRuntimeStatusIdle {
		t.Fatalf("runner status = %q, want idle", state.Status)
	}
	select {
	case <-fetcher.started:
	case <-time.After(time.Second):
		t.Fatal("background remote builtin update did not start")
	}
	releaseFetch()
	select {
	case <-fetcher.done:
	case <-time.After(time.Second):
		t.Fatal("background remote builtin update did not finish")
	}
}

func TestAppCenterServiceStartEnabledRefreshesPreferredCatalogChannel(t *testing.T) {
	ctx := context.Background()
	t.Setenv("TUTTI_APP_CATALOG_FILE", "")
	oldDir := createWorkspaceAppPackageForTest(t, t.TempDir(), workspacebiz.AppManifest{
		SchemaVersion: workspacebiz.AppManifestSchemaVersionV1,
		AppID:         "large-builtin",
		Version:       "1.0.0",
		Name:          "Large Builtin",
		Description:   "Old large app",
		Runtime: workspacebiz.AppManifestRuntime{
			Bootstrap:       "bootstrap.sh",
			HealthcheckPath: "/",
		},
	})

	store := newAppStoreStub()
	if err := store.PutAppPackage(ctx, workspacebiz.AppPackage{
		AppID:      "large-builtin",
		Version:    "1.0.0",
		PackageDir: oldDir,
		Manifest:   mustReadManifestForTest(t, oldDir),
		Source:     workspacebiz.AppPackageSourceBuiltin,
	}); err != nil {
		t.Fatalf("PutAppPackage() error = %v", err)
	}
	if err := store.PutWorkspaceAppInstallation(ctx, workspacebiz.AppInstallation{
		WorkspaceID: "ws-1",
		AppID:       "large-builtin",
		Enabled:     true,
	}); err != nil {
		t.Fatalf("PutWorkspaceAppInstallation() error = %v", err)
	}

	fetcher := newBlockingArtifactFetcher()
	close(fetcher.release)
	refreshedURLs := make([]string, 0, 1)
	refreshedHosts := make([]builtinapps.CatalogHost, 0, 1)
	service := AppCenterService{
		Store:           store,
		WorkspaceStore:  &catalogStoreStub{getWorkspace: workspacebiz.Summary{ID: "ws-1", Name: "Workspace"}},
		Runner:          &AppRunner{RuntimeResolver: &preloadThenFailRuntimeResolver{called: make(chan struct{}), startErr: errors.New("skip runtime")}},
		StateDir:        t.TempDir(),
		ArtifactFetcher: fetcher,
		PreferencesStore: appCenterPreferencesStoreStub{
			preferences: preferencesbiz.DesktopPreferences{
				AppCatalogChannel: "staging",
			},
		},
		HostTuttiVersion:      "0.12.0",
		HostTuttiCapabilities: []string{"managed-model-cli-v1"},
		RemoteCatalogRefresher: func(_ context.Context, catalogURL string, host builtinapps.CatalogHost) (builtinapps.CatalogSnapshot, error) {
			refreshedURLs = append(refreshedURLs, catalogURL)
			refreshedHosts = append(refreshedHosts, host)
			return builtinapps.CatalogSnapshot{
				Apps: []builtinapps.App{
					{
						Manifest: workspacebiz.AppManifest{
							SchemaVersion: workspacebiz.AppManifestSchemaVersionV1,
							AppID:         "large-builtin",
							Version:       "1.1.0",
							Name:          "Large Builtin",
							Description:   "New large app",
							Runtime: workspacebiz.AppManifestRuntime{
								Bootstrap:       "bootstrap.sh",
								HealthcheckPath: "/",
							},
						},
						Distribution: builtinapps.Distribution{
							Kind:           builtinapps.DistributionRemote,
							ArtifactURL:    "https://cdn.example.test/large-builtin.zip",
							ArtifactSHA256: "sha256",
							IconURL:        "https://cdn.example.test/large-builtin.png",
						},
					},
				},
				RemoteCatalog: builtinapps.RemoteCatalogLoadState{Status: builtinapps.RemoteCatalogLoadStatusReady},
			}, nil
		},
	}

	apps, err := service.StartEnabled(ctx, "ws-1")
	if err != nil {
		t.Fatalf("StartEnabled() error = %v", err)
	}
	if len(refreshedURLs) != 1 || refreshedURLs[0] != builtinapps.StagingRemoteCatalogURL {
		t.Fatalf("refreshed catalog URLs = %#v, want %#v", refreshedURLs, []string{builtinapps.StagingRemoteCatalogURL})
	}
	if len(refreshedHosts) != 1 || refreshedHosts[0].TuttiVersion != "0.12.0" || len(refreshedHosts[0].Capabilities) != 1 || refreshedHosts[0].Capabilities[0] != "managed-model-cli-v1" {
		t.Fatalf("refreshed catalog hosts = %#v", refreshedHosts)
	}
	app := findWorkspaceAppForTest(apps, "large-builtin")
	if app == nil || !app.UpdateAvailable || app.AvailableVersion == nil || *app.AvailableVersion != "1.1.0" {
		t.Fatalf("StartEnabled() app = %#v", app)
	}
	select {
	case <-fetcher.started:
	case <-time.After(time.Second):
		t.Fatal("background staging remote builtin update did not start")
	}
}

func TestAppCenterServiceStartEnabledFallsBackToCachedCatalogWhenRefreshFails(t *testing.T) {
	ctx := context.Background()
	t.Setenv("TUTTI_APP_CATALOG_FILE", "")
	packageDir := createWorkspaceAppPackageForTest(t, t.TempDir(), workspacebiz.AppManifest{
		SchemaVersion: workspacebiz.AppManifestSchemaVersionV1,
		AppID:         "tutti-onboarding",
		Version:       "0.1.0",
		Name:          "Getting Started",
		Description:   "Learn Tutti and Agent collaboration",
		Runtime: workspacebiz.AppManifestRuntime{
			Bootstrap:       "bootstrap.sh",
			HealthcheckPath: "/healthz",
			Profile:         "standalone",
		},
	})

	store := newAppStoreStub()
	if err := store.PutAppPackage(ctx, workspacebiz.AppPackage{
		AppID:      "tutti-onboarding",
		Version:    "0.1.0",
		PackageDir: packageDir,
		Manifest:   mustReadManifestForTest(t, packageDir),
		Source:     workspacebiz.AppPackageSourceBuiltin,
	}); err != nil {
		t.Fatalf("PutAppPackage() error = %v", err)
	}
	if err := store.PutWorkspaceAppInstallation(ctx, workspacebiz.AppInstallation{
		WorkspaceID: "ws-1",
		AppID:       "tutti-onboarding",
		Enabled:     true,
	}); err != nil {
		t.Fatalf("PutWorkspaceAppInstallation() error = %v", err)
	}

	launchURL := "http://127.0.0.1:41001"
	runner := &AppRunner{}
	runner.ensure()
	runner.mu.Lock()
	runner.states[appRuntimeKey("ws-1", "tutti-onboarding")] = workspacebiz.AppRuntimeState{
		Status:    workspacebiz.AppRuntimeStatusRunning,
		LaunchURL: &launchURL,
	}
	runner.mu.Unlock()

	refreshCalls := 0
	service := AppCenterService{
		Store:          store,
		WorkspaceStore: &catalogStoreStub{getWorkspace: workspacebiz.Summary{ID: "ws-1", Name: "Workspace"}},
		Runner:         runner,
		StateDir:       t.TempDir(),
		RemoteCatalogRefresher: func(context.Context, string, builtinapps.CatalogHost) (builtinapps.CatalogSnapshot, error) {
			refreshCalls += 1
			return builtinapps.CatalogSnapshot{}, errors.New("catalog unavailable")
		},
	}

	apps, err := service.StartEnabled(ctx, "ws-1")
	if err != nil {
		t.Fatalf("StartEnabled() error = %v", err)
	}
	if refreshCalls != 1 {
		t.Fatalf("remote catalog refresh calls = %d, want 1", refreshCalls)
	}
	app := findWorkspaceAppForTest(apps, "tutti-onboarding")
	if app == nil || app.Runtime.Status != workspacebiz.AppRuntimeStatusRunning {
		t.Fatalf("StartEnabled() app = %#v", app)
	}
}

func TestAppCenterServiceStartEnabledDoesNotWaitForRemoteCatalogWhenNoAppsAreEnabled(t *testing.T) {
	ctx := context.Background()
	t.Setenv("TUTTI_APP_CATALOG_FILE", "")

	releaseCatalog := make(chan struct{})
	releaseCatalogOnce := sync.Once{}
	releaseCatalogResponse := func() {
		releaseCatalogOnce.Do(func() {
			close(releaseCatalog)
		})
	}
	t.Cleanup(releaseCatalogResponse)
	catalogRequested := make(chan struct{})
	var catalogRequestedOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		catalogRequestedOnce.Do(func() {
			close(catalogRequested)
		})
		<-releaseCatalog
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{
			"schemaVersion": "tutti.app.catalog.v1",
			"apps": []
		}`))
	}))
	t.Cleanup(server.Close)
	t.Setenv("TUTTI_APP_CATALOG_URL", server.URL+"/catalog.json")

	resolver := &appRuntimeResolverStub{called: make(chan struct{})}
	service := AppCenterService{
		Store:          newAppStoreStub(),
		WorkspaceStore: &catalogStoreStub{getWorkspace: workspacebiz.Summary{ID: "ws-1", Name: "Workspace"}},
		Runner:         &AppRunner{RuntimeResolver: resolver},
		StateDir:       t.TempDir(),
	}

	resultCh := make(chan error, 1)
	go func() {
		_, err := service.StartEnabled(ctx, "ws-1")
		resultCh <- err
	}()

	select {
	case err := <-resultCh:
		if err != nil {
			t.Fatalf("StartEnabled() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("StartEnabled() did not return after catalog refresh completed")
	}
	assertRuntimeResolverNotCalled(t, resolver)

	select {
	case <-catalogRequested:
	case <-time.After(50 * time.Millisecond):
	}
	releaseCatalogResponse()
	resolver.mu.Lock()
	preloadedProfile := resolver.profile
	resolver.mu.Unlock()
	if preloadedProfile != "" {
		t.Fatalf("runtime preload profile = %q, want empty", preloadedProfile)
	}
}
