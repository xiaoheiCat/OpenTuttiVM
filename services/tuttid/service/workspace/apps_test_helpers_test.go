//lint:ignore SA5011 Preserve mechanically moved nil guards whose t.Fatalf calls terminate the current test goroutine.
package workspace

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	preferencesbiz "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/biz/preferences"
	workspacebiz "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/biz/workspace"
	workspacedata "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/data/workspace"
)

type appStoreStub struct {
	packages               map[string]workspacebiz.AppPackage
	packageVersions        map[string]map[string]workspacebiz.AppPackage
	installations          map[string]workspacebiz.AppInstallation
	listPackageVersionsErr error
}

type appCenterPreferencesStoreStub struct {
	preferences preferencesbiz.DesktopPreferences
	err         error
}

type workspaceAppPublisherStub struct {
	published  []workspacebiz.WorkspaceApp
	workspaces []string
}

func (s appCenterPreferencesStoreStub) GetDesktopPreferences(context.Context) (preferencesbiz.DesktopPreferences, error) {
	if s.err != nil {
		return preferencesbiz.DesktopPreferences{}, s.err
	}
	return s.preferences, nil
}

func (s appCenterPreferencesStoreStub) PutDesktopPreferences(_ context.Context, preferences preferencesbiz.DesktopPreferences) (preferencesbiz.DesktopPreferences, error) {
	if s.err != nil {
		return preferencesbiz.DesktopPreferences{}, s.err
	}
	return preferences, nil
}

type appArtifactFetcherStub struct {
	calls []string
	err   error
}

type appRuntimeResolverStub struct {
	called         chan struct{}
	once           sync.Once
	mu             sync.Mutex
	profile        string
	resolveProfile string
	resolveCalls   int
	err            error
}

type preloadThenFailRuntimeResolver struct {
	called   chan struct{}
	once     sync.Once
	mu       sync.Mutex
	calls    int
	startErr error
}

type waitingAppRuntimeResolver struct {
	waitForFetch <-chan int
	called       chan struct{}
	once         sync.Once
	err          error
}

func (f *appArtifactFetcherStub) FetchAppArtifact(_ context.Context, artifactURL string, _ string) error {
	f.calls = append(f.calls, artifactURL)
	if f.err != nil {
		return f.err
	}
	return errors.New("unexpected artifact fetch")
}

type blockingArtifactFetcher struct {
	started   chan struct{}
	release   chan struct{}
	done      chan struct{}
	startOnce sync.Once
	doneOnce  sync.Once
}

func newBlockingArtifactFetcher() *blockingArtifactFetcher {
	return &blockingArtifactFetcher{
		started: make(chan struct{}),
		release: make(chan struct{}),
		done:    make(chan struct{}),
	}
}

func (f *blockingArtifactFetcher) FetchAppArtifact(ctx context.Context, _ string, destinationPath string) error {
	f.startOnce.Do(func() {
		close(f.started)
	})
	defer f.doneOnce.Do(func() {
		close(f.done)
	})
	select {
	case <-f.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	return os.WriteFile(destinationPath, []byte("not a zip"), 0o644)
}

type copyingArtifactFetcher struct {
	sourcePath string
	done       chan struct{}
	doneOnce   sync.Once
}

type trackingArtifactFetcher struct {
	sourcePath string
	entered    chan int
	release    chan struct{}

	mu        sync.Mutex
	calls     int
	active    int
	maxActive int
}

func newCopyingArtifactFetcher(sourcePath string) *copyingArtifactFetcher {
	return &copyingArtifactFetcher{
		sourcePath: sourcePath,
		done:       make(chan struct{}),
	}
}

func (f *copyingArtifactFetcher) FetchAppArtifact(_ context.Context, _ string, destinationPath string) error {
	defer f.doneOnce.Do(func() {
		close(f.done)
	})
	data, err := os.ReadFile(f.sourcePath)
	if err != nil {
		return err
	}
	return os.WriteFile(destinationPath, data, 0o644)
}

func newTrackingArtifactFetcher(sourcePath string) *trackingArtifactFetcher {
	return &trackingArtifactFetcher{
		sourcePath: sourcePath,
		entered:    make(chan int, 2),
		release:    make(chan struct{}),
	}
}

func (f *trackingArtifactFetcher) FetchAppArtifact(ctx context.Context, _ string, destinationPath string) error {
	f.mu.Lock()
	f.calls += 1
	call := f.calls
	f.active += 1
	if f.active > f.maxActive {
		f.maxActive = f.active
	}
	f.mu.Unlock()

	f.entered <- call
	defer func() {
		f.mu.Lock()
		f.active -= 1
		f.mu.Unlock()
	}()
	select {
	case <-f.release:
	case <-ctx.Done():
		return ctx.Err()
	}

	data, err := os.ReadFile(f.sourcePath)
	if err != nil {
		return err
	}
	return os.WriteFile(destinationPath, data, 0o644)
}

func (f *trackingArtifactFetcher) MaxActive() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.maxActive
}

func (r *appRuntimeResolverStub) Resolve(context.Context) (ResolvedAppRuntime, error) {
	r.once.Do(func() {
		close(r.called)
	})
	r.mu.Lock()
	r.resolveCalls += 1
	r.mu.Unlock()
	if r.err != nil {
		return ResolvedAppRuntime{}, r.err
	}
	return ResolvedAppRuntime{}, nil
}

func (r *appRuntimeResolverStub) PreloadProfile(_ context.Context, profile string) error {
	r.once.Do(func() {
		close(r.called)
	})
	r.mu.Lock()
	r.profile = profile
	r.mu.Unlock()
	return r.err
}

func (r *appRuntimeResolverStub) ResolveProfile(_ context.Context, profile string) (ResolvedAppRuntime, error) {
	r.once.Do(func() {
		close(r.called)
	})
	r.mu.Lock()
	r.resolveProfile = profile
	r.mu.Unlock()
	if r.err != nil {
		return ResolvedAppRuntime{}, r.err
	}
	return ResolvedAppRuntime{}, nil
}

func (r *preloadThenFailRuntimeResolver) Resolve(context.Context) (ResolvedAppRuntime, error) {
	r.once.Do(func() {
		close(r.called)
	})
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls += 1
	if r.calls > 1 && r.startErr != nil {
		return ResolvedAppRuntime{}, r.startErr
	}
	return ResolvedAppRuntime{}, nil
}

func (r *waitingAppRuntimeResolver) Resolve(ctx context.Context) (ResolvedAppRuntime, error) {
	r.once.Do(func() {
		close(r.called)
	})
	select {
	case <-r.waitForFetch:
		return ResolvedAppRuntime{}, r.err
	case <-ctx.Done():
		return ResolvedAppRuntime{}, ctx.Err()
	}
}

func (s *workspaceAppPublisherStub) PublishWorkspaceAppUpdated(_ context.Context, workspaceID string, app workspacebiz.WorkspaceApp) error {
	s.workspaces = append(s.workspaces, workspaceID)
	s.published = append(s.published, app)
	return nil
}

func newAppStoreStub() *appStoreStub {
	return &appStoreStub{
		packages:        make(map[string]workspacebiz.AppPackage),
		packageVersions: make(map[string]map[string]workspacebiz.AppPackage),
		installations:   make(map[string]workspacebiz.AppInstallation),
	}
}

func (s *appStoreStub) PutAppPackage(_ context.Context, appPackage workspacebiz.AppPackage) error {
	if s.packageVersions[appPackage.AppID] == nil {
		s.packageVersions[appPackage.AppID] = make(map[string]workspacebiz.AppPackage)
	}
	s.packageVersions[appPackage.AppID][appPackage.Version] = appPackage
	s.packages[appPackage.AppID] = appPackage
	return nil
}

func (s *appStoreStub) PutAppPackageVersion(_ context.Context, appPackage workspacebiz.AppPackage) error {
	if s.packageVersions[appPackage.AppID] == nil {
		s.packageVersions[appPackage.AppID] = make(map[string]workspacebiz.AppPackage)
	}
	s.packageVersions[appPackage.AppID][appPackage.Version] = appPackage
	return nil
}

func (s *appStoreStub) DeleteAppPackage(_ context.Context, appID string) error {
	if _, ok := s.packages[appID]; !ok {
		return workspacedata.ErrWorkspaceAppNotFound
	}
	delete(s.packages, appID)
	delete(s.packageVersions, appID)
	for key, installation := range s.installations {
		if installation.AppID == appID {
			delete(s.installations, key)
		}
	}
	return nil
}

func (s *appStoreStub) DeleteAppPackageVersion(_ context.Context, appID string, version string) error {
	versionPackages, ok := s.packageVersions[appID]
	if !ok {
		return workspacedata.ErrWorkspaceAppNotFound
	}
	if active, ok := s.packages[appID]; ok && active.Version == version {
		return errors.New("active workspace app package version cannot be deleted")
	}
	if _, ok := versionPackages[version]; !ok {
		return workspacedata.ErrWorkspaceAppNotFound
	}
	delete(versionPackages, version)
	if len(versionPackages) == 0 {
		delete(s.packageVersions, appID)
	}
	return nil
}

func (s *appStoreStub) GetAppPackage(_ context.Context, appID string) (workspacebiz.AppPackage, error) {
	appPackage, ok := s.packages[appID]
	if !ok {
		return workspacebiz.AppPackage{}, workspacedata.ErrWorkspaceAppNotFound
	}
	return appPackage, nil
}

func (s *appStoreStub) GetAppPackageVersion(_ context.Context, appID string, version string) (workspacebiz.AppPackage, error) {
	versionPackages, ok := s.packageVersions[appID]
	if !ok {
		return workspacebiz.AppPackage{}, workspacedata.ErrWorkspaceAppNotFound
	}
	appPackage, ok := versionPackages[version]
	if !ok {
		return workspacebiz.AppPackage{}, workspacedata.ErrWorkspaceAppNotFound
	}
	return appPackage, nil
}

func (s *appStoreStub) ListAppPackageVersions(_ context.Context, appID string) ([]workspacebiz.AppPackage, error) {
	if s.listPackageVersionsErr != nil {
		return nil, s.listPackageVersionsErr
	}
	versionPackages, ok := s.packageVersions[appID]
	if !ok {
		return nil, nil
	}
	result := make([]workspacebiz.AppPackage, 0, len(versionPackages))
	for _, appPackage := range versionPackages {
		result = append(result, appPackage)
	}
	sort.SliceStable(result, func(i int, j int) bool {
		if result[i].CreatedAtUnixMs != result[j].CreatedAtUnixMs {
			return result[i].CreatedAtUnixMs > result[j].CreatedAtUnixMs
		}
		return result[i].Version > result[j].Version
	})
	return result, nil
}

func (s *appStoreStub) ListAppPackageFileRecords(_ context.Context, appID string) ([]workspacebiz.AppPackageFileRecord, error) {
	versionPackages, ok := s.packageVersions[appID]
	if !ok {
		return nil, nil
	}
	result := make([]workspacebiz.AppPackageFileRecord, 0, len(versionPackages))
	for _, appPackage := range versionPackages {
		result = append(result, workspacebiz.AppPackageFileRecord{
			AppID:      appPackage.AppID,
			Version:    appPackage.Version,
			PackageDir: appPackage.PackageDir,
			Source:     appPackage.Source,
		})
	}
	sort.SliceStable(result, func(i int, j int) bool {
		return result[i].Version > result[j].Version
	})
	return result, nil
}

func (s *appStoreStub) ListAppPackages(context.Context) ([]workspacebiz.AppPackage, error) {
	result := make([]workspacebiz.AppPackage, 0, len(s.packages))
	for _, appPackage := range s.packages {
		result = append(result, appPackage)
	}
	return result, nil
}

func (s *appStoreStub) PutWorkspaceAppInstallation(_ context.Context, installation workspacebiz.AppInstallation) error {
	s.installations[installation.WorkspaceID+"\x00"+installation.AppID] = installation
	return nil
}

func (s *appStoreStub) SetActiveAppPackageVersion(_ context.Context, appID string, version string) error {
	appPackage, err := s.GetAppPackageVersion(context.Background(), appID, version)
	if err != nil {
		return workspacedata.ErrWorkspaceAppNotFound
	}
	s.packages[appID] = appPackage
	return nil
}

func (s *appStoreStub) DeleteWorkspaceAppInstallation(_ context.Context, workspaceID string, appID string) error {
	key := workspaceID + "\x00" + appID
	if _, ok := s.installations[key]; !ok {
		return workspacedata.ErrWorkspaceAppNotFound
	}
	delete(s.installations, key)
	return nil
}

func (s *appStoreStub) ListWorkspaceAppInstallations(_ context.Context, workspaceID string) ([]workspacebiz.AppInstallation, error) {
	var result []workspacebiz.AppInstallation
	for _, installation := range s.installations {
		if installation.WorkspaceID == workspaceID {
			result = append(result, installation)
		}
	}
	return result, nil
}

func (s *appStoreStub) ListWorkspaceAppInstallationsByApp(_ context.Context, appID string) ([]workspacebiz.AppInstallation, error) {
	var result []workspacebiz.AppInstallation
	for _, installation := range s.installations {
		if installation.AppID == appID {
			result = append(result, installation)
		}
	}
	return result, nil
}
func createWorkspaceAppPackageForTest(t *testing.T, packageDir string, manifest workspacebiz.AppManifest) string {
	t.Helper()
	data := []byte(`{
  "schemaVersion": "` + manifest.SchemaVersion + `",
  "appId": "` + manifest.AppID + `",
  "version": "` + manifest.Version + `",
  "name": "` + manifest.Name + `",
  "description": "` + manifest.Description + `",
  "icon": {
    "type": "` + manifest.Icon.Type + `",
    "src": "` + manifest.Icon.Src + `"
  },
  "runtime": {
    "bootstrap": "` + manifest.Runtime.Bootstrap + `",
    "healthcheckPath": "` + manifest.Runtime.HealthcheckPath + `"
  }
}
`)
	if err := os.WriteFile(filepath.Join(packageDir, "tutti.app.json"), data, 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(packageDir, "bootstrap.sh"), []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("write bootstrap: %v", err)
	}
	if strings.TrimSpace(manifest.Icon.Src) != "" {
		if err := os.WriteFile(filepath.Join(packageDir, manifest.Icon.Src), []byte{0x89, 0x50, 0x4e, 0x47}, 0o644); err != nil {
			t.Fatalf("write icon: %v", err)
		}
	}
	if err := os.WriteFile(filepath.Join(packageDir, "AGENTS.md"), []byte("Test app package.\n"), 0o644); err != nil {
		t.Fatalf("write AGENTS.md: %v", err)
	}
	return packageDir
}

func mustReadManifestForTest(t *testing.T, packageDir string) workspacebiz.AppManifest {
	t.Helper()
	manifest, _, err := workspacebiz.ReadAppManifestFile(filepath.Join(packageDir, "tutti.app.json"))
	if err != nil {
		t.Fatalf("ReadAppManifestFile() error = %v", err)
	}
	return manifest
}

func findWorkspaceAppForTest(apps []workspacebiz.WorkspaceApp, appID string) *workspacebiz.WorkspaceApp {
	for index := range apps {
		if apps[index].Package.AppID == appID {
			return &apps[index]
		}
	}
	return nil
}

func newLaunchTestAppCenterService(t *testing.T) (AppCenterService, *AppRunner) {
	t.Helper()

	ctx := context.Background()
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
		Source:     workspacebiz.AppPackageSourceImported,
	}
	store := newAppStoreStub()
	if err := store.PutAppPackage(ctx, appPackage); err != nil {
		t.Fatalf("PutAppPackage() error = %v", err)
	}
	if err := store.PutWorkspaceAppInstallation(ctx, workspacebiz.AppInstallation{
		WorkspaceID: "ws-1",
		AppID:       "local-app",
		Enabled:     true,
	}); err != nil {
		t.Fatalf("PutWorkspaceAppInstallation() error = %v", err)
	}
	runner := &AppRunner{
		RuntimeResolver: &appRuntimeResolverStub{
			called: make(chan struct{}),
			err:    errors.New("skip runtime"),
		},
	}
	return AppCenterService{
		Store:          store,
		WorkspaceStore: &catalogStoreStub{getWorkspace: workspacebiz.Summary{ID: "ws-1", Name: "Workspace"}},
		Runner:         runner,
		StateDir:       t.TempDir(),
	}, runner
}

func intPtr(value int) *int {
	return &value
}

func waitForActiveAppPackageVersionForTest(t *testing.T, store *appStoreStub, appID string, version string) workspacebiz.AppPackage {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		appPackage, err := store.GetAppPackage(context.Background(), appID)
		if err == nil && appPackage.Version == version {
			return appPackage
		}
		if time.Now().After(deadline) {
			t.Fatalf("GetAppPackage(%s) version = %q, error = %v, want %s", appID, appPackage.Version, err, version)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitForActiveAppPackageDirChangeForTest(t *testing.T, store *appStoreStub, appID string, previousPackageDir string) workspacebiz.AppPackage {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		appPackage, err := store.GetAppPackage(context.Background(), appID)
		if err == nil && appPackage.PackageDir != previousPackageDir {
			return appPackage
		}
		if time.Now().After(deadline) {
			t.Fatalf("GetAppPackage(%s) packageDir = %q, error = %v, want different from %q", appID, appPackage.PackageDir, err, previousPackageDir)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitForInstallJobProgressForTest(t *testing.T, service *AppCenterService, workspaceID string, appID string) workspacebiz.AppInstallProgress {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		job, ok := service.installJob(workspaceID, appID)
		if ok && job.Progress != nil {
			return *job.Progress
		}
		if time.Now().After(deadline) {
			t.Fatalf("install job progress for %s/%s was not published", workspaceID, appID)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
