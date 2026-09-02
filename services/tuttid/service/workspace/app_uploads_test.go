package workspace

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	workspacebiz "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/biz/workspace"
)

func TestAppCenterServiceWorkspaceAppUploadWritesManagedFile(t *testing.T) {
	service := newAppUploadTestService(t)
	content := []byte("hello upload")
	now := time.Date(2026, time.June, 24, 12, 0, 0, 0, time.UTC)

	session, err := service.PrepareWorkspaceAppUpload(context.Background(), "ws-1", "canvas", PrepareWorkspaceAppUploadInput{
		MimeType:  " image/png ",
		Name:      " image.PNG ",
		Now:       now,
		Purpose:   WorkspaceAppUploadPurposeAppAsset,
		SizeBytes: int64(len(content)),
	})
	if err != nil {
		t.Fatalf("PrepareWorkspaceAppUpload() error = %v", err)
	}
	if session.UploadID == "" {
		t.Fatal("UploadID is empty")
	}

	if err := service.PutWorkspaceAppUploadContent(context.Background(), "ws-1", "canvas", session.UploadID, PutWorkspaceAppUploadContentInput{
		Body: bytes.NewReader(content),
		Now:  now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("PutWorkspaceAppUploadContent() error = %v", err)
	}
	file, err := service.CompleteWorkspaceAppUpload(context.Background(), "ws-1", "canvas", session.UploadID, now.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("CompleteWorkspaceAppUpload() error = %v", err)
	}

	hash := sha256.Sum256(content)
	wantSHA := hex.EncodeToString(hash[:])
	wantPath := filepath.Join(service.workspaceAppUploadDir("ws-1", "canvas"), wantSHA[:2], wantSHA+".png")
	if file.Path != wantPath {
		t.Fatalf("uploaded path = %q, want %q", file.Path, wantPath)
	}
	if file.Name != "image.PNG" {
		t.Fatalf("name = %q, want image.PNG", file.Name)
	}
	if file.MimeType != "image/png" {
		t.Fatalf("mimeType = %q, want image/png", file.MimeType)
	}
	if file.SizeBytes != int64(len(content)) {
		t.Fatalf("sizeBytes = %d, want %d", file.SizeBytes, len(content))
	}
	if file.SHA256 != wantSHA {
		t.Fatalf("sha256 = %q, want %q", file.SHA256, wantSHA)
	}
	gotContent, err := os.ReadFile(file.Path)
	if err != nil {
		t.Fatalf("read uploaded file: %v", err)
	}
	if !bytes.Equal(gotContent, content) {
		t.Fatalf("uploaded content = %q, want %q", gotContent, content)
	}
	if _, err := os.Stat(filepath.Join(service.workspaceAppUploadTempDir("ws-1", "canvas"), session.UploadID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temp file stat error = %v, want not exist", err)
	}
}

func TestAppCenterServiceWorkspaceAppUploadReusesDuplicateContent(t *testing.T) {
	service := newAppUploadTestService(t)
	content := []byte("same bytes")
	now := time.Date(2026, time.June, 24, 12, 0, 0, 0, time.UTC)

	first := completeAppUploadForTest(t, service, now, "first.png", content)
	second := completeAppUploadForTest(t, service, now.Add(time.Minute), "second.png", content)

	if second.Path != first.Path {
		t.Fatalf("duplicate path = %q, want %q", second.Path, first.Path)
	}
	if second.Name != "second.png" {
		t.Fatalf("second name = %q, want second.png", second.Name)
	}
}

func TestAppCenterServiceWorkspaceAppUploadRejectsSizeMismatch(t *testing.T) {
	service := newAppUploadTestService(t)
	now := time.Date(2026, time.June, 24, 12, 0, 0, 0, time.UTC)
	session, err := service.PrepareWorkspaceAppUpload(context.Background(), "ws-1", "canvas", PrepareWorkspaceAppUploadInput{
		MimeType:  "text/plain",
		Name:      "note.txt",
		Now:       now,
		Purpose:   WorkspaceAppUploadPurposeAppAsset,
		SizeBytes: 10,
	})
	if err != nil {
		t.Fatalf("PrepareWorkspaceAppUpload() error = %v", err)
	}

	err = service.PutWorkspaceAppUploadContent(context.Background(), "ws-1", "canvas", session.UploadID, PutWorkspaceAppUploadContentInput{
		Body: bytes.NewReader([]byte("short")),
		Now:  now.Add(time.Minute),
	})
	if !errors.Is(err, ErrInvalidWorkspaceAppUpload) {
		t.Fatalf("PutWorkspaceAppUploadContent() error = %v, want ErrInvalidWorkspaceAppUpload", err)
	}
	if _, err := os.Stat(filepath.Join(service.workspaceAppUploadTempDir("ws-1", "canvas"), session.UploadID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temp file stat error = %v, want not exist", err)
	}
}

func TestAppCenterServiceWorkspaceAppUploadRejectsOversizedBodyWithoutReadingBeyondLimit(t *testing.T) {
	service := newAppUploadTestService(t)
	content := []byte("0123456789abcdef")
	body := bytes.NewReader(content)
	now := time.Date(2026, time.June, 24, 12, 0, 0, 0, time.UTC)
	session, err := service.PrepareWorkspaceAppUpload(context.Background(), "ws-1", "canvas", PrepareWorkspaceAppUploadInput{
		MimeType:  "text/plain",
		Name:      "note.txt",
		Now:       now,
		Purpose:   WorkspaceAppUploadPurposeAppAsset,
		SizeBytes: 5,
	})
	if err != nil {
		t.Fatalf("PrepareWorkspaceAppUpload() error = %v", err)
	}

	err = service.PutWorkspaceAppUploadContent(context.Background(), "ws-1", "canvas", session.UploadID, PutWorkspaceAppUploadContentInput{
		Body: body,
		Now:  now.Add(time.Minute),
	})
	if !errors.Is(err, ErrInvalidWorkspaceAppUpload) {
		t.Fatalf("PutWorkspaceAppUploadContent() error = %v, want ErrInvalidWorkspaceAppUpload", err)
	}
	if readBytes := len(content) - body.Len(); readBytes != 6 {
		t.Fatalf("read bytes = %d, want 6", readBytes)
	}
	if _, err := os.Stat(filepath.Join(service.workspaceAppUploadTempDir("ws-1", "canvas"), session.UploadID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temp file stat error = %v, want not exist", err)
	}
}

func TestAppCenterServiceWorkspaceAppUploadRequiresContentBeforeComplete(t *testing.T) {
	service := newAppUploadTestService(t)
	now := time.Date(2026, time.June, 24, 12, 0, 0, 0, time.UTC)
	session, err := service.PrepareWorkspaceAppUpload(context.Background(), "ws-1", "canvas", PrepareWorkspaceAppUploadInput{
		MimeType:  "text/plain",
		Name:      "note.txt",
		Now:       now,
		Purpose:   WorkspaceAppUploadPurposeAppAsset,
		SizeBytes: 4,
	})
	if err != nil {
		t.Fatalf("PrepareWorkspaceAppUpload() error = %v", err)
	}

	_, err = service.CompleteWorkspaceAppUpload(context.Background(), "ws-1", "canvas", session.UploadID, now.Add(time.Minute))
	if !errors.Is(err, ErrWorkspaceAppUploadNotReady) {
		t.Fatalf("CompleteWorkspaceAppUpload() error = %v, want ErrWorkspaceAppUploadNotReady", err)
	}
}

func TestAppCenterServiceWorkspaceAppUploadCancelCleansTempFileAndSession(t *testing.T) {
	service := newAppUploadTestService(t)
	content := []byte("cancel me")
	now := time.Date(2026, time.June, 24, 12, 0, 0, 0, time.UTC)
	session, err := service.PrepareWorkspaceAppUpload(context.Background(), "ws-1", "canvas", PrepareWorkspaceAppUploadInput{
		MimeType:  "text/plain",
		Name:      "note.txt",
		Now:       now,
		Purpose:   WorkspaceAppUploadPurposeAppAsset,
		SizeBytes: int64(len(content)),
	})
	if err != nil {
		t.Fatalf("PrepareWorkspaceAppUpload() error = %v", err)
	}
	if err := service.PutWorkspaceAppUploadContent(context.Background(), "ws-1", "canvas", session.UploadID, PutWorkspaceAppUploadContentInput{
		Body: bytes.NewReader(content),
		Now:  now.Add(time.Second),
	}); err != nil {
		t.Fatalf("PutWorkspaceAppUploadContent() error = %v", err)
	}

	if err := service.CancelWorkspaceAppUpload(context.Background(), "ws-1", "canvas", session.UploadID); err != nil {
		t.Fatalf("CancelWorkspaceAppUpload() error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(service.workspaceAppUploadTempDir("ws-1", "canvas"), session.UploadID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temp file stat error = %v, want not exist", err)
	}
	if _, err := service.CompleteWorkspaceAppUpload(context.Background(), "ws-1", "canvas", session.UploadID, now.Add(2*time.Second)); !errors.Is(err, ErrWorkspaceAppUploadNotFound) {
		t.Fatalf("CompleteWorkspaceAppUpload() after cancel error = %v, want ErrWorkspaceAppUploadNotFound", err)
	}
}

func TestAppCenterServiceWorkspaceAppUploadCancelKeepsCompletedFile(t *testing.T) {
	service := newAppUploadTestService(t)
	now := time.Date(2026, time.June, 24, 12, 0, 0, 0, time.UTC)
	file := completeAppUploadForTest(t, service, now, "image.png", []byte("keep me"))

	if err := service.CancelWorkspaceAppUpload(context.Background(), "ws-1", "canvas", ""); !errors.Is(err, ErrInvalidWorkspaceAppUpload) {
		t.Fatalf("CancelWorkspaceAppUpload() empty upload error = %v, want ErrInvalidWorkspaceAppUpload", err)
	}

	service.uploadMu.Lock()
	var uploadID string
	for id := range service.uploadSessions {
		uploadID = id
		break
	}
	service.uploadMu.Unlock()
	if uploadID == "" {
		t.Fatal("completed upload session was not retained for cancel test")
	}
	if err := service.CancelWorkspaceAppUpload(context.Background(), "ws-1", "canvas", uploadID); err != nil {
		t.Fatalf("CancelWorkspaceAppUpload() completed upload error = %v", err)
	}
	if _, err := os.Stat(file.Path); err != nil {
		t.Fatalf("completed file stat error = %v", err)
	}
}

func TestAppCenterServiceWorkspaceAppUploadExpiresAndCleansTempFile(t *testing.T) {
	service := newAppUploadTestService(t)
	content := []byte("expires")
	now := time.Date(2026, time.June, 24, 12, 0, 0, 0, time.UTC)
	session, err := service.PrepareWorkspaceAppUpload(context.Background(), "ws-1", "canvas", PrepareWorkspaceAppUploadInput{
		MimeType:  "text/plain",
		Name:      "note.txt",
		Now:       now,
		Purpose:   WorkspaceAppUploadPurposeAppAsset,
		SizeBytes: int64(len(content)),
	})
	if err != nil {
		t.Fatalf("PrepareWorkspaceAppUpload() error = %v", err)
	}
	if err := service.PutWorkspaceAppUploadContent(context.Background(), "ws-1", "canvas", session.UploadID, PutWorkspaceAppUploadContentInput{
		Body: bytes.NewReader(content),
		Now:  now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("PutWorkspaceAppUploadContent() error = %v", err)
	}

	_, err = service.CompleteWorkspaceAppUpload(context.Background(), "ws-1", "canvas", session.UploadID, session.ExpiresAt.Add(time.Second))
	if !errors.Is(err, ErrWorkspaceAppUploadExpired) {
		t.Fatalf("CompleteWorkspaceAppUpload() error = %v, want ErrWorkspaceAppUploadExpired", err)
	}
	if _, err := os.Stat(filepath.Join(service.workspaceAppUploadTempDir("ws-1", "canvas"), session.UploadID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temp file stat error = %v, want not exist", err)
	}

	err = service.PutWorkspaceAppUploadContent(context.Background(), "ws-1", "canvas", session.UploadID, PutWorkspaceAppUploadContentInput{
		Body: bytes.NewReader(content),
		Now:  session.ExpiresAt.Add(2 * time.Second),
	})
	if !errors.Is(err, ErrWorkspaceAppUploadNotFound) {
		t.Fatalf("PutWorkspaceAppUploadContent() after expiry error = %v, want ErrWorkspaceAppUploadNotFound", err)
	}
}

func TestAppCenterServiceWorkspaceAppUploadJanitorCleansExpiredTempFile(t *testing.T) {
	service := newAppUploadTestService(t)
	service.uploadJanitorInterval = 10 * time.Millisecond

	uploadID := "upload-for-cleanup"
	tempPath := filepath.Join(service.workspaceAppUploadTempDir("ws-1", "canvas"), uploadID)
	if err := os.MkdirAll(filepath.Dir(tempPath), 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	if err := os.WriteFile(tempPath, []byte("partial"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	service.uploadSessions = map[string]*workspaceAppUploadSession{
		uploadID: {
			AppID:       "canvas",
			ExpiresAt:   time.Now().UTC().Add(-time.Second),
			TempPath:    tempPath,
			UploadID:    uploadID,
			WorkspaceID: "ws-1",
		},
	}
	service.startWorkspaceAppUploadJanitor()

	waitForUploadCleanupForTest(t, func() bool {
		_, statErr := os.Stat(tempPath)
		service.uploadMu.Lock()
		_, exists := service.uploadSessions[uploadID]
		service.uploadMu.Unlock()
		return errors.Is(statErr, os.ErrNotExist) && !exists
	})
}

func TestAppCenterServiceWorkspaceAppUploadPrepareCleansStaleOrphanTempFiles(t *testing.T) {
	service := newAppUploadTestService(t)
	now := time.Date(2026, time.June, 24, 12, 0, 0, 0, time.UTC)
	tempDir := service.workspaceAppUploadTempDir("ws-1", "canvas")
	if err := os.MkdirAll(tempDir, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	stalePath := filepath.Join(tempDir, "stale-upload")
	freshPath := filepath.Join(tempDir, "fresh-upload")
	if err := os.WriteFile(stalePath, []byte("stale"), 0o644); err != nil {
		t.Fatalf("WriteFile(stale) error = %v", err)
	}
	if err := os.WriteFile(freshPath, []byte("fresh"), 0o644); err != nil {
		t.Fatalf("WriteFile(fresh) error = %v", err)
	}
	staleTime := now.Add(-workspaceAppUploadTTL - time.Minute)
	freshTime := now.Add(-time.Minute)
	if err := os.Chtimes(stalePath, staleTime, staleTime); err != nil {
		t.Fatalf("Chtimes(stale) error = %v", err)
	}
	if err := os.Chtimes(freshPath, freshTime, freshTime); err != nil {
		t.Fatalf("Chtimes(fresh) error = %v", err)
	}

	if _, err := service.PrepareWorkspaceAppUpload(context.Background(), "ws-1", "canvas", PrepareWorkspaceAppUploadInput{
		MimeType:  "text/plain",
		Name:      "note.txt",
		Now:       now,
		Purpose:   WorkspaceAppUploadPurposeAppAsset,
		SizeBytes: 0,
	}); err != nil {
		t.Fatalf("PrepareWorkspaceAppUpload() error = %v", err)
	}

	if _, err := os.Stat(stalePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale temp file stat error = %v, want not exist", err)
	}
	if _, err := os.Stat(freshPath); err != nil {
		t.Fatalf("fresh temp file stat error = %v", err)
	}
}

func completeAppUploadForTest(t *testing.T, service *AppCenterService, now time.Time, name string, content []byte) WorkspaceAppUploadedFile {
	t.Helper()

	session, err := service.PrepareWorkspaceAppUpload(context.Background(), "ws-1", "canvas", PrepareWorkspaceAppUploadInput{
		MimeType:  "image/png",
		Name:      name,
		Now:       now,
		Purpose:   WorkspaceAppUploadPurposeAppAsset,
		SizeBytes: int64(len(content)),
	})
	if err != nil {
		t.Fatalf("PrepareWorkspaceAppUpload() error = %v", err)
	}
	if err := service.PutWorkspaceAppUploadContent(context.Background(), "ws-1", "canvas", session.UploadID, PutWorkspaceAppUploadContentInput{
		Body: bytes.NewReader(content),
		Now:  now.Add(time.Second),
	}); err != nil {
		t.Fatalf("PutWorkspaceAppUploadContent() error = %v", err)
	}
	file, err := service.CompleteWorkspaceAppUpload(context.Background(), "ws-1", "canvas", session.UploadID, now.Add(2*time.Second))
	if err != nil {
		t.Fatalf("CompleteWorkspaceAppUpload() error = %v", err)
	}
	return file
}

func waitForUploadCleanupForTest(t *testing.T, ready func() bool) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for upload cleanup")
}

func newAppUploadTestService(t *testing.T) *AppCenterService {
	t.Helper()

	store := newAppStoreStub()
	manifest := workspacebiz.AppManifest{
		SchemaVersion: workspacebiz.AppManifestSchemaVersionV1,
		AppID:         "canvas",
		Version:       "1.0.0",
		Name:          "Canvas",
		Runtime: workspacebiz.AppManifestRuntime{
			Bootstrap:       "bootstrap.sh",
			HealthcheckPath: "/",
		},
	}
	packageDir := createWorkspaceAppPackageForTest(t, t.TempDir(), manifest)
	if err := store.PutAppPackage(context.Background(), workspacebiz.AppPackage{
		AppID:      manifest.AppID,
		Version:    manifest.Version,
		Manifest:   manifest,
		PackageDir: packageDir,
		Source:     workspacebiz.AppPackageSourceGenerated,
	}); err != nil {
		t.Fatalf("PutAppPackage() error = %v", err)
	}
	if err := store.PutWorkspaceAppInstallation(context.Background(), workspacebiz.AppInstallation{
		WorkspaceID: "ws-1",
		AppID:       manifest.AppID,
		Enabled:     true,
	}); err != nil {
		t.Fatalf("PutWorkspaceAppInstallation() error = %v", err)
	}

	service := &AppCenterService{
		Store:    store,
		StateDir: t.TempDir(),
	}
	t.Cleanup(service.StopWorkspaceAppUploadJanitor)
	return service
}
