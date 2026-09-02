package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	workspacefiles "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/files"
	tuttigenerated "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/api/generated"
)

func TestDaemonAPIGeneratedRoutesCreateWorkspaceFileDirectoryMapsMissingParentTo404(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRoutes(
		mux,
		NewRoutes(DaemonAPI{
			FileService: stubFileService{
				createDirectoryFn: func(_ context.Context, workspaceID string, path string) (workspacefiles.FileEntry, error) {
					if workspaceID != "ws-1" {
						t.Fatalf("workspaceID = %q, want ws-1", workspaceID)
					}
					if path != "/workspace/missing/notes" {
						t.Fatalf("path = %q, want /workspace/missing/notes", path)
					}
					return workspacefiles.FileEntry{}, workspacefiles.ErrEntryNotFound
				},
			},
		}),
	)

	recorder := performGeneratedRouteRequest(
		t,
		mux,
		http.MethodPut,
		"/v1/workspaces/ws-1/files/directory",
		map[string]any{"path": "/workspace/missing/notes"},
	)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d; body: %s", recorder.Code, http.StatusNotFound, recorder.Body.String())
	}

	assertGeneratedRouteError(
		t,
		recorder,
		tuttigenerated.WorkspaceFileNotFound,
		"workspace_file_not_found",
		workspacefiles.ErrEntryNotFound.Error(),
	)
}

func TestDaemonAPIGeneratedRoutesCreateWorkspaceFileMapsMissingParentTo404(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRoutes(
		mux,
		NewRoutes(DaemonAPI{
			FileService: stubFileService{
				createFileFn: func(_ context.Context, workspaceID string, path string) (workspacefiles.FileEntry, error) {
					if workspaceID != "ws-1" {
						t.Fatalf("workspaceID = %q, want ws-1", workspaceID)
					}
					if path != "/workspace/missing/todo.md" {
						t.Fatalf("path = %q, want /workspace/missing/todo.md", path)
					}
					return workspacefiles.FileEntry{}, workspacefiles.ErrEntryNotFound
				},
			},
		}),
	)

	recorder := performGeneratedRouteRequest(
		t,
		mux,
		http.MethodPut,
		"/v1/workspaces/ws-1/files/file",
		map[string]any{"path": "/workspace/missing/todo.md"},
	)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d; body: %s", recorder.Code, http.StatusNotFound, recorder.Body.String())
	}

	assertGeneratedRouteError(
		t,
		recorder,
		tuttigenerated.WorkspaceFileNotFound,
		"workspace_file_not_found",
		workspacefiles.ErrEntryNotFound.Error(),
	)
}

func TestDaemonAPIGeneratedRoutesReadWorkspaceFilePreview(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRoutes(
		mux,
		NewRoutes(DaemonAPI{
			FileService: stubFileService{
				readFileFn: func(_ context.Context, workspaceID string, path string, maxBytes int64) (workspacefiles.FileContent, error) {
					if workspaceID != "ws-1" {
						t.Fatalf("workspaceID = %q, want ws-1", workspaceID)
					}
					if path != "/workspace/docs/todo.md" {
						t.Fatalf("path = %q, want /workspace/docs/todo.md", path)
					}
					if maxBytes != workspacefiles.DefaultReadFileMaxBytes {
						t.Fatalf("maxBytes = %d, want %d", maxBytes, workspacefiles.DefaultReadFileMaxBytes)
					}
					return workspacefiles.FileContent{
						Bytes:     []byte("hello"),
						Name:      "todo.md",
						Path:      "/workspace/docs/todo.md",
						SizeBytes: 5,
					}, nil
				},
			},
		}),
	)

	request, err := http.NewRequest(
		http.MethodGet,
		"/v1/workspaces/ws-1/files/file/preview?path=%2Fworkspace%2Fdocs%2Ftodo.md",
		nil,
	)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}

	var response tuttigenerated.WorkspaceFilePreviewResponse
	decodeGeneratedRouteResponse(t, recorder, &response)
	if response.Path != "/workspace/docs/todo.md" {
		t.Fatalf("path = %q", response.Path)
	}
	if response.BytesBase64 != "aGVsbG8=" {
		t.Fatalf("bytesBase64 = %q", response.BytesBase64)
	}
	if response.SizeBytes != 5 {
		t.Fatalf("sizeBytes = %d", response.SizeBytes)
	}
}

func TestDaemonAPIGeneratedRoutesReadWorkspaceFilePreviewUsesPathAwareRoot(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRoutes(
		mux,
		NewRoutes(DaemonAPI{
			FileService: stubFileService{
				readFileFn: func(_ context.Context, workspaceID string, path string, _ int64) (workspacefiles.FileContent, error) {
					if workspaceID != "ws-1" {
						t.Fatalf("workspaceID = %q, want ws-1", workspaceID)
					}
					if path != "/tmp/report.md" {
						t.Fatalf("path = %q, want /tmp/report.md", path)
					}
					return workspacefiles.FileContent{
						Bytes:     []byte("hello"),
						Name:      "report.md",
						Path:      "/tmp/report.md",
						SizeBytes: 5,
					}, nil
				},
				resolveRootForPathFn: func(_ context.Context, workspaceID string, path string) (workspacefiles.WorkspaceRoot, error) {
					if workspaceID != "ws-1" {
						t.Fatalf("workspaceID = %q, want ws-1", workspaceID)
					}
					if path != "/tmp/report.md" {
						t.Fatalf("root path = %q, want /tmp/report.md", path)
					}
					return workspacefiles.WorkspaceRoot{
						WorkspaceID:  workspaceID,
						LogicalRoot:  "/",
						PhysicalRoot: "/",
					}, nil
				},
			},
		}),
	)

	request, err := http.NewRequest(
		http.MethodGet,
		"/v1/workspaces/ws-1/files/file/preview?path=%2Ftmp%2Freport.md",
		nil,
	)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}

	var response tuttigenerated.WorkspaceFilePreviewResponse
	decodeGeneratedRouteResponse(t, recorder, &response)
	if response.Root != "/" {
		t.Fatalf("root = %q, want /", response.Root)
	}
	if response.Path != "/tmp/report.md" {
		t.Fatalf("path = %q, want /tmp/report.md", response.Path)
	}
}

func TestDaemonAPIGeneratedRoutesWriteWorkspaceFileText(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRoutes(
		mux,
		NewRoutes(DaemonAPI{
			FileService: stubFileService{
				writeTextFileFn: func(_ context.Context, workspaceID string, path string, content string) (workspacefiles.FileEntry, error) {
					if workspaceID != "ws-1" {
						t.Fatalf("workspaceID = %q, want ws-1", workspaceID)
					}
					if path != "/workspace/docs/todo.md" {
						t.Fatalf("path = %q, want /workspace/docs/todo.md", path)
					}
					if content != "updated" {
						t.Fatalf("content = %q, want updated", content)
					}
					size := int64(len(content))
					return workspacefiles.FileEntry{
						Path:      "/workspace/docs/todo.md",
						Name:      "todo.md",
						Kind:      workspacefiles.EntryKindFile,
						SizeBytes: &size,
					}, nil
				},
			},
		}),
	)

	recorder := performGeneratedRouteRequest(
		t,
		mux,
		http.MethodPut,
		"/v1/workspaces/ws-1/files/file/text",
		map[string]any{"content": "updated", "path": "/workspace/docs/todo.md"},
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}

	var response tuttigenerated.WorkspaceFileEntryResponse
	decodeGeneratedRouteResponse(t, recorder, &response)
	if response.Entry.Path != "/workspace/docs/todo.md" {
		t.Fatalf("entry path = %q", response.Entry.Path)
	}
	if response.Entry.SizeBytes == nil || *response.Entry.SizeBytes != int64(len("updated")) {
		t.Fatalf("entry size = %#v", response.Entry.SizeBytes)
	}
}

func TestDaemonAPIGeneratedRoutesRenameWorkspaceFileEntryUsesPathAwareRoot(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRoutes(
		mux,
		NewRoutes(DaemonAPI{
			FileService: stubFileService{
				renameEntryFn: func(_ context.Context, workspaceID string, path string, newName string) (workspacefiles.FileEntry, error) {
					if workspaceID != "ws-1" {
						t.Fatalf("workspaceID = %q, want ws-1", workspaceID)
					}
					if path != "/tmp/report.md" {
						t.Fatalf("path = %q, want /tmp/report.md", path)
					}
					if newName != "renamed.md" {
						t.Fatalf("newName = %q, want renamed.md", newName)
					}
					return workspacefiles.FileEntry{
						Path: "/tmp/renamed.md",
						Name: "renamed.md",
						Kind: workspacefiles.EntryKindFile,
					}, nil
				},
				resolveRootForPathFn: func(_ context.Context, workspaceID string, path string) (workspacefiles.WorkspaceRoot, error) {
					if workspaceID != "ws-1" {
						t.Fatalf("workspaceID = %q, want ws-1", workspaceID)
					}
					if path != "/tmp/renamed.md" {
						t.Fatalf("root path = %q, want /tmp/renamed.md", path)
					}
					return workspacefiles.WorkspaceRoot{
						WorkspaceID:  workspaceID,
						LogicalRoot:  "/",
						PhysicalRoot: "/",
					}, nil
				},
			},
		}),
	)

	recorder := performGeneratedRouteRequest(
		t,
		mux,
		http.MethodPost,
		"/v1/workspaces/ws-1/files/entry/rename",
		map[string]any{"newName": "renamed.md", "path": "/tmp/report.md"},
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}

	var response tuttigenerated.WorkspaceFileEntryResponse
	decodeGeneratedRouteResponse(t, recorder, &response)
	if response.Root != "/" {
		t.Fatalf("root = %q, want /", response.Root)
	}
	if response.Entry.Path != "/tmp/renamed.md" {
		t.Fatalf("entry path = %q, want /tmp/renamed.md", response.Entry.Path)
	}
}

func TestDaemonAPIGeneratedRoutesUploadWorkspaceFiles(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRoutes(
		mux,
		NewRoutes(DaemonAPI{
			FileService: stubFileService{
				uploadFilesFn: func(_ context.Context, workspaceID string, input workspacefiles.UploadInput) (workspacefiles.UploadResult, error) {
					if workspaceID != "ws-1" {
						t.Fatalf("workspaceID = %q, want ws-1", workspaceID)
					}
					if !input.Overwrite {
						t.Fatal("overwrite should be forwarded")
					}
					if input.TargetDirectoryPath != "/workspace/docs" {
						t.Fatalf("targetDirectoryPath = %q, want /workspace/docs", input.TargetDirectoryPath)
					}
					if len(input.SourcePaths) != 2 || input.SourcePaths[0] != "/tmp/a.txt" || input.SourcePaths[1] != "/tmp/b.txt" {
						t.Fatalf("sourcePaths = %#v", input.SourcePaths)
					}
					return workspacefiles.UploadResult{
						WorkspaceID:         workspaceID,
						Root:                "/workspace",
						TargetDirectoryPath: "/workspace/docs",
						Entries: []workspacefiles.FileEntry{
							{Path: "/workspace/docs/a.txt", Name: "a.txt", Kind: workspacefiles.EntryKindFile},
							{Path: "/workspace/docs/b.txt", Name: "b.txt", Kind: workspacefiles.EntryKindFile},
						},
					}, nil
				},
			},
		}),
	)

	recorder := performGeneratedRouteRequest(t, mux, http.MethodPost, "/v1/workspaces/ws-1/files/upload", map[string]any{
		"overwrite":           true,
		"sourcePaths":         []string{"/tmp/a.txt", "/tmp/b.txt"},
		"targetDirectoryPath": "/workspace/docs",
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}

	var response tuttigenerated.UploadWorkspaceFilesResponse
	decodeGeneratedRouteResponse(t, recorder, &response)
	if response.WorkspaceId != "ws-1" || response.TargetDirectoryPath != "/workspace/docs" || len(response.Entries) != 2 {
		t.Fatalf("response = %#v", response)
	}
}

func TestDaemonAPIGeneratedRoutesPreflightUploadWorkspaceFiles(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRoutes(
		mux,
		NewRoutes(DaemonAPI{
			FileService: stubFileService{
				preflightUploadFilesFn: func(_ context.Context, workspaceID string, input workspacefiles.PreflightUploadInput) (workspacefiles.PreflightUploadResult, error) {
					if workspaceID != "ws-1" {
						t.Fatalf("workspaceID = %q, want ws-1", workspaceID)
					}
					if input.TargetDirectoryPath != "/workspace/docs" {
						t.Fatalf("targetDirectoryPath = %q, want /workspace/docs", input.TargetDirectoryPath)
					}
					if len(input.SourcePaths) != 1 || input.SourcePaths[0] != "/tmp/report.md" {
						t.Fatalf("sourcePaths = %#v", input.SourcePaths)
					}
					return workspacefiles.PreflightUploadResult{
						WorkspaceID:         workspaceID,
						Root:                "/workspace",
						TargetDirectoryPath: "/workspace/docs",
						Conflicts: []workspacefiles.UploadConflict{
							{
								DestinationKind: workspacefiles.EntryKindFile,
								DestinationPath: "/workspace/docs/report.md",
								Name:            "report.md",
								SourcePath:      "/tmp/report.md",
							},
						},
					}, nil
				},
			},
		}),
	)

	recorder := performGeneratedRouteRequest(t, mux, http.MethodPost, "/v1/workspaces/ws-1/files/upload/preflight", map[string]any{
		"sourcePaths":         []string{"/tmp/report.md"},
		"targetDirectoryPath": "/workspace/docs",
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}

	var response tuttigenerated.PreflightUploadWorkspaceFilesResponse
	decodeGeneratedRouteResponse(t, recorder, &response)
	if len(response.Conflicts) != 1 {
		t.Fatalf("conflicts len = %d, want 1", len(response.Conflicts))
	}
	if response.Conflicts[0].DestinationPath != "/workspace/docs/report.md" {
		t.Fatalf("destinationPath = %q, want /workspace/docs/report.md", response.Conflicts[0].DestinationPath)
	}
}
