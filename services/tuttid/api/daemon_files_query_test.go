package api

import (
	"context"
	"net/http"
	"testing"
	"time"

	workspacefiles "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/files"
	tuttigenerated "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/api/generated"
)

func TestDaemonAPIGeneratedRoutesSearchWorkspaceFilesRequiresQuery(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRoutes(
		mux,
		NewRoutes(DaemonAPI{
			FileService: stubFileService{
				searchFn: func(context.Context, string, workspacefiles.SearchInput) (workspacefiles.SearchResult, error) {
					t.Fatal("Search should not be called when query is missing")
					return workspacefiles.SearchResult{}, nil
				},
			},
		}),
	)

	recorder := performGeneratedRouteRequest(t, mux, http.MethodGet, "/v1/workspaces/ws-1/files/search", nil)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body: %s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
	}

	assertGeneratedRouteError(
		t,
		recorder,
		tuttigenerated.InvalidRequest,
		"malformed_request",
		"Query argument query is required, but not found",
	)
}

func TestDaemonAPIGeneratedRoutesSearchWorkspaceFilesRejectsInvalidLimit(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRoutes(
		mux,
		NewRoutes(DaemonAPI{
			FileService: stubFileService{
				searchFn: func(context.Context, string, workspacefiles.SearchInput) (workspacefiles.SearchResult, error) {
					t.Fatal("Search should not be called when limit is invalid")
					return workspacefiles.SearchResult{}, nil
				},
			},
		}),
	)

	recorder := performGeneratedRouteRequest(
		t,
		mux,
		http.MethodGet,
		"/v1/workspaces/ws-1/files/search?query=main&limit=nope",
		nil,
	)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body: %s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
	}

	assertGeneratedRouteError(
		t,
		recorder,
		tuttigenerated.InvalidRequest,
		"malformed_request",
		"Invalid format for parameter limit: error binding string parameter: strconv.ParseInt: parsing \"nope\": invalid syntax",
	)
}

func TestDaemonAPIGeneratedRoutesSearchWorkspaceFilesForwardsIncludeHidden(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRoutes(
		mux,
		NewRoutes(DaemonAPI{
			FileService: stubFileService{
				searchFn: func(_ context.Context, workspaceID string, input workspacefiles.SearchInput) (workspacefiles.SearchResult, error) {
					if workspaceID != "ws-1" {
						t.Fatalf("workspaceID = %q, want ws-1", workspaceID)
					}
					if input.Query != "main" {
						t.Fatalf("query = %q, want main", input.Query)
					}
					if !input.IncludeHidden {
						t.Fatal("includeHidden = false, want true")
					}
					return workspacefiles.SearchResult{
						Entries: []workspacefiles.SearchEntry{},
					}, nil
				},
			},
		}),
	)

	recorder := performGeneratedRouteRequest(
		t,
		mux,
		http.MethodGet,
		"/v1/workspaces/ws-1/files/search?query=main&includeHidden=true",
		nil,
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
}

func TestDaemonAPIGeneratedRoutesListWorkspaceFileDirectory(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRoutes(
		mux,
		NewRoutes(DaemonAPI{
			FileService: stubFileService{
				listDirectoryFn: func(_ context.Context, workspaceID string, input workspacefiles.DirectoryListInput) (workspacefiles.DirectoryListing, error) {
					if workspaceID != "ws-1" {
						t.Fatalf("workspaceID = %q, want ws-1", workspaceID)
					}
					if input.Path != "/workspace/src" {
						t.Fatalf("path = %q, want /workspace/src", input.Path)
					}
					if input.IncludeHidden {
						t.Fatal("includeHidden = true, want false")
					}
					size := int64(12)
					return workspacefiles.DirectoryListing{
						WorkspaceID:   workspaceID,
						Root:          "/workspace",
						DirectoryPath: "/workspace/src",
						Entries: []workspacefiles.FileEntry{
							{
								Path:      "/workspace/src/main.go",
								Name:      "main.go",
								Kind:      workspacefiles.EntryKindFile,
								SizeBytes: &size,
							},
						},
					}, nil
				},
			},
		}),
	)

	recorder := performGeneratedRouteRequest(t, mux, http.MethodGet, "/v1/workspaces/ws-1/files/directory?path=/workspace/src", nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}

	var response tuttigenerated.WorkspaceFileDirectoryResponse
	decodeGeneratedRouteResponse(t, recorder, &response)
	if response.WorkspaceId != "ws-1" || response.DirectoryPath != "/workspace/src" {
		t.Fatalf("response = %#v", response)
	}
	if len(response.Entries) != 1 || response.Entries[0].Path != "/workspace/src/main.go" {
		t.Fatalf("entries = %#v", response.Entries)
	}
}

func TestDaemonAPIGeneratedRoutesListWorkspaceRecentFiles(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRoutes(
		mux,
		NewRoutes(DaemonAPI{
			FileService: stubFileService{
				listRecentFn: func(_ context.Context, workspaceID string, input workspacefiles.RecentListInput) (workspacefiles.DirectoryListing, error) {
					if workspaceID != "ws-1" {
						t.Fatalf("workspaceID = %q, want ws-1", workspaceID)
					}
					if input.Limit != 5 {
						t.Fatalf("limit = %d, want 5", input.Limit)
					}
					lastUsed := int64(1700)
					return workspacefiles.DirectoryListing{
						WorkspaceID:   workspaceID,
						Root:          "/workspace",
						DirectoryPath: "/workspace",
						Entries: []workspacefiles.FileEntry{
							{
								Path:         "/workspace/src/main.go",
								Name:         "main.go",
								Kind:         workspacefiles.EntryKindFile,
								LastOpenedMs: &lastUsed,
							},
						},
					}, nil
				},
			},
		}),
	)

	recorder := performGeneratedRouteRequest(t, mux, http.MethodGet, "/v1/workspaces/ws-1/files/recent?limit=5", nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}

	var response tuttigenerated.WorkspaceFileDirectoryResponse
	decodeGeneratedRouteResponse(t, recorder, &response)
	if response.WorkspaceId != "ws-1" {
		t.Fatalf("response = %#v", response)
	}
	if len(response.Entries) != 1 || response.Entries[0].Path != "/workspace/src/main.go" {
		t.Fatalf("entries = %#v", response.Entries)
	}
}

func TestDaemonAPIGeneratedRoutesListWorkspaceFileDirectoryForwardsIncludeHidden(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRoutes(
		mux,
		NewRoutes(DaemonAPI{
			FileService: stubFileService{
				listDirectoryFn: func(_ context.Context, workspaceID string, input workspacefiles.DirectoryListInput) (workspacefiles.DirectoryListing, error) {
					if workspaceID != "ws-1" {
						t.Fatalf("workspaceID = %q, want ws-1", workspaceID)
					}
					if input.Path != "" {
						t.Fatalf("path = %q, want empty root path", input.Path)
					}
					if !input.IncludeHidden {
						t.Fatal("includeHidden = false, want true")
					}
					return workspacefiles.DirectoryListing{
						WorkspaceID:   workspaceID,
						Root:          "/workspace",
						DirectoryPath: "/workspace",
						Entries:       []workspacefiles.FileEntry{},
					}, nil
				},
			},
		}),
	)

	recorder := performGeneratedRouteRequest(t, mux, http.MethodGet, "/v1/workspaces/ws-1/files/directory?includeHidden=true", nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
}

func TestDaemonAPIGeneratedRoutesGetWorkspaceFileTreeSnapshot(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRoutes(
		mux,
		NewRoutes(DaemonAPI{
			FileService: stubFileService{
				getDirectoryTreeFn: func(_ context.Context, workspaceID string, input workspacefiles.DirectoryTreeSnapshotInput) (workspacefiles.DirectoryTreeSnapshot, error) {
					if workspaceID != "ws-1" {
						t.Fatalf("workspaceID = %q, want ws-1", workspaceID)
					}
					if input.Path != "/workspace/src" {
						t.Fatalf("path = %q, want /workspace/src", input.Path)
					}
					if !input.IncludeHidden {
						t.Fatal("includeHidden = false, want true")
					}
					if input.PrefetchDepth != 3 {
						t.Fatalf("prefetchDepth = %d, want 3", input.PrefetchDepth)
					}
					if input.PrefetchBudget != 250*time.Millisecond {
						t.Fatalf("prefetchBudget = %s, want 250ms", input.PrefetchBudget)
					}
					return workspacefiles.DirectoryTreeSnapshot{
						WorkspaceID:      workspaceID,
						Root:             "/workspace",
						PrefetchBudgetMs: 250,
						PrefetchDepth:    3,
						BudgetExceeded:   true,
						Directory: workspacefiles.DirectoryTreeDirectory{
							DirectoryPath: "/workspace/src",
							PrefetchState: workspacefiles.DirectoryTreePrefetchStatePartial,
							Entries: []workspacefiles.DirectoryTreeEntry{
								{
									Path:           "/workspace/src/app",
									Name:           "app",
									Kind:           workspacefiles.EntryKindDirectory,
									HasChildren:    true,
									PrefetchState:  workspacefiles.DirectoryTreePrefetchStateNotLoaded,
									PrefetchReason: workspacefiles.DirectoryTreePrefetchReasonBudgetExhausted,
								},
							},
						},
					}, nil
				},
			},
		}),
	)

	recorder := performGeneratedRouteRequest(
		t,
		mux,
		http.MethodGet,
		"/v1/workspaces/ws-1/files/tree-snapshot?path=/workspace/src&includeHidden=true&prefetchDepth=3&prefetchBudgetMs=250",
		nil,
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}

	var response tuttigenerated.WorkspaceFileTreeSnapshotResponse
	decodeGeneratedRouteResponse(t, recorder, &response)
	if response.Directory.DirectoryPath != "/workspace/src" {
		t.Fatalf("directoryPath = %q, want /workspace/src", response.Directory.DirectoryPath)
	}
	if !response.BudgetExceeded || response.PrefetchDepth != 3 || response.PrefetchBudgetMs != 250 {
		t.Fatalf("response = %#v", response)
	}
	if len(response.Directory.Entries) != 1 || response.Directory.Entries[0].Path != "/workspace/src/app" {
		t.Fatalf("entries = %#v", response.Directory.Entries)
	}
}
