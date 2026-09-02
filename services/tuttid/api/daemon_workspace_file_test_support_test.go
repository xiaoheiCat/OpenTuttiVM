package api

import (
	"context"

	workspacefiles "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/files"
)

type stubFileService struct {
	createDirectoryFn      func(context.Context, string, string) (workspacefiles.FileEntry, error)
	createFileFn           func(context.Context, string, string) (workspacefiles.FileEntry, error)
	readFileFn             func(context.Context, string, string, int64) (workspacefiles.FileContent, error)
	writeTextFileFn        func(context.Context, string, string, string) (workspacefiles.FileEntry, error)
	deleteEntryFn          func(context.Context, string, string, workspacefiles.EntryKind) error
	getDirectoryTreeFn     func(context.Context, string, workspacefiles.DirectoryTreeSnapshotInput) (workspacefiles.DirectoryTreeSnapshot, error)
	listDirectoryFn        func(context.Context, string, workspacefiles.DirectoryListInput) (workspacefiles.DirectoryListing, error)
	listRecentFn           func(context.Context, string, workspacefiles.RecentListInput) (workspacefiles.DirectoryListing, error)
	moveEntryFn            func(context.Context, string, string, string) (workspacefiles.FileEntry, error)
	renameEntryFn          func(context.Context, string, string, string) (workspacefiles.FileEntry, error)
	preflightUploadFilesFn func(context.Context, string, workspacefiles.PreflightUploadInput) (workspacefiles.PreflightUploadResult, error)
	resolveRootFn          func(context.Context, string) (workspacefiles.WorkspaceRoot, error)
	resolveRootForPathFn   func(context.Context, string, string) (workspacefiles.WorkspaceRoot, error)
	searchFn               func(context.Context, string, workspacefiles.SearchInput) (workspacefiles.SearchResult, error)
	uploadFilesFn          func(context.Context, string, workspacefiles.UploadInput) (workspacefiles.UploadResult, error)
}

func (s stubFileService) ResolveWorkspaceRoot(
	ctx context.Context,
	workspaceID string,
) (workspacefiles.WorkspaceRoot, error) {
	if s.resolveRootFn == nil {
		return workspacefiles.WorkspaceRoot{
			WorkspaceID:  workspaceID,
			LogicalRoot:  workspacefiles.DefaultLogicalRoot,
			PhysicalRoot: workspacefiles.DefaultLogicalRoot,
		}, nil
	}
	return s.resolveRootFn(ctx, workspaceID)
}

func (s stubFileService) ResolveWorkspaceRootForPath(
	ctx context.Context,
	workspaceID string,
	path string,
) (workspacefiles.WorkspaceRoot, error) {
	if s.resolveRootForPathFn == nil {
		return s.ResolveWorkspaceRoot(ctx, workspaceID)
	}
	return s.resolveRootForPathFn(ctx, workspaceID, path)
}

func (s stubFileService) ListDirectory(
	ctx context.Context,
	workspaceID string,
	input workspacefiles.DirectoryListInput,
) (workspacefiles.DirectoryListing, error) {
	if s.listDirectoryFn == nil {
		return workspacefiles.DirectoryListing{}, nil
	}
	return s.listDirectoryFn(ctx, workspaceID, input)
}

func (s stubFileService) ListRecent(
	ctx context.Context,
	workspaceID string,
	input workspacefiles.RecentListInput,
) (workspacefiles.DirectoryListing, error) {
	if s.listRecentFn == nil {
		return workspacefiles.DirectoryListing{}, nil
	}
	return s.listRecentFn(ctx, workspaceID, input)
}

func (s stubFileService) GetDirectoryTreeSnapshot(
	ctx context.Context,
	workspaceID string,
	input workspacefiles.DirectoryTreeSnapshotInput,
) (workspacefiles.DirectoryTreeSnapshot, error) {
	if s.getDirectoryTreeFn == nil {
		return workspacefiles.DirectoryTreeSnapshot{}, nil
	}
	return s.getDirectoryTreeFn(ctx, workspaceID, input)
}

func (s stubFileService) CreateFile(
	ctx context.Context,
	workspaceID string,
	path string,
) (workspacefiles.FileEntry, error) {
	if s.createFileFn == nil {
		return workspacefiles.FileEntry{}, nil
	}
	return s.createFileFn(ctx, workspaceID, path)
}

func (s stubFileService) ReadFile(
	ctx context.Context,
	workspaceID string,
	path string,
	maxBytes int64,
) (workspacefiles.FileContent, error) {
	if s.readFileFn == nil {
		return workspacefiles.FileContent{}, nil
	}
	return s.readFileFn(ctx, workspaceID, path, maxBytes)
}

func (s stubFileService) WriteTextFile(
	ctx context.Context,
	workspaceID string,
	path string,
	content string,
) (workspacefiles.FileEntry, error) {
	if s.writeTextFileFn == nil {
		return workspacefiles.FileEntry{}, nil
	}
	return s.writeTextFileFn(ctx, workspaceID, path, content)
}

func (s stubFileService) CreateDirectory(
	ctx context.Context,
	workspaceID string,
	path string,
) (workspacefiles.FileEntry, error) {
	if s.createDirectoryFn == nil {
		return workspacefiles.FileEntry{}, nil
	}
	return s.createDirectoryFn(ctx, workspaceID, path)
}

func (s stubFileService) DeleteEntry(
	ctx context.Context,
	workspaceID string,
	path string,
	kind workspacefiles.EntryKind,
) error {
	if s.deleteEntryFn == nil {
		return nil
	}
	return s.deleteEntryFn(ctx, workspaceID, path, kind)
}

func (s stubFileService) MoveEntry(
	ctx context.Context,
	workspaceID string,
	path string,
	targetDirectoryPath string,
) (workspacefiles.FileEntry, error) {
	if s.moveEntryFn == nil {
		return workspacefiles.FileEntry{}, nil
	}
	return s.moveEntryFn(ctx, workspaceID, path, targetDirectoryPath)
}

func (s stubFileService) RenameEntry(
	ctx context.Context,
	workspaceID string,
	path string,
	newName string,
) (workspacefiles.FileEntry, error) {
	if s.renameEntryFn != nil {
		return s.renameEntryFn(ctx, workspaceID, path, newName)
	}
	return workspacefiles.FileEntry{Path: workspacefiles.LogicalPath(path)}, nil
}

func (stubFileService) CopyEntry(
	_ context.Context,
	_ string,
	path string,
) (workspacefiles.FileEntry, error) {
	return workspacefiles.FileEntry{Path: workspacefiles.LogicalPath(path)}, nil
}

func (s stubFileService) PreflightUploadFiles(
	ctx context.Context,
	workspaceID string,
	input workspacefiles.PreflightUploadInput,
) (workspacefiles.PreflightUploadResult, error) {
	if s.preflightUploadFilesFn == nil {
		return workspacefiles.PreflightUploadResult{}, nil
	}
	return s.preflightUploadFilesFn(ctx, workspaceID, input)
}

func (s stubFileService) UploadFiles(
	ctx context.Context,
	workspaceID string,
	input workspacefiles.UploadInput,
) (workspacefiles.UploadResult, error) {
	if s.uploadFilesFn == nil {
		return workspacefiles.UploadResult{}, nil
	}
	return s.uploadFilesFn(ctx, workspaceID, input)
}

func (s stubFileService) Search(
	ctx context.Context,
	workspaceID string,
	input workspacefiles.SearchInput,
) (workspacefiles.SearchResult, error) {
	if s.searchFn == nil {
		return workspacefiles.SearchResult{}, nil
	}
	return s.searchFn(ctx, workspaceID, input)
}
