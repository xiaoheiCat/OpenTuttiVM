package workspace

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	workspacefiles "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/files"
)

type uploadSourcePlan struct {
	files                  []uploadFilePlan
	rootSourceInfo         os.FileInfo
	rootSourcePath         string
	rootTargetLogicalPath  workspacefiles.LogicalPath
	rootTargetPhysicalPath string
}

type uploadFilePlan struct {
	sourceInfo         os.FileInfo
	sourcePath         string
	targetLogicalPath  workspacefiles.LogicalPath
	targetPhysicalPath string
}

func (LocalFilesAdapter) UploadFiles(
	ctx context.Context,
	root workspacefiles.WorkspaceRoot,
	targetDirectoryPath workspacefiles.LogicalPath,
	sourcePaths []string,
	overwrite bool,
) ([]workspacefiles.FileEntry, error) {
	if err := validateUploadTargetDirectory(root, targetDirectoryPath); err != nil {
		return nil, err
	}

	plans, err := collectUploadPlans(ctx, root, targetDirectoryPath, sourcePaths)
	if err != nil {
		return nil, err
	}

	entries := make([]workspacefiles.FileEntry, 0, len(plans))
	for _, plan := range plans {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		if plan.rootSourceInfo.IsDir() {
			if err := ensureUploadDirectoryTarget(plan); err != nil {
				return nil, err
			}
		}

		for _, filePlan := range plan.files {
			if err := copyUploadedFile(filePlan, overwrite); err != nil {
				return nil, err
			}
		}

		entry, err := localFileEntry(root, plan.rootTargetLogicalPath)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

func (LocalFilesAdapter) PreflightUploadFiles(
	ctx context.Context,
	root workspacefiles.WorkspaceRoot,
	targetDirectoryPath workspacefiles.LogicalPath,
	sourcePaths []string,
) ([]workspacefiles.UploadConflict, error) {
	if err := validateUploadTargetDirectory(root, targetDirectoryPath); err != nil {
		return nil, err
	}

	plans, err := collectUploadPlans(ctx, root, targetDirectoryPath, sourcePaths)
	if err != nil {
		return nil, err
	}

	conflicts := make([]workspacefiles.UploadConflict, 0, len(plans))
	for _, plan := range plans {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		if plan.rootSourceInfo.IsDir() {
			existingInfo, err := os.Stat(plan.rootTargetPhysicalPath)
			if err == nil && !existingInfo.IsDir() {
				conflicts = append(conflicts, workspacefiles.UploadConflict{
					DestinationKind: entryKind(existingInfo.Mode()),
					DestinationPath: plan.rootTargetLogicalPath,
					Kind:            workspacefiles.UploadConflictKindTypeMismatch,
					Name:            filepath.Base(plan.rootSourcePath),
					SourcePath:      plan.rootSourcePath,
				})
				continue
			}
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return nil, fileError(err, plan.rootTargetLogicalPath)
			}
		}

		for _, filePlan := range plan.files {
			existingInfo, err := os.Stat(filePlan.targetPhysicalPath)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return nil, fileError(err, filePlan.targetLogicalPath)
			}
			if existingInfo.IsDir() {
				conflicts = append(conflicts, workspacefiles.UploadConflict{
					DestinationKind: entryKind(existingInfo.Mode()),
					DestinationPath: filePlan.targetLogicalPath,
					Kind:            workspacefiles.UploadConflictKindTypeMismatch,
					Name:            filepath.Base(filePlan.sourcePath),
					SourcePath:      filePlan.sourcePath,
				})
				continue
			}

			conflicts = append(conflicts, workspacefiles.UploadConflict{
				DestinationKind: entryKind(existingInfo.Mode()),
				DestinationPath: filePlan.targetLogicalPath,
				Kind:            workspacefiles.UploadConflictKindReplaceable,
				Name:            filepath.Base(filePlan.sourcePath),
				SourcePath:      filePlan.sourcePath,
			})
		}
	}

	return conflicts, nil
}

func validateUploadTargetDirectory(
	root workspacefiles.WorkspaceRoot,
	targetDirectoryPath workspacefiles.LogicalPath,
) error {
	targetPhysicalPath, err := existingPhysicalPath(root, targetDirectoryPath)
	if err != nil {
		return err
	}

	targetInfo, err := os.Stat(targetPhysicalPath)
	if err != nil {
		return fileError(err, targetDirectoryPath)
	}
	if !targetInfo.IsDir() {
		return fmt.Errorf("%w: %s", workspacefiles.ErrInvalidEntryKind, targetDirectoryPath)
	}

	return nil
}

func collectUploadPlans(
	ctx context.Context,
	root workspacefiles.WorkspaceRoot,
	targetDirectoryPath workspacefiles.LogicalPath,
	sourcePaths []string,
) ([]uploadSourcePlan, error) {
	plans := make([]uploadSourcePlan, 0, len(sourcePaths))
	for _, sourcePath := range sourcePaths {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		plan, err := planUploadSource(root, targetDirectoryPath, sourcePath)
		if err != nil {
			return nil, err
		}
		plans = append(plans, plan)
	}
	return plans, nil
}

func planUploadSource(
	root workspacefiles.WorkspaceRoot,
	targetDirectoryPath workspacefiles.LogicalPath,
	sourcePath string,
) (uploadSourcePlan, error) {
	sourcePath = strings.TrimSpace(sourcePath)
	if sourcePath == "" {
		return uploadSourcePlan{}, fmt.Errorf("%w: source path is empty", workspacefiles.ErrInvalidUploadSource)
	}

	sourceInfo, err := os.Stat(sourcePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return uploadSourcePlan{}, fmt.Errorf("%w: %s", workspacefiles.ErrInvalidUploadSource, sourcePath)
		}
		return uploadSourcePlan{}, err
	}

	rootTargetLogicalPath, err := workspacefiles.NormalizeLogicalPathWithinRoot(
		targetDirectoryPath.String()+"/"+filepath.Base(sourcePath),
		root.LogicalRoot,
	)
	if err != nil {
		return uploadSourcePlan{}, err
	}
	rootTargetPhysicalPath, err := creatablePhysicalPath(root, rootTargetLogicalPath)
	if err != nil {
		return uploadSourcePlan{}, err
	}

	plan := uploadSourcePlan{
		files:                  []uploadFilePlan{},
		rootSourceInfo:         sourceInfo,
		rootSourcePath:         sourcePath,
		rootTargetLogicalPath:  rootTargetLogicalPath,
		rootTargetPhysicalPath: rootTargetPhysicalPath,
	}

	if sourceInfo.Mode().IsRegular() {
		plan.files = append(plan.files, uploadFilePlan{
			sourceInfo:         sourceInfo,
			sourcePath:         sourcePath,
			targetLogicalPath:  rootTargetLogicalPath,
			targetPhysicalPath: rootTargetPhysicalPath,
		})
		return plan, nil
	}
	if !sourceInfo.IsDir() {
		return uploadSourcePlan{}, fmt.Errorf("%w: %s", workspacefiles.ErrInvalidUploadSource, sourcePath)
	}

	walkErr := filepath.WalkDir(sourcePath, func(physicalPath string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if physicalPath == sourcePath {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return nil
		}
		if entry.IsDir() {
			return nil
		}

		fileInfo, err := entry.Info()
		if err != nil {
			return err
		}
		if !fileInfo.Mode().IsRegular() {
			return fmt.Errorf("%w: %s", workspacefiles.ErrInvalidUploadSource, physicalPath)
		}

		relativePath, err := filepath.Rel(sourcePath, physicalPath)
		if err != nil {
			return err
		}

		targetLogicalPath, err := workspacefiles.NormalizeLogicalPathWithinRoot(
			rootTargetLogicalPath.String()+"/"+filepath.ToSlash(relativePath),
			root.LogicalRoot,
		)
		if err != nil {
			return err
		}

		plan.files = append(plan.files, uploadFilePlan{
			sourceInfo:         fileInfo,
			sourcePath:         physicalPath,
			targetLogicalPath:  targetLogicalPath,
			targetPhysicalPath: filepath.Join(rootTargetPhysicalPath, filepath.FromSlash(relativePath)),
		})
		return nil
	})
	if walkErr != nil {
		return uploadSourcePlan{}, walkErr
	}

	return plan, nil
}

func ensureUploadDirectoryTarget(plan uploadSourcePlan) error {
	existingInfo, err := os.Stat(plan.rootTargetPhysicalPath)
	if err == nil {
		if !existingInfo.IsDir() {
			return fmt.Errorf("%w: %s", workspacefiles.ErrInvalidEntryKind, plan.rootTargetLogicalPath)
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return fileError(err, plan.rootTargetLogicalPath)
	}
	if err := os.MkdirAll(plan.rootTargetPhysicalPath, plan.rootSourceInfo.Mode().Perm()); err != nil {
		return fileError(err, plan.rootTargetLogicalPath)
	}
	return nil
}

func copyUploadedFile(plan uploadFilePlan, overwrite bool) error {
	if err := os.MkdirAll(filepath.Dir(plan.targetPhysicalPath), 0o755); err != nil {
		return fileError(err, plan.targetLogicalPath)
	}

	if existingInfo, err := os.Stat(plan.targetPhysicalPath); err == nil {
		if existingInfo.IsDir() {
			return fmt.Errorf("%w: %s", workspacefiles.ErrInvalidEntryKind, plan.targetLogicalPath)
		}
		if !overwrite {
			return fmt.Errorf("%w: %s", workspacefiles.ErrEntryAlreadyExists, plan.targetLogicalPath)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fileError(err, plan.targetLogicalPath)
	}

	sourceFile, err := os.Open(plan.sourcePath)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	tempFile, err := os.CreateTemp(filepath.Dir(plan.targetPhysicalPath), ".tutti-upload-*")
	if err != nil {
		return err
	}
	tempPath := tempFile.Name()

	copyErr := func() error {
		if _, err := io.Copy(tempFile, sourceFile); err != nil {
			return err
		}
		if err := tempFile.Chmod(plan.sourceInfo.Mode().Perm()); err != nil {
			return err
		}
		return tempFile.Close()
	}()
	if copyErr != nil {
		_ = tempFile.Close()
		_ = os.Remove(tempPath)
		return copyErr
	}

	if overwrite {
		if err := os.Remove(plan.targetPhysicalPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			_ = os.Remove(tempPath)
			return err
		}
	}
	if err := os.Rename(tempPath, plan.targetPhysicalPath); err != nil {
		_ = os.Remove(tempPath)
		return fileError(err, plan.targetLogicalPath)
	}

	return nil
}
