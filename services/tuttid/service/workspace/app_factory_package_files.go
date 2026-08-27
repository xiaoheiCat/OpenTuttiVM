package workspace

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	workspacebiz "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/biz/workspace"
)

func appFactoryDraftChanged(job workspacebiz.AppFactoryJob) (bool, error) {
	draftPackageDir := appFactoryDraftPackageDir(job)
	packageDir := strings.TrimSpace(job.PackageDir)
	if draftPackageDir == "" || packageDir == "" {
		return true, nil
	}
	if _, err := os.Stat(draftPackageDir); err != nil {
		if os.IsNotExist(err) {
			return true, nil
		}
		return false, fmt.Errorf("stat app factory draft package: %w", err)
	}
	if _, err := os.Stat(packageDir); err != nil {
		if os.IsNotExist(err) {
			return true, nil
		}
		return false, fmt.Errorf("stat published app package: %w", err)
	}
	draftFingerprint, err := appFactoryDirectoryFingerprint(draftPackageDir)
	if err != nil {
		return false, fmt.Errorf("fingerprint app factory draft: %w", err)
	}
	packageFingerprint, err := appFactoryDirectoryFingerprint(packageDir)
	if err != nil {
		return false, fmt.Errorf("fingerprint published app package: %w", err)
	}
	return draftFingerprint != packageFingerprint, nil
}

func appFactoryDraftPackageDir(job workspacebiz.AppFactoryJob) string {
	draftDir := strings.TrimSpace(job.DraftDir)
	if draftDir == "" {
		return ""
	}
	return filepath.Join(draftDir, filepath.FromSlash(appFactoryPackageRootRelativePath))
}

type appFactoryFingerprintEntry struct {
	relativePath string
	fileHash     [sha256.Size]byte
	executable   bool
}

func appFactoryDirectoryFingerprint(root string) (string, error) {
	root = filepath.Clean(root)
	var entries []appFactoryFingerprintEntry
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relativePath, err := filepath.Rel(root, path)
		if err != nil {
			return fmt.Errorf("resolve app package relative path: %w", err)
		}
		if relativePath == "." {
			return nil
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("read app package file info: %w", err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read app package file: %w", err)
		}
		entries = append(entries, appFactoryFingerprintEntry{
			relativePath: filepath.ToSlash(filepath.Clean(relativePath)),
			fileHash:     sha256.Sum256(data),
			executable:   info.Mode()&0o111 != 0,
		})
		return nil
	}); err != nil {
		return "", err
	}
	sort.Slice(entries, func(left int, right int) bool {
		return entries[left].relativePath < entries[right].relativePath
	})

	digest := sha256.New()
	for _, entry := range entries {
		_, _ = digest.Write([]byte(entry.relativePath))
		_, _ = digest.Write([]byte{0})
		if entry.executable {
			_, _ = digest.Write([]byte{1})
		} else {
			_, _ = digest.Write([]byte{0})
		}
		_, _ = digest.Write(entry.fileHash[:])
	}
	return fmt.Sprintf("%x", digest.Sum(nil)), nil
}

func copyDirectory(sourceDir string, targetDir string) error {
	sourceDir = filepath.Clean(sourceDir)
	targetDir = filepath.Clean(targetDir)
	return filepath.WalkDir(sourceDir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relativePath, err := filepath.Rel(sourceDir, path)
		if err != nil {
			return fmt.Errorf("resolve app package relative path: %w", err)
		}
		if relativePath == "." {
			return nil
		}
		targetPath := filepath.Join(targetDir, relativePath)
		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("read app package file info: %w", err)
		}
		if entry.IsDir() {
			return os.MkdirAll(targetPath, info.Mode())
		}
		if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
			return fmt.Errorf("create app package target parent: %w", err)
		}
		return copyFile(path, targetPath, info.Mode())
	})
}

func copyFile(sourcePath string, targetPath string, mode os.FileMode) error {
	sourceFile, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("open app package source file: %w", err)
	}

	targetFile, err := os.OpenFile(targetPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return errors.Join(
			fmt.Errorf("open app package target file: %w", err),
			wrapCopyFileError("close app package source file", sourceFile.Close()),
		)
	}

	_, copyErr := io.Copy(targetFile, sourceFile)
	return errors.Join(
		wrapCopyFileError("copy app package file", copyErr),
		wrapCopyFileError("close app package target file", targetFile.Close()),
		wrapCopyFileError("close app package source file", sourceFile.Close()),
	)
}

func wrapCopyFileError(message string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", message, err)
}
