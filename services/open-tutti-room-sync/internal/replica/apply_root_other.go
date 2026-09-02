//go:build !windows

package replica

import (
	"fmt"
	"os"
)

func isApplyReparsePoint(string) bool { return false }

func prepareApplyRoot(path string) (os.FileInfo, error) {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(path, 0o755); err != nil {
			return nil, err
		}
		info, err = os.Lstat(path)
	}
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return nil, fmt.Errorf("apply root must be a real directory: %s", path)
	}
	return info, nil
}

func verifyApplyRoot(path string, want os.FileInfo) error {
	got, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("apply root changed: %w", err)
	}
	if got.Mode()&os.ModeSymlink != 0 || !got.IsDir() || !os.SameFile(want, got) {
		return fmt.Errorf("apply root changed during apply")
	}
	return nil
}
