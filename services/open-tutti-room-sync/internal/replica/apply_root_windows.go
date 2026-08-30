//go:build windows

package replica

import (
	"fmt"
	"os"
	"syscall"
)

func isApplyReparsePoint(path string) bool {
	p, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return true
	}
	attrs, err := syscall.GetFileAttributes(p)
	return err != nil || attrs&syscall.FILE_ATTRIBUTE_REPARSE_POINT != 0
}

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
	if isApplyReparsePoint(path) || !info.IsDir() {
		return nil, fmt.Errorf("apply root must be a real directory: %s", path)
	}
	return info, nil
}

func verifyApplyRoot(path string, want os.FileInfo) error {
	got, err := os.Lstat(path)
	if err != nil || isApplyReparsePoint(path) || !got.IsDir() || !os.SameFile(want, got) {
		return fmt.Errorf("apply root changed during apply")
	}
	return nil
}
