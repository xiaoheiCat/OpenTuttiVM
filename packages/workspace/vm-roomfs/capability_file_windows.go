//go:build windows

package roomfs

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

var (
	roomfsKernel32        = syscall.NewLazyDLL("kernel32.dll")
	roomfsMoveFileExW     = roomfsKernel32.NewProc("MoveFileExW")
	roomfsMoveFileReplace = uintptr(0x1) // MOVEFILE_REPLACE_EXISTING
	roomfsMoveFileThrough = uintptr(0x8) // MOVEFILE_WRITE_THROUGH
)

const (
	fileAttributeReparse = 0x400
	fileAttributeDir     = 0x10
)

func validateCapabilityPath(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("capability path is not a regular file")
	}
	attrs, err := windowsFileAttributes(path)
	if err != nil {
		return err
	}
	if attrs&fileAttributeReparse != 0 || attrs&fileAttributeDir != 0 {
		return fmt.Errorf("capability path is not a regular file")
	}
	return nil
}

func windowsFileAttributes(path string) (uint32, error) {
	p, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	attrs, err := syscall.GetFileAttributes(p)
	if err != nil {
		return 0, err
	}
	return attrs, nil
}

func atomicRenameCapability(from, to string) error {
	if err := validateCapabilityPath(from); err != nil {
		return fmt.Errorf("validate capability source: %w", err)
	}
	if err := validateCapabilityPath(to); err != nil {
		return fmt.Errorf("validate capability target: %w", err)
	}
	src, err := syscall.UTF16PtrFromString(from)
	if err != nil {
		return err
	}
	dst, err := syscall.UTF16PtrFromString(to)
	if err != nil {
		return err
	}
	if r1, _, callErr := roomfsMoveFileExW.Call(
		uintptr(unsafe.Pointer(src)), uintptr(unsafe.Pointer(dst)),
		roomfsMoveFileReplace|roomfsMoveFileThrough,
	); r1 == 0 {
		return fmt.Errorf("publish capability: %w", callErr)
	}
	if err := validateCapabilityPath(to); err != nil {
		return fmt.Errorf("validate published capability: %w", err)
	}
	return nil
}

func syncCapabilityDir(string) error { return nil }
