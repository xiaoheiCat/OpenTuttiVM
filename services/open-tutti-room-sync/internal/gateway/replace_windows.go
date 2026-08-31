//go:build windows

package gateway

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

var (
	caKernel32    = syscall.NewLazyDLL("kernel32.dll")
	caMoveFileExW = caKernel32.NewProc("MoveFileExW")
)

const (
	caMoveFileReplace = uintptr(0x1) // MOVEFILE_REPLACE_EXISTING
	caMoveFileThrough = uintptr(0x8) // MOVEFILE_WRITE_THROUGH
	caFileReparse     = 0x400
	caFileDirectory   = 0x10
)

func validateCAPairTarget(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("room CA pair target is not a regular file")
	}
	p, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return err
	}
	attrs, err := syscall.GetFileAttributes(p)
	if err != nil {
		return err
	}
	if attrs&caFileReparse != 0 || attrs&caFileDirectory != 0 {
		return fmt.Errorf("room CA pair target is not a regular file")
	}
	return nil
}

func replaceCAPairFile(source, destination string) error {
	if err := validateCAPairTarget(source); err != nil {
		return fmt.Errorf("validate room CA pair source: %w", err)
	}
	if err := validateCAPairTarget(destination); err != nil {
		return fmt.Errorf("validate room CA pair target: %w", err)
	}
	src, err := syscall.UTF16PtrFromString(source)
	if err != nil {
		return err
	}
	dst, err := syscall.UTF16PtrFromString(destination)
	if err != nil {
		return err
	}
	if r1, _, callErr := caMoveFileExW.Call(
		uintptr(unsafe.Pointer(src)), uintptr(unsafe.Pointer(dst)),
		caMoveFileReplace|caMoveFileThrough,
	); r1 == 0 {
		return fmt.Errorf("publish room CA pair: %w", callErr)
	}
	if err := validateCAPairTarget(destination); err != nil {
		return fmt.Errorf("validate published room CA pair: %w", err)
	}
	return nil
}
