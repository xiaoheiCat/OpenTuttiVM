//go:build windows

package vmcas

import (
	"fmt"
	"syscall"
	"unsafe"
)

var (
	casKernel32        = syscall.NewLazyDLL("kernel32.dll")
	casMoveFileExW     = casKernel32.NewProc("MoveFileExW")
	casMoveFileReplace = uintptr(0x1)
	casMoveFileThrough = uintptr(0x8)
)

func replaceCASFile(source, destination string) error {
	src, err := syscall.UTF16PtrFromString(source)
	if err != nil {
		return err
	}
	dst, err := syscall.UTF16PtrFromString(destination)
	if err != nil {
		return err
	}
	r1, _, callErr := casMoveFileExW.Call(
		uintptr(unsafe.Pointer(src)), uintptr(unsafe.Pointer(dst)),
		casMoveFileReplace|casMoveFileThrough,
	)
	if r1 == 0 {
		return fmt.Errorf("replace cas object: %w", callErr)
	}
	return nil
}
