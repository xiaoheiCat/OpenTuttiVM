//go:build windows

package replica

import (
	"fmt"
	"syscall"
	"unsafe"
)

// replaceFile atomically swaps source over destination on Windows via
// MoveFileEx(REPLACE_EXISTING): os.Rename refuses to replace an
// existing destination on Windows, and a remove-then-rename fallback
// deletes the ONLY installed copy before the second rename — a crash
// or second failure leaves the workspace with neither version, despite
// the caller's atomic-write contract. Declared through the raw proc
// (stdlib syscall only) so this module needs no new dependency.
var (
	kernel32        = syscall.NewLazyDLL("kernel32.dll")
	procMoveFileExW = kernel32.NewProc("MoveFileExW")
	movefileReplace = uintptr(0x1) // MOVEFILE_REPLACE_EXISTING
	movefileThrough = uintptr(0x8) // MOVEFILE_WRITE_THROUGH
)

func replaceFile(source, destination string) error {
	src, err := syscall.UTF16PtrFromString(source)
	if err != nil {
		return fmt.Errorf("replace %s: %w", destination, err)
	}
	dst, err := syscall.UTF16PtrFromString(destination)
	if err != nil {
		return fmt.Errorf("replace %s: %w", destination, err)
	}
	r1, _, e1 := procMoveFileExW.Call(
		uintptr(unsafe.Pointer(src)),
		uintptr(unsafe.Pointer(dst)),
		movefileReplace|movefileThrough,
	)
	if r1 == 0 {
		return fmt.Errorf("replace %s: %w", destination, e1)
	}
	return nil
}
