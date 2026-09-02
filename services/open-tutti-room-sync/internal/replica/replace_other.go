//go:build !windows

package replica

import "os"

// replaceFile is POSIX rename(2): it atomically replaces an existing
// destination, which is exactly the atomic-swap contract. The Windows
// variant (replace_windows.go) uses MoveFileEx(REPLACE_EXISTING)
// because os.Rename there refuses to replace.
func replaceFile(source, destination string) error {
	return os.Rename(source, destination)
}

// clearReadOnly is Windows-only (POSIX rename/unlink ignore file mode).
func clearReadOnly(path string) {}
