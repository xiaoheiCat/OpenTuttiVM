//go:build windows

package roomfs

import (
	"fmt"
	"os"
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
		return fmt.Errorf("capability path is not a regular file or reparse point")
	}
	return nil
}

func atomicRenameCapability(from, to string) error { return os.Rename(from, to) }
func syncCapabilityDir(string) error               { return nil }
