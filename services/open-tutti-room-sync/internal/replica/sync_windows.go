//go:build windows

package replica

import (
	"os"
	"syscall"
)

func syncFile(f *os.File) error {
	if err := f.Sync(); err != nil {
		return err
	}
	return syscall.FlushFileBuffers(syscall.Handle(f.Fd()))
}

func syncDir(string) error { return nil }
