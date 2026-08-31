//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package roomfs

import (
	"fmt"
	"io"
	"os"
	"syscall"
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
	if info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("capability file permissions are too broad")
	}
	return nil
}

func readCapabilityFile(path string) ([]byte, error) {
	if path == "" {
		return nil, fmt.Errorf("capability path is empty")
	}
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	var st syscall.Stat_t
	if err := syscall.Fstat(fd, &st); err != nil {
		return nil, err
	}
	if st.Mode&syscall.S_IFMT != syscall.S_IFREG {
		return nil, fmt.Errorf("capability path is not a regular file")
	}
	if st.Mode&0o077 != 0 {
		return nil, fmt.Errorf("capability file permissions are too broad")
	}
	return io.ReadAll(f)
}

func atomicRenameCapability(from, to string) error { return os.Rename(from, to) }

func syncCapabilityDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
