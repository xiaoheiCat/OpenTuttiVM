//go:build !windows

package replica

import "os"

func syncFile(f *os.File) error { return f.Sync() }

func syncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
