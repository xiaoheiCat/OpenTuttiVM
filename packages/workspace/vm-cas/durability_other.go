//go:build !windows

package vmcas

import "os"

func syncCASFile(f *os.File) error { return f.Sync() }

func syncCASDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
