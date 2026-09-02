//go:build windows

package vmcas

import "os"

// Windows' replace boundary is supplied by the filesystem rename semantics;
// Sync on the temporary file provides the durable data-before-publication
// boundary without attempting POSIX directory fsync.
func syncCASFile(f *os.File) error { return f.Sync() }

func syncCASDir(string) error { return nil }
