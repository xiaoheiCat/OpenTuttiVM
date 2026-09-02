//go:build !windows

package vmcas

import "os"

func replaceCASFile(source, destination string) error { return os.Rename(source, destination) }
