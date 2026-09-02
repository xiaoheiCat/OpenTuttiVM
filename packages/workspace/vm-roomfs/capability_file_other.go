//go:build !windows && !(aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris)

package roomfs

import "os"

// Platforms without the Unix descriptor API retain the existing safe path
// checks; the supported desktop and daemon platforms use capability_file_unix.
func readCapabilityFile(path string) ([]byte, error) { return os.ReadFile(path) }
