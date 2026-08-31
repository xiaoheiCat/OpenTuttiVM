//go:build !windows

package gateway

import "os"

func replaceCAPairFile(source, destination string) error {
	return os.Rename(source, destination)
}
