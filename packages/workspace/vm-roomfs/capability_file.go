package roomfs

import (
	"fmt"
	"os"
	"path/filepath"
)

// WriteCapabilityFile publishes a capability without following a pre-created
// link or replacing an unsafe existing file.
func WriteCapabilityFile(path, capability string) error {
	if path == "" {
		return fmt.Errorf("capability path is empty")
	}
	if err := validateCapabilityPath(path); err != nil {
		return err
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".roomfs-cap-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.WriteString(capability); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := atomicRenameCapability(tmpName, path); err != nil {
		return err
	}
	if err := validateCapabilityPath(path); err != nil {
		return fmt.Errorf("validate published capability: %w", err)
	}
	return syncCapabilityDir(dir)
}

// ReadCapabilityFile authenticates only from a safe regular file.
func ReadCapabilityFile(path string) (string, error) {
	if path == "" {
		return "", fmt.Errorf("capability path is empty")
	}
	data, err := readCapabilityFile(path)
	if err != nil {
		return "", err
	}
	if len(data) == 0 {
		return "", fmt.Errorf("capability file is empty")
	}
	return string(data), nil
}
