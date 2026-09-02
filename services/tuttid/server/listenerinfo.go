package server

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"

	tuttitypes "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/types"
)

type listenerInfo struct {
	Version int              `json:"version"`
	Addr    string           `json:"addr"`
	Auth    listenerInfoAuth `json:"auth"`
}

type listenerInfoAuth struct {
	Scheme string `json:"scheme"`
	Token  string `json:"token"`
}

func WriteListenerInfo(listener net.Listener, spec ListenerSpec) error {
	if listener == nil {
		return fmt.Errorf("listener is not configured")
	}
	if spec.AccessToken == "" {
		return fmt.Errorf("listener access token is required")
	}

	addr := listener.Addr()
	if addr == nil {
		return fmt.Errorf("listener address is not available")
	}

	targetPath := tuttitypes.TuttidListenerInfoPath()
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
		return fmt.Errorf("create listener info directory: %w", err)
	}

	body, err := json.Marshal(listenerInfo{
		Version: 1,
		Addr:    addr.String(),
		Auth: listenerInfoAuth{
			Scheme: "bearer",
			Token:  spec.AccessToken,
		},
	})
	if err != nil {
		return fmt.Errorf("marshal listener info: %w", err)
	}

	tempPath := targetPath + ".tmp"
	if err := os.WriteFile(tempPath, body, 0o600); err != nil {
		return fmt.Errorf("write listener info: %w", err)
	}

	if err := os.Rename(tempPath, targetPath); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("publish listener info: %w", err)
	}

	return nil
}
