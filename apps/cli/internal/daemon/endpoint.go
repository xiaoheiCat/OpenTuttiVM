package daemon

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/xiaoheiCat/OpenTuttiVM/apps/cli/internal/defaults"
)

type Endpoint struct {
	Addr  string
	Token string
}

type endpointFile struct {
	Version int              `json:"version"`
	Addr    string           `json:"addr"`
	Auth    endpointFileAuth `json:"auth"`
}

type endpointFileAuth struct {
	Scheme string `json:"scheme"`
	Token  string `json:"token"`
}

func DiscoverEndpoint() (Endpoint, error) {
	return ReadEndpointFile(ListenerInfoPath())
}

func ReadEndpointFile(path string) (Endpoint, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Endpoint{}, fmt.Errorf("daemon endpoint is not available; start the Tutti desktop app")
		}
		return Endpoint{}, fmt.Errorf("read daemon endpoint: %w", err)
	}

	var payload endpointFile
	if err := json.Unmarshal(content, &payload); err != nil {
		return Endpoint{}, fmt.Errorf("daemon endpoint file is invalid JSON")
	}

	addr := strings.TrimSpace(payload.Addr)
	if addr == "" {
		return Endpoint{}, fmt.Errorf("daemon endpoint file does not contain addr")
	}
	if payload.Version != 0 && payload.Version != 1 {
		return Endpoint{}, fmt.Errorf("daemon endpoint file version is not supported")
	}

	scheme := strings.ToLower(strings.TrimSpace(payload.Auth.Scheme))
	token := strings.TrimSpace(payload.Auth.Token)
	if scheme != "bearer" || token == "" {
		return Endpoint{}, fmt.Errorf("daemon endpoint file does not contain bearer auth")
	}

	return Endpoint{Addr: addr, Token: token}, nil
}

func (endpoint Endpoint) BaseURL() (string, error) {
	addr := strings.TrimSpace(endpoint.Addr)
	if addr == "" {
		return "", fmt.Errorf("daemon address is empty")
	}
	if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
		parsed, err := url.Parse(addr)
		if err != nil || parsed.Host == "" {
			return "", fmt.Errorf("daemon address is invalid")
		}
		return strings.TrimRight(addr, "/"), nil
	}
	return "http://" + strings.TrimRight(addr, "/"), nil
}

func ListenerInfoPath() string {
	return defaults.ResolveDefaultsFromEnv().State.TuttidListenerInfoPath
}
