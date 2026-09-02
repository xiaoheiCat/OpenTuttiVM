package desktopupdateadmission

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/xiaoheiCat/OpenTuttiVM/packages/agent/daemon/httpx"
	admissiondaemon "github.com/xiaoheiCat/OpenTuttiVM/packages/desktop/update-admission/daemon"
	tuttitypes "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/types"
)

const (
	managedEnvironment        = "TUTTI_DESKTOP_UPDATE_ADMISSION_MANAGED"
	packagedEnvironment       = "TUTTI_DESKTOP_UPDATE_ADMISSION_PACKAGED"
	currentVersionEnvironment = "TUTTI_DESKTOP_UPDATE_ADMISSION_CURRENT_VERSION"
	platformEnvironment       = "TUTTI_DESKTOP_UPDATE_ADMISSION_PLATFORM"
	architectureEnvironment   = "TUTTI_DESKTOP_UPDATE_ADMISSION_ARCHITECTURE"

	productionEndpoint = "https://tutti.sh/api/desktop/v1/public/desktop-version/check"
)

type Service struct {
	runtime *admissiondaemon.Service
}

func NewFromEnvironment() (*Service, error) {
	managed, err := readBooleanEnvironment(managedEnvironment)
	if err != nil {
		return nil, err
	}
	if !managed {
		return nil, nil
	}
	packaged, err := readBooleanEnvironment(packagedEnvironment)
	if err != nil {
		return nil, err
	}
	identity := admissiondaemon.Identity{
		Product:        admissiondaemon.ProductTuttiDesktop,
		Platform:       admissiondaemon.Platform(strings.TrimSpace(os.Getenv(platformEnvironment))),
		Architecture:   admissiondaemon.Architecture(strings.TrimSpace(os.Getenv(architectureEnvironment))),
		CurrentVersion: strings.TrimSpace(os.Getenv(currentVersionEnvironment)),
	}

	checksEnabled := packaged
	foregroundInterval := 30 * time.Minute
	var checker admissiondaemon.Checker
	if packaged {
		checker = admissiondaemon.HTTPChecker{
			Client:    httpx.Default(),
			Endpoint:  productionEndpoint,
			UserAgent: "Tutti Desktop",
		}
	} else {
		development, err := admissiondaemon.ResolveDevelopment(environmentMap(), identity)
		if err != nil {
			return nil, err
		}
		checksEnabled = development.Enabled
		if development.ForegroundInterval > 0 {
			foregroundInterval = development.ForegroundInterval
		}
		switch development.Transport {
		case "in-process":
			checker = development.Checker
		case "loopback":
			checker = admissiondaemon.HTTPChecker{
				Client:    httpx.Default(),
				Endpoint:  development.MockServerURL + "/api/desktop/v1/public/desktop-version/check",
				UserAgent: "Tutti Desktop",
			}
		}
	}

	runtime, err := admissiondaemon.New(admissiondaemon.Config{
		Identity:      identity,
		ChecksEnabled: checksEnabled,
		Checker:       checker,
		FeatureCache: admissiondaemon.FileFeatureCache{
			Path: filepath.Join(
				tuttitypes.DefaultStateDir(),
				"desktop-update-admission-feature-v1.json",
			),
		},
		ForegroundInterval: foregroundInterval,
		Logger:             slog.Default(),
	})
	if err != nil {
		return nil, fmt.Errorf("create desktop update admission service: %w", err)
	}
	return &Service{runtime: runtime}, nil
}

func (service *Service) Start(ctx context.Context) {
	if service != nil && service.runtime != nil {
		service.runtime.Start(ctx)
	}
}

func (service *Service) WaitInitial(ctx context.Context) (admissiondaemon.Snapshot, error) {
	if service == nil || service.runtime == nil {
		return admissiondaemon.Snapshot{}, errors.New("desktop update admission service is unavailable")
	}
	return service.runtime.WaitInitial(ctx)
}

func (service *Service) Snapshot() admissiondaemon.Snapshot {
	if service == nil || service.runtime == nil {
		return admissiondaemon.Snapshot{}
	}
	return service.runtime.Snapshot()
}

func (service *Service) Refresh(
	ctx context.Context,
	trigger admissiondaemon.RefreshTrigger,
) (admissiondaemon.RefreshResult, error) {
	if service == nil || service.runtime == nil {
		return admissiondaemon.RefreshResult{}, errors.New("desktop update admission service is unavailable")
	}
	return service.runtime.Refresh(ctx, trigger)
}

func (service *Service) Close() {
	if service != nil && service.runtime != nil {
		service.runtime.Close()
	}
}

func readBooleanEnvironment(name string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(name))) {
	case "", "0", "false", "no", "off":
		return false, nil
	case "1", "true", "yes", "on":
		return true, nil
	default:
		return false, fmt.Errorf("%s must be a boolean flag", name)
	}
}

func environmentMap() map[string]string {
	result := make(map[string]string)
	for _, entry := range os.Environ() {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			result[key] = value
		}
	}
	return result
}
