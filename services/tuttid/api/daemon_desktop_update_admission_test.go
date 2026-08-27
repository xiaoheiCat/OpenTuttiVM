package api

import (
	"context"
	"net/http"
	"testing"
	"time"

	admissiondaemon "github.com/xiaoheiCat/OpenTuttiVM/packages/desktop/update-admission/daemon"
	tuttigenerated "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/api/generated"
)

type stubDesktopUpdateAdmissionService struct {
	initial admissiondaemon.Snapshot
	current admissiondaemon.Snapshot
	refresh admissiondaemon.RefreshResult
	trigger admissiondaemon.RefreshTrigger
}

func (service *stubDesktopUpdateAdmissionService) WaitInitial(context.Context) (admissiondaemon.Snapshot, error) {
	return service.initial, nil
}

func (service *stubDesktopUpdateAdmissionService) Snapshot() admissiondaemon.Snapshot {
	return service.current
}

func (service *stubDesktopUpdateAdmissionService) Refresh(
	_ context.Context,
	trigger admissiondaemon.RefreshTrigger,
) (admissiondaemon.RefreshResult, error) {
	service.trigger = trigger
	return service.refresh, nil
}

func TestDaemonAPIDesktopUpdateAdmissionStartupAndRefresh(t *testing.T) {
	now := time.Date(2026, time.August, 2, 9, 30, 0, 0, time.UTC)
	snapshot := admissiondaemon.Snapshot{
		Identity: admissiondaemon.Identity{
			Product:        admissiondaemon.ProductTuttiDesktop,
			Platform:       admissiondaemon.PlatformMacOS,
			Architecture:   admissiondaemon.ArchitectureARM64,
			CurrentVersion: "1.2.3",
		},
		Policy: admissiondaemon.PolicySnapshot{
			Status: "resolved",
			Response: &admissiondaemon.PolicyResponse{
				Channel:        "stable",
				MinimumVersion: "1.2.0",
				Decision:       "allowed",
				Reason:         "meetsMinimum",
				PolicyRevision: "v12",
			},
		},
		FeatureAvailability: admissiondaemon.FeatureAvailabilitySnapshot{
			Keys:           []string{"desktop.example"},
			Source:         "remote",
			PolicyRevision: pointerString("v12"),
			FetchedAt:      &now,
		},
		LastAttemptAt:         &now,
		NextForegroundCheckAt: pointerTimeForAPI(now.Add(30 * time.Minute)),
	}
	service := &stubDesktopUpdateAdmissionService{
		initial: snapshot,
		current: snapshot,
		refresh: admissiondaemon.RefreshResult{Performed: true, Snapshot: snapshot},
	}
	mux := http.NewServeMux()
	RegisterRoutes(mux, NewRoutes(DaemonAPI{DesktopUpdateAdmissionService: service}))

	startupRecorder := performGeneratedRouteRequest(
		t,
		mux,
		http.MethodGet,
		"/v1/desktop-update-admission/startup",
		nil,
	)
	if startupRecorder.Code != http.StatusOK {
		t.Fatalf("startup status = %d; body: %s", startupRecorder.Code, startupRecorder.Body.String())
	}
	var startup tuttigenerated.DesktopUpdateAdmissionSnapshot
	decodeGeneratedRouteResponse(t, startupRecorder, &startup)
	if startup.Identity.CurrentVersion != "1.2.3" ||
		startup.Policy.Response == nil ||
		startup.Policy.Response.MinimumVersion == nil ||
		*startup.Policy.Response.MinimumVersion != "1.2.0" ||
		len(startup.FeatureAvailability.Keys) != 1 {
		t.Fatalf("startup response = %#v", startup)
	}

	refreshRecorder := performGeneratedRouteRequest(
		t,
		mux,
		http.MethodPost,
		"/v1/desktop-update-admission/refresh",
		map[string]any{"trigger": "retry"},
	)
	if refreshRecorder.Code != http.StatusOK {
		t.Fatalf("refresh status = %d; body: %s", refreshRecorder.Code, refreshRecorder.Body.String())
	}
	if service.trigger != admissiondaemon.RefreshTriggerRetry {
		t.Fatalf("refresh trigger = %q", service.trigger)
	}
}

func TestDaemonAPIDesktopUpdateAdmissionRejectsInvalidRefresh(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRoutes(mux, NewRoutes(DaemonAPI{
		DesktopUpdateAdmissionService: &stubDesktopUpdateAdmissionService{},
	}))

	recorder := performGeneratedRouteRequest(
		t,
		mux,
		http.MethodPost,
		"/v1/desktop-update-admission/refresh",
		map[string]any{"trigger": "startup"},
	)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d; body: %s", recorder.Code, recorder.Body.String())
	}
}

func pointerString(value string) *string {
	return &value
}

func pointerTimeForAPI(value time.Time) *time.Time {
	return &value
}
