package api

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/xiaoheiCat/OpenTuttiVM/packages/agent/daemon/providerregistry"
	tuttigenerated "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/api/generated"
	agenttargetbiz "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/biz/agenttarget"
	agenttargetservice "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/agenttarget"
)

func TestDaemonAPIGeneratedRoutesListAgentTargets(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRoutes(mux, NewRoutes(DaemonAPI{
		AgentTargetService: stubAgentTargetService{},
	}))

	recorder := performGeneratedRouteRequest(t, mux, http.MethodGet, "/v1/agent-targets", nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}

	var response tuttigenerated.ListAgentTargetsResponse
	decodeGeneratedRouteResponse(t, recorder, &response)
	if len(response.Targets) != 7 {
		t.Fatalf("targets len = %d, want descriptor catalog size 7", len(response.Targets))
	}
	wantIDs := []string{
		agenttargetbiz.IDLocalTuttiAgent,
		agenttargetbiz.IDLocalCodex,
		agenttargetbiz.IDLocalClaudeCode,
		agenttargetbiz.IDLocalCursor,
		agenttargetbiz.IDLocalOpenCode,
		providerregistry.NexightTargetID,
		providerregistry.OpenClawTargetID,
	}
	for index, target := range response.Targets {
		launchRef, err := target.LaunchRef.AsAgentTargetBuiltinLocalLaunchRef()
		if err != nil || target.Id != wantIDs[index] || launchRef.Type != tuttigenerated.AgentTargetBuiltinLocalLaunchRefTypeBuiltinLocal {
			t.Fatalf("target[%d] = %#v, want id %q builtin_local", index, target, wantIDs[index])
		}
	}
}

func TestDaemonAPIGeneratedRoutesSetSystemAgentTargetEnabled(t *testing.T) {
	mux := http.NewServeMux()
	var captured agenttargetservice.SetEnabledInput
	var readinessTrigger string
	RegisterRoutes(mux, NewRoutes(DaemonAPI{
		AgentTargetService: stubAgentTargetService{
			setEnabledFn: func(_ context.Context, input agenttargetservice.SetEnabledInput) (agenttargetbiz.Target, error) {
				captured = input
				var target agenttargetbiz.Target
				for _, candidate := range agenttargetbiz.DefaultSystemTargets(1) {
					if candidate.ID == agenttargetbiz.IDLocalTuttiAgent {
						target = candidate
						break
					}
				}
				target.Enabled = input.Enabled
				target.UpdatedAtUnixMS = 2
				return target, nil
			},
		},
		TuttiAgentReadiness: stubTuttiAgentReadiness{
			triggerFn: func(reason string) {
				readinessTrigger = reason
			},
		},
	}))

	recorder := performGeneratedRouteRequest(
		t,
		mux,
		http.MethodPatch,
		"/v1/agent-targets/local:tutti-agent/enabled",
		map[string]any{"enabled": false},
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	if captured.ID != agenttargetbiz.IDLocalTuttiAgent || captured.Enabled {
		t.Fatalf("captured input = %#v", captured)
	}
	if readinessTrigger != "target_enabled_changed" {
		t.Fatalf("readiness trigger = %q, want target_enabled_changed", readinessTrigger)
	}
	var response tuttigenerated.AgentTarget
	decodeGeneratedRouteResponse(t, recorder, &response)
	if response.Id != agenttargetbiz.IDLocalTuttiAgent || response.Enabled {
		t.Fatalf("response = %#v", response)
	}
}

func TestDaemonAPIGeneratedRoutesSetSystemAgentTargetEnabledRejectsUserTarget(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRoutes(mux, NewRoutes(DaemonAPI{
		AgentTargetService: stubAgentTargetService{
			setEnabledFn: func(context.Context, agenttargetservice.SetEnabledInput) (agenttargetbiz.Target, error) {
				return agenttargetbiz.Target{}, agenttargetservice.ErrSystemTargetImmutable
			},
		},
	}))

	recorder := performGeneratedRouteRequest(
		t,
		mux,
		http.MethodPatch,
		"/v1/agent-targets/custom-codex/enabled",
		map[string]any{"enabled": false},
	)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body: %s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
	}
}

func TestDaemonAPIGeneratedRoutesRejectsDisablingTuttiAgent(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRoutes(mux, NewRoutes(DaemonAPI{
		AgentTargetService: stubAgentTargetService{
			setEnabledFn: func(context.Context, agenttargetservice.SetEnabledInput) (agenttargetbiz.Target, error) {
				return agenttargetbiz.Target{}, agenttargetservice.ErrTuttiAgentAlwaysEnabled
			},
		},
	}))

	recorder := performGeneratedRouteRequest(
		t,
		mux,
		http.MethodPatch,
		"/v1/agent-targets/local:tutti-agent/enabled",
		map[string]any{"enabled": false},
	)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body: %s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
	}
}

func TestDaemonAPIGeneratedRoutesSetSystemAgentTargetEnabledRequiresEnabled(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		body any
	}{
		{name: "empty object", body: map[string]any{}},
		{name: "null", body: json.RawMessage("null")},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			mux := http.NewServeMux()
			RegisterRoutes(mux, NewRoutes(DaemonAPI{
				AgentTargetService: stubAgentTargetService{
					setEnabledFn: func(context.Context, agenttargetservice.SetEnabledInput) (agenttargetbiz.Target, error) {
						t.Fatal("SetEnabled should not be called when enabled is missing")
						return agenttargetbiz.Target{}, nil
					},
				},
			}))

			recorder := performGeneratedRouteRequest(
				t,
				mux,
				http.MethodPatch,
				"/v1/agent-targets/local:tutti-agent/enabled",
				test.body,
			)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d; body: %s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
			}
		})
	}
}
