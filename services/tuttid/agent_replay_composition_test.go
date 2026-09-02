package main

import (
	"context"
	"errors"
	"testing"

	agentdaemon "github.com/xiaoheiCat/OpenTuttiVM/packages/agent/daemon"
	sessionreplay "github.com/xiaoheiCat/OpenTuttiVM/packages/agent/session-replay"
	agentservice "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/agent"
	agentstatusservice "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/agentstatus"
)

type replayVerifierTransport struct {
	err               error
	calls             int
	speed             float64
	playbackElapsedMS float64
	paused            bool
	fastForward       bool
	setSpeed          float64
	providerCursor    map[string]sessionreplay.ProviderUnitPosition
}

func (t *replayVerifierTransport) Finalize() error {
	t.calls++
	return t.err
}

func (t *replayVerifierTransport) ReplayPlaybackState(string) (agentdaemon.ReplayPlaybackState, error) {
	return agentdaemon.ReplayPlaybackState{
		Drained:           true,
		Speed:             t.speed,
		PlaybackElapsedMS: t.playbackElapsedMS,
		Paused:            t.paused,
		FastForward:       t.fastForward,
	}, t.err
}

func (t *replayVerifierTransport) SetReplayPlaybackSpeed(_ string, speed float64) error {
	t.setSpeed = speed
	t.speed = speed
	return t.err
}

func (t *replayVerifierTransport) PauseReplayPlayback(string) error {
	t.paused = true
	return t.err
}

func (t *replayVerifierTransport) ResumeReplayPlayback(string) error {
	t.paused = false
	return t.err
}

func (t *replayVerifierTransport) SetReplayPlaybackFastForward(_ string, enabled bool) error {
	t.fastForward = enabled
	return t.err
}

func (t *replayVerifierTransport) SetReplayProviderCursor(
	_ string,
	targets []sessionreplay.ProviderUnitPosition,
) error {
	t.providerCursor = make(
		map[string]sessionreplay.ProviderUnitPosition,
		len(targets),
	)
	for _, target := range targets {
		t.providerCursor[target.ConnectionID] = target
	}
	return t.err
}

func (t *replayVerifierTransport) ClearReplayProviderCursor(string) error {
	t.providerCursor = map[string]sessionreplay.ProviderUnitPosition{}
	return t.err
}

func (t *replayVerifierTransport) ReplayProviderCursor(
	string,
) (map[string]sessionreplay.ProviderUnitPosition, error) {
	return t.providerCursor, t.err
}

func (t *replayVerifierTransport) VerifyComplete(string) error {
	t.calls++
	return t.err
}

func TestReplayProviderAvailabilityCheckerDoesNotProbeHost(t *testing.T) {
	got, err := (replayProviderAvailabilityChecker{}).ListProviderAvailability(
		context.Background(),
		[]string{"codex"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 ||
		got[0].Provider != "codex" ||
		got[0].Status != agentservice.ProviderAvailabilityAvailable {
		t.Fatalf("availability = %#v, want replay-local available", got)
	}
}

func TestReplayProviderStatusDoesNotRequireHostCLIOrCredentials(t *testing.T) {
	service := replayAgentProviderStatusService{}
	snapshot, err := service.List(
		context.Background(),
		agentstatusservice.ListInput{Providers: []string{"claude-code", "cursor"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Providers) != 2 ||
		snapshot.Providers[0].Availability.Status != agentstatusservice.AvailabilityReady ||
		snapshot.Providers[0].Auth.Status != agentstatusservice.AuthAuthenticated ||
		snapshot.Providers[1].Availability.Status != agentstatusservice.AvailabilityUnsupported {
		t.Fatalf("replay provider statuses = %#v", snapshot.Providers)
	}
	probe, err := service.Probe(
		context.Background(),
		agentstatusservice.ProbeInput{Provider: "claude-code"},
	)
	if err != nil || probe.Status != agentstatusservice.ProbeSkipped ||
		!probe.ProtocolReady || len(probe.Command) != 0 {
		t.Fatalf("replay provider probe = %#v, err=%v", probe, err)
	}
	if err := service.DiscoverManagedProviderUpdates(context.Background()); err != nil {
		t.Fatalf("replay provider update discovery = %v", err)
	}
}

func TestAgentReplayTransportVerifierFailsClosedAndPropagatesMismatch(t *testing.T) {
	transport := &replayVerifierTransport{err: errors.New("leftover frame")}
	disabled := agentReplayTransportVerifier{transport: transport}
	if err := disabled.Verify(context.Background(), "cassette-1"); err == nil {
		t.Fatal("disabled verifier succeeded")
	}
	if transport.calls != 0 {
		t.Fatalf("disabled verifier finalized %d times", transport.calls)
	}
	enabled := agentReplayTransportVerifier{
		enabled: true, transport: transport,
		verifyState: func(context.Context, string) error { return nil },
	}
	if err := enabled.Verify(context.Background(), "cassette-1"); err == nil ||
		err.Error() != "leftover frame" {
		t.Fatalf("enabled verifier error = %v", err)
	}
	if transport.calls != 1 {
		t.Fatalf("enabled verifier finalized %d times, want 1", transport.calls)
	}
}

func TestAgentReplayTransportVerifierControlsPlaybackOnlyWhenEnabled(t *testing.T) {
	transport := &replayVerifierTransport{speed: 1}
	disabled := agentReplayTransportVerifier{transport: transport}
	if _, err := disabled.PlaybackState(context.Background(), "cassette-1"); err == nil {
		t.Fatal("disabled playback read succeeded")
	}
	enabled := agentReplayTransportVerifier{
		enabled: true, transport: transport,
		verifyState: func(context.Context, string) error { return nil },
	}
	state, err := enabled.SetPlaybackSpeed(context.Background(), "cassette-1", 2)
	if err != nil {
		t.Fatal(err)
	}
	if state.Speed != 2 || !state.Drained || transport.setSpeed != 2 {
		t.Fatalf("playback = %#v / %v, want 2 drained", state, transport.setSpeed)
	}
	state, err = enabled.SetPlaybackPaused(context.Background(), "cassette-1", true)
	if err != nil || !state.Paused {
		t.Fatalf("paused playback = %#v, err=%v", state, err)
	}
	state, err = enabled.SetPlaybackPaused(context.Background(), "cassette-1", false)
	if err != nil || state.Paused {
		t.Fatalf("resumed playback = %#v, err=%v", state, err)
	}
	state, err = enabled.SetPlaybackFastForward(context.Background(), "cassette-1", true)
	if err != nil || !state.FastForward {
		t.Fatalf("fast-forward playback = %#v, err=%v", state, err)
	}
}
