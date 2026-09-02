package agent

import (
	"testing"

	"github.com/xiaoheiCat/OpenTuttiVM/packages/agent/store-sqlite/canonical"
)

func TestServiceSessionCapabilityMergeUsesLiveSnapshotIndependentlyOfFreshness(t *testing.T) {
	t.Run("live reported wins when runtime is newer", func(t *testing.T) {
		session := serviceSessionWithPersistedFreshness(ProviderRuntimeSession{
			ID:              "session",
			Provider:        "codex",
			Capabilities:    canonical.NewCapabilitySnapshot([]string{canonical.CapabilityGoalPause}),
			UpdatedAtUnixMS: 200,
		}, PersistedSession{ID: "session", UpdatedAtUnixMS: 100}, true)
		assertSessionCapabilityValues(t, session.Capabilities, canonical.CapabilityGoalPause)
	})

	t.Run("live reported empty wins when persistence is newer", func(t *testing.T) {
		session := serviceSessionWithPersistedFreshness(ProviderRuntimeSession{
			ID:              "session",
			Provider:        "codex",
			Capabilities:    canonical.NewCapabilitySnapshot(nil),
			UpdatedAtUnixMS: 100,
		}, PersistedSession{
			ID:              "session",
			Provider:        "codex",
			Capabilities:    canonical.NewCapabilitySnapshot([]string{canonical.CapabilityGoalPause}),
			UpdatedAtUnixMS: 200,
		}, true)
		assertSessionCapabilityValues(t, session.Capabilities)
	})

	t.Run("persisted snapshot fills unknown live state", func(t *testing.T) {
		session := serviceSessionWithPersistedFreshness(ProviderRuntimeSession{
			ID:              "session",
			Provider:        "codex",
			UpdatedAtUnixMS: 200,
		}, PersistedSession{
			ID:              "session",
			Provider:        "codex",
			Capabilities:    canonical.NewCapabilitySnapshot([]string{canonical.CapabilityGoalPause}),
			UpdatedAtUnixMS: 100,
		}, true)
		assertSessionCapabilityValues(t, session.Capabilities, canonical.CapabilityGoalPause)
	})
}

func assertSessionCapabilityValues(t *testing.T, snapshot *canonical.CapabilitySnapshot, expected ...string) {
	t.Helper()
	if snapshot == nil {
		t.Fatal("capability snapshot = nil")
		return
	}
	if len(snapshot.Values) != len(expected) {
		t.Fatalf("capability values = %#v, want %#v", snapshot.Values, expected)
	}
	for index := range expected {
		if snapshot.Values[index] != expected[index] {
			t.Fatalf("capability values = %#v, want %#v", snapshot.Values, expected)
		}
	}
}
