package api

import (
	"testing"

	"github.com/xiaoheiCat/OpenTuttiVM/packages/agent/store-sqlite/canonical"
)

func TestGeneratedAgentCapabilitiesProjectsActiveTurnGuidance(t *testing.T) {
	t.Parallel()

	capabilities := generatedAgentCapabilities([]string{"activeTurnGuidance"})
	if !capabilities.ActiveTurnGuidance {
		t.Fatal("activeTurnGuidance = false, want true")
	}
	if capabilities.Interrupt {
		t.Fatal("interrupt = true, want capability fields projected independently")
	}
}

func TestGeneratedAgentSessionCapabilitiesPreservesUnknownSnapshot(t *testing.T) {
	t.Parallel()

	if capabilities := generatedAgentSessionCapabilities(nil); capabilities != nil {
		t.Fatalf("unknown capabilities = %#v, want nil", capabilities)
	}
	reportedEmpty := generatedAgentSessionCapabilities(canonical.NewCapabilitySnapshot(nil))
	if reportedEmpty == nil {
		t.Fatal("reported empty capabilities = nil")
	}
	if reportedEmpty.GoalPause || reportedEmpty.Interrupt {
		t.Fatalf("reported empty capabilities = %#v, want closed false record", reportedEmpty)
	}
}
