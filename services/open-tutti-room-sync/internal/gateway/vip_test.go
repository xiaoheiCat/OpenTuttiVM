package gateway

import (
	"errors"
	"fmt"
	"testing"

	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
)

// The reserved 100.96/12 block binds only inside the room VM image (or on
// Linux with IP_FREEBIND). On every other runtime those addresses are
// unassigned and listeners die with EADDRNOTAVAIL — shared mode answers
// 127.0.0.1 for every host and demultiplexes by SNI/Host at the proxy.
func TestVIPAllocatorSharedMode(t *testing.T) {
	a := NewVIPAllocator()
	a.mode.Store(int32(modeShared))

	host := vmprotocol.TuttiHost{Device: "annas-macbook-pro", Session: "main"}
	ip := a.Assign(host)
	// One process-wide answer: the first non-loopback unicast IPv4
	// (container/bridge address) or 127.0.0.1 when none exists.
	if want := probeSharedAddr(); !ip.Equal(want) {
		t.Fatalf("shared mode assigned %s, want %s", ip, want)
	}
	// Shared mode is one address for every host; identity moves to the
	// proxy's SNI/Host demultiplexer.
	other := a.Assign(vmprotocol.TuttiHost{Device: "bobs-thinkpad"})
	if !other.Equal(ip) {
		t.Fatalf("shared mode must answer one address, got %s vs %s", other, ip)
	}
	if got, ok := a.Lookup(host); !ok || !got.Equal(ip) {
		t.Fatalf("lookup mismatch %v %v", got, ok)
	}
	if !a.Shared() {
		t.Fatal("Shared() must report shared mode")
	}
}

// VIP mode keeps distinct stable per-host addresses (the room VM image
// path).
func TestVIPAllocatorVIPModeDistinctHosts(t *testing.T) {
	a := NewVIPAllocator()
	first := a.Assign(vmprotocol.TuttiHost{Device: "a"})
	second := a.Assign(vmprotocol.TuttiHost{Device: "b"})
	if first.Equal(second) {
		t.Fatalf("distinct hosts share %s", first)
	}
	if again := a.Assign(vmprotocol.TuttiHost{Device: "a"}); !again.Equal(first) {
		t.Fatalf("unstable assignment %s vs %s", first, again)
	}
	if a.Shared() {
		t.Fatal("default mode must not be shared")
	}
}

func TestVIPAllocatorExhaustionDoesNotCollide(t *testing.T) {
	a := NewVIPAllocator()
	seen := map[string]bool{}
	for i := 0; i < 200*200; i++ {
		ip, err := a.AssignWithError(vmprotocol.TuttiHost{Device: fmt.Sprintf("device-%d", i)})
		if err != nil {
			t.Fatal(err)
		}
		if seen[ip.String()] {
			t.Fatalf("colliding allocation %s", ip)
		}
		seen[ip.String()] = true
	}
	if _, err := a.AssignWithError(vmprotocol.TuttiHost{Device: "exhausted"}); !errors.Is(err, ErrVIPExhausted) {
		t.Fatalf("exhaustion error = %v", err)
	}
}

func TestVIPAllocatorReleaseReusesWithoutStealingLiveHost(t *testing.T) {
	a := NewVIPAllocator()
	keep := vmprotocol.TuttiHost{Device: "keep"}
	old := vmprotocol.TuttiHost{Device: "old"}
	keepIP := a.Assign(keep)
	oldIP := a.Assign(old)
	a.Release(old)
	newIP, err := a.AssignWithError(vmprotocol.TuttiHost{Device: "new"})
	if err != nil || !newIP.Equal(oldIP) {
		t.Fatalf("released address was not reused: %v %v", newIP, err)
	}
	if got := a.Assign(keep); !got.Equal(keepIP) {
		t.Fatalf("live host changed from %s to %s", keepIP, got)
	}
}
