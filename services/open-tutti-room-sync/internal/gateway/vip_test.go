package gateway

import (
	"net"
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
	if !ip.Equal(net.IP{127, 0, 0, 1}) {
		t.Fatalf("shared mode assigned %s, want 127.0.0.1", ip)
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
