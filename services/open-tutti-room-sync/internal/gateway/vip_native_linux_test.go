//go:build linux

package gateway

import (
	"net"
	"testing"

	"github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
)

// TestNativeVIPListener covers the PRODUCTION listener path on Linux:
// the relay test overrides p.listen with loopback, so IP_FREEBIND
// probing and the freebind bind itself would otherwise never execute
// in CI while deployed containers silently lost their VIP listeners.
func TestNativeVIPListener(t *testing.T) {
	vips := NewVIPAllocator()
	vips.Probe()
	if vipMode(vips.mode.Load()) != modeVIP {
		t.Skip("environment has no freebind-capable synthetic block (containerized runner) — shared-mode fallback covered elsewhere")
	}
	if !vips.NeedsFreebind() {
		t.Fatalf("vip mode without freebind need is inconsistent")
	}
	// Bind a real reserved address through the production helper.
	host, err := vmprotocol.ParseTuttiHost("dev-nat.Annas-MBP.tutti:3000")
	if err != nil {
		t.Fatalf("parse host: %v", err)
	}
	ip := vips.Assign(host)
	ln, err := freebindListen(net.JoinHostPort(ip.String(), "0"))
	if err != nil {
		t.Fatalf("freebind listen: %v", err)
	}
	defer ln.Close()
	// The listener must actually accept on the reserved address.
	go func() {
		c, aerr := ln.Accept()
		if aerr == nil {
			c.Close()
		}
	}()
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial reserved %s: %v", ln.Addr(), err)
	}
	c.Close()
}
