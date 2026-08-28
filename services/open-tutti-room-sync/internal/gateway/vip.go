package gateway

import (
	"net"
	"sync"
	"sync/atomic"

	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
)

// Addressing modes for the room's virtual network.
type vipMode int32

const (
	// modeVIP: the reserved 100.96/12 block binds locally (room VM
	// image, or IP_FREEBIND on Linux) — every .tutti host owns a
	// distinct address.
	modeVIP vipMode = iota
	// modeShared: reserved addresses are unassignable (plain
	// containers without NET_ADMIN, stock macOS/Windows runtimes).
	// DNS answers 127.0.0.1 for every host and listeners share
	// 127.0.0.1:port, demultiplexed by TLS SNI or the HTTP Host.
	modeShared
)

// VIPAllocator assigns synthetic addresses for .tutti hosts:
// device hostnames get addresses in 100.96.0.0/16, session hostnames get
// addresses in 100.96.16.0+ space derived from the device block. Addresses
// are stable while held and released back to the pool on Room teardown.
type VIPAllocator struct {
	mu    sync.Mutex
	byKey map[string]net.IP
	used  map[string]bool
	next  uint32

	mode       atomic.Int32 // vipMode
	probed     sync.Once
	freebindOK bool
}

// NewVIPAllocator returns an allocator over the reserved block.
func NewVIPAllocator() *VIPAllocator {
	return &VIPAllocator{byKey: map[string]net.IP{}, used: map[string]bool{}, next: 1}
}

func ipFromOffset(device, index uint32) net.IP {
	// 4-byte IPv4 form: 100.96.<device>.<index> — devices and sessions
	// share the /12 block; the 4-byte form avoids the v6-mapped
	// representation's leading zero bytes.
	return net.IP{100, 96, byte(device), byte(index)}
}

// sharedAddr is the single shared-address answer in modeShared (only
// 127.0.0.1 is bindable on stock macOS/Windows without configuration).
var sharedAddr = net.IP{127, 0, 0, 1}

// Probe decides the addressing mode BEFORE the first Assign: binding a
// reserved address that no adapter owns fails with EADDRNOTAVAIL on every
// platform, which would leave every .tutti route listener dead outside the
// room VM image. IP_FREEBIND (Linux) lets listeners claim the address
// without interface configuration; everywhere else the allocator shares
// 127.0.0.1 and the proxy demultiplexes by SNI/Host.
func (a *VIPAllocator) Probe() {
	a.probed.Do(func() {
		if ln, err := net.Listen("tcp", net.JoinHostPort(ipFromOffset(200, 200).String(), "0")); err == nil {
			ln.Close()
			return // the block is locally configured: real VIPs
		}
		if ln, err := freebindListen(net.JoinHostPort(ipFromOffset(200, 200).String(), "0")); err == nil {
			ln.Close()
			a.freebindOK = true
			return // Linux: listeners can FREEBIND reserved addresses
		}
		a.mode.Store(int32(modeShared))
	})
}

// Shared reports whether the room network runs on the shared loopback
// address (reserved block unavailable).
func (a *VIPAllocator) Shared() bool { return vipMode(a.mode.Load()) == modeShared }

// NeedsFreebind reports whether listeners must set IP_FREEBIND to bind
// their reserved addresses.
func (a *VIPAllocator) NeedsFreebind() bool {
	a.Probe()
	return vipMode(a.mode.Load()) == modeVIP && a.freebindOK
}

// Assign returns the synthetic IP for a hostname, allocating on first use.
func (a *VIPAllocator) Assign(host vmprotocol.TuttiHost) net.IP {
	a.mu.Lock()
	defer a.mu.Unlock()
	if vipMode(a.mode.Load()) == modeShared {
		a.byKey[host.String()] = sharedAddr
		return sharedAddr
	}
	key := host.String()
	if ip, ok := a.byKey[key]; ok {
		return ip
	}
	device := uint32(1 + (a.next/200)%200)
	index := uint32(1 + a.next%200)
	a.next++
	for a.used[ipFromOffset(device, index).String()] && a.next < 1<<16 {
		index = uint32(1 + a.next%200)
		device = uint32(1 + (a.next/200)%200)
		a.next++
	}
	ip := ipFromOffset(device, index)
	a.byKey[key] = ip
	a.used[ip.String()] = true
	return ip
}

// ReleaseAll clears allocations (room teardown).
func (a *VIPAllocator) ReleaseAll() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.byKey = map[string]net.IP{}
	a.used = map[string]bool{}
	a.next = 1
}

// Lookup returns the assigned address for a hostname.
func (a *VIPAllocator) Lookup(host vmprotocol.TuttiHost) (net.IP, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	ip, ok := a.byKey[host.String()]
	return ip, ok
}

// ReservedBlock is the synthetic address block as a CIDR string.
const ReservedBlock = vmprotocol.ReservedCIDR
