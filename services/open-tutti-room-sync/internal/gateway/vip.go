package gateway

import (
	"net"
	"sync"

	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
)

// VIPAllocator assigns synthetic addresses from 100.96.0.0/12:
// device hostnames get addresses in 100.96.0.0/16, session hostnames get
// addresses in 100.96.16.0+ space derived from the device block. Addresses
// are stable while held and released back to the pool on Room teardown.
//
// These addresses exist only inside the room network namespace; the gateway
// intercepts them and tunnels to the server.
type VIPAllocator struct {
	mu    sync.Mutex
	byKey map[string]net.IP
	used  map[string]bool
	next  uint32
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

// Assign returns the synthetic IP for a hostname, allocating on first use.
func (a *VIPAllocator) Assign(host vmprotocol.TuttiHost) net.IP {
	a.mu.Lock()
	defer a.mu.Unlock()
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
