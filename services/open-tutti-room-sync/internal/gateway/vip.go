package gateway

import (
	"errors"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
)

var ErrVIPExhausted = errors.New("tutti VIP address pool exhausted")

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

// sharedAddrMu/record cache the shared-mode answer. On LINUX (the
// room container topology) DNS consumers (agent and session
// containers) live in OTHER network namespaces, so 127.0.0.1 would
// point back into the CALLING container: answer the process's own
// bridge/overlay address, which is private to the compose network and
// unreachable from the LAN. On OTHER systems room-sync runs native in
// one namespace (no container bridge to cross), so the answer stays
// loopback — binding a LAN interface there would expose unauthenticated
// room-only services to the local network.
var (
	sharedAddrOnce sync.Once
	sharedAddr     net.IP
)

func probeSharedAddr() net.IP {
	sharedAddrOnce.Do(func() {
		sharedAddr = net.IP{127, 0, 0, 1}
		if runtime.GOOS != "linux" {
			return
		}
		// INSIDE a container the shared address must be this
		// namespace's own eth0 (sibling agent/session containers reach
		// it; loopback would point at each container itself). On a
		// NATIVE host, only an identified docker*/br-* bridge qualifies
		// — the first private address there is routinely Wi-Fi/Ethernet,
		// and binding unauthenticated session listeners to it would
		// expose them to the whole LAN.
		inContainer := runningInContainer()
		ifaces, err := net.Interfaces()
		if err != nil {
			return
		}
		for _, ifc := range ifaces {
			if inContainer {
				if ifc.Name != "eth0" {
					continue
				}
			} else if !strings.HasPrefix(ifc.Name, "docker") && !strings.HasPrefix(ifc.Name, "br-") {
				continue
			}
			addrs, err := ifc.Addrs()
			if err != nil {
				continue
			}
			for _, a := range addrs {
				ipNet, ok := a.(*net.IPNet)
				if !ok || ipNet.IP.IsLoopback() || ipNet.IP.To4() == nil || !ipNet.IP.IsPrivate() {
					continue
				}
				sharedAddr = ipNet.IP.To4()
				return
			}
		}
	})
	return sharedAddr
}

// Probe decides the addressing mode BEFORE the first Assign: binding a
// reserved address that no adapter owns fails with EADDRNOTAVAIL on every
// platform, which would leave every .tutti route listener dead outside the
// room VM image. IP_FREEBIND (Linux) lets listeners claim the address
// without interface configuration; everywhere else the allocator shares
// 127.0.0.1 and the proxy demultiplexes by SNI/Host.
func (a *VIPAllocator) Probe() {
	a.probed.Do(func() {
		vip := ipFromOffset(200, 200).String()
		if ln, err := net.Listen("tcp", net.JoinHostPort(vip, "0")); err == nil {
			ln.Close()
			return // the block is locally configured: real VIPs
		}
		ln, err := freebindListen(net.JoinHostPort(vip, "0"))
		if err != nil {
			a.mode.Store(int32(modeShared))
			return
		}
		// FREEBIND only permits the BIND — it neither assigns nor
		// routes the address. On hosts without a route for
		// 100.96.0.0/12 the bind above succeeds while nothing can
		// ever connect, and DNS advertising 100.96.* would blackhole
		// every client instead of handing out the shared-mode address.
		// Prove reachability by connecting to the freebound listener
		// from this namespace before selecting VIP mode.
		if !dialable(ln) {
			ln.Close()
			a.mode.Store(int32(modeShared))
			return
		}
		ln.Close()
		a.freebindOK = true
	})
}

// runningInContainer reports whether this process shares a container
// namespace (the standard no-NET_ADMIN deployment): /.dockerenv or a
// container runtime marker in PID 1's cgroup.
func runningInContainer() bool {
	if _, err := os.Stat("/.dockerenv"); err == nil {
		return true
	}
	if data, err := os.ReadFile("/proc/1/cgroup"); err == nil {
		for _, marker := range []string{"docker", "containerd", "kubepods"} {
			if strings.Contains(string(data), marker) {
				return true
			}
		}
	}
	return false
}

// dialable reports whether the listener's freebound address actually
// routes: a successful dial through the host routing table is the only
// evidence a client could ever reach it.
func dialable(ln net.Listener) bool {
	addr := ln.Addr().String()
	done := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err == nil {
			c.Close()
		}
		done <- err
	}()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return <-done == nil
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
	ip, _ := a.AssignWithError(host)
	return ip
}

// AssignWithError allocates without ever reusing an address held by another
// host. A nil IP means the finite reserved pool is exhausted.
func (a *VIPAllocator) AssignWithError(host vmprotocol.TuttiHost) (net.IP, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if vipMode(a.mode.Load()) == modeShared {
		addr := probeSharedAddr()
		a.byKey[host.String()] = addr
		return addr, nil
	}
	key := host.String()
	if ip, ok := a.byKey[key]; ok {
		return ip, nil
	}
	const poolSize = 200 * 200
	for a.next <= poolSize && a.used[ipFromOffset(1+(a.next-1)/200, 1+(a.next-1)%200).String()] {
		a.next++
	}
	if a.next > poolSize {
		return nil, ErrVIPExhausted
	}
	device := uint32(1 + (a.next-1)/200)
	index := uint32(1 + (a.next-1)%200)
	a.next++
	ip := ipFromOffset(device, index)
	a.byKey[key] = ip
	a.used[ip.String()] = true
	return ip, nil
}

// Release returns a hostname's VIP to the pool. It is safe to call for an
// unknown host and never releases an address still held by another key.
func (a *VIPAllocator) Release(host vmprotocol.TuttiHost) {
	a.mu.Lock()
	defer a.mu.Unlock()
	key := host.String()
	ip, ok := a.byKey[key]
	if !ok {
		return
	}
	delete(a.byKey, key)
	delete(a.used, ip.String())
	if a.next > 1 {
		// Reusing the lowest returned slot bounds churn without changing
		// addresses held by live hosts.
		for n := uint32(1); n < a.next; n++ {
			if !a.used[ipFromOffset(1+(n-1)/200, 1+(n-1)%200).String()] {
				a.next = n
				break
			}
		}
	}
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
