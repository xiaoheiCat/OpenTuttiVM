package gateway

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"

	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
)

// TunneledDialer opens a stream to a resolved room route (the yamux tunnel
// client).
type TunneledDialer interface {
	Connect(route vmprotocol.RouteKey) (net.Conn, error)
}

// RouteSource lists every live route in the room so the proxy can bind
// listeners for the virtual addresses it owns.
type RouteSource interface {
	RoomRoutes(ctx context.Context) ([]vmprotocol.RouteKey, error)
}

// Proxy owns the .tutti interception surface: one listener per live route
// on its synthetic VIP, piping every connection through the server relay.
type Proxy struct {
	vips     *VIPAllocator
	dialer   TunneledDialer
	routes   RouteSource
	log      *slog.Logger
	roomID   string
	deviceID string

	mu        sync.Mutex
	listeners map[string]net.Listener

	// listen is injectable: dev machines may not own the synthetic block,
	// while container networks route it natively.
	listen func(addr string) (net.Listener, error)
}

// NewProxy wires the proxy. The allocator must be the process-wide one so
// hostname→VIP mappings stay stable.
func NewProxy(vips *VIPAllocator, dialer TunneledDialer, routes RouteSource, roomID, deviceID string, log *slog.Logger) *Proxy {
	if log == nil {
		log = slog.Default()
	}
	return &Proxy{
		vips: vips, dialer: dialer, routes: routes, roomID: roomID,
		deviceID: deviceID, log: log, listeners: map[string]net.Listener{},
		listen: func(addr string) (net.Listener, error) { return net.Listen("tcp", addr) },
	}
}

// Sync reconciles listeners with the room's live routes. Each session
// address gets a synthetic VIP; connections land on the session owner's
// tunnel. Called on ports_changed events and periodically.
func (p *Proxy) Sync(ctx context.Context) error {
	live, err := p.routes.RoomRoutes(ctx)
	if err != nil {
		return err
	}
	want := map[string]vmprotocol.RouteKey{}
	for _, r := range live {
		if r.SessionID == "" || r.Port == 0 {
			continue
		}
		want[p.addrFor(r)] = r
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	for addr, ln := range p.listeners {
		if _, ok := want[addr]; !ok {
			ln.Close()
			delete(p.listeners, addr)
		}
	}
	for addr, route := range want {
		if _, exists := p.listeners[addr]; exists {
			continue
		}
		ln, err := p.listen(addr)
		if err != nil {
			p.log.Warn("tutti vip listen failed", "addr", addr, "err", err)
			continue
		}
		p.listeners[addr] = ln
		go p.accept(ln, route)
		p.log.Info("tutti route live", "addr", addr, "session", route.SessionID, "port", route.Port)
	}
	return nil
}

// Close drops every listener (room teardown).
func (p *Proxy) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for addr, ln := range p.listeners {
		ln.Close()
		delete(p.listeners, addr)
	}
}

// LiveAddrs exposes the actually-bound listener addresses (status/tests).
func (p *Proxy) LiveAddrs() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, 0, len(p.listeners))
	for _, ln := range p.listeners {
		out = append(out, ln.Addr().String())
	}
	return out
}

func (p *Proxy) addrFor(r vmprotocol.RouteKey) string {
	host := vmprotocol.TuttiHost{Device: p.slugOf(r), Session: strings.TrimPrefix(r.SessionID, "sess-")}
	ip := p.vips.Assign(host)
	return net.JoinHostPort(ip.String(), fmt.Sprintf("%d", r.Port))
}

func (p *Proxy) slugOf(r vmprotocol.RouteKey) string {
	if r.DeviceID == p.deviceID {
		return "self"
	}
	return vmprotocol.SlugifyHostname(r.DeviceID)
}

func (p *Proxy) accept(ln net.Listener, route vmprotocol.RouteKey) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go p.pipe(conn, route)
	}
}

func (p *Proxy) pipe(client net.Conn, route vmprotocol.RouteKey) {
	defer client.Close()
	target, err := p.dialer.Connect(vmprotocol.RouteKey{
		RoomID: p.roomID, DeviceID: route.DeviceID, SessionID: route.SessionID, Port: route.Port,
	})
	if err != nil {
		p.log.Warn("tutti tunnel connect failed", "session", route.SessionID, "port", route.Port, "err", err)
		return
	}
	defer target.Close()
	go io.Copy(target, client)
	io.Copy(client, target)
}
