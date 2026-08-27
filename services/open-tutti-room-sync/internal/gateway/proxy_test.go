package gateway

import (
	"bufio"
	"context"
	"net"
	"sync"
	"testing"
	"time"

	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
)

type fakeDialer struct {
	mu     sync.Mutex
	routes []vmprotocol.RouteKey
	echo   net.Listener
}

func (f *fakeDialer) Connect(route vmprotocol.RouteKey) (net.Conn, error) {
	f.mu.Lock()
	f.routes = append(f.routes, route)
	f.mu.Unlock()
	// Echo endpoint: dial a local listener that mirrors bytes.
	return net.Dial("tcp", f.echo.Addr().String())
}

func startEcho(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				bufio.NewReader(c).WriteTo(c)
			}(conn)
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return ln
}

type staticRoutes struct {
	routes []vmprotocol.RouteKey
}

func (s *staticRoutes) RoomRoutes(ctx context.Context) ([]vmprotocol.RouteKey, error) {
	return s.routes, nil
}

func TestProxyBindsVIPAndRelaysThroughTunnel(t *testing.T) {
	echo := startEcho(t)
	dialer := &fakeDialer{echo: echo}
	vips := NewVIPAllocator()
	routes := &staticRoutes{routes: []vmprotocol.RouteKey{
		{DeviceID: "dev_peer", SessionID: "sess-claude-a", Port: 3000},
	}}
	p := NewProxy(vips, dialer, routes, "room1", "dev_me", nil)
	defer p.Close()
	// Dev machines cannot bind the synthetic block; the mapping (route →
	// stable VIP address) stays under test by binding loopback instead.
	p.listen = func(addr string) (net.Listener, error) {
		_, port, _ := net.SplitHostPort(addr)
		return net.Listen("tcp", "127.0.0.1:"+port)
	}

	if err := p.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	addrs := p.LiveAddrs()
	if len(addrs) != 1 {
		t.Fatalf("live addrs %+v", addrs)
	}
	// The route's hostname holds a synthetic VIP in the reserved block.
	host, _, err := net.SplitHostPort(addrs[0])
	if err != nil {
		t.Fatal(err)
	}
	_ = host
	vipHost := vmprotocol.TuttiHost{Device: vmprotocol.SlugifyHostname("dev_peer"), Session: "claude-a"}
	ip, ok := vips.Lookup(vipHost)
	if !ok || ip[0] != 100 || ip[1] != 96 {
		t.Fatalf("vip mapping missing or outside reserved block: %v ok=%v", ip, ok)
	}

	// Bytes through the VIP listener arrive via the tunnel and echo back.
	conn, err := net.DialTimeout("tcp", addrs[0], 2*time.Second)
	if err != nil {
		t.Fatalf("dial bound listener: %v", err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := conn.Write([]byte("hello tutti")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	n, err := conn.Read(buf)
	if err != nil || string(buf[:n]) != "hello tutti" {
		t.Fatalf("echo = %q err=%v", buf[:n], err)
	}
	dialer.mu.Lock()
	defer dialer.mu.Unlock()
	if len(dialer.routes) != 1 || dialer.routes[0].SessionID != "sess-claude-a" || dialer.routes[0].Port != 3000 {
		t.Fatalf("tunneled routes %+v", dialer.routes)
	}

	// Sync drops the listener when the route disappears.
	routes.routes = nil
	if err := p.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := len(p.LiveAddrs()); got != 0 {
		t.Fatalf("listeners after route removal = %d", got)
	}
}
