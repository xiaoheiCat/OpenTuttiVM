package gateway

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
)

// routedEcho answers each connection with the route it arrived on, so a
// shared-mode test can assert the Host header demultiplexer picked the
// right session.
type routedEcho struct {
	mu    sync.Mutex
	route string
	ln    net.Listener
}

func startRoutedEcho(t *testing.T) *routedEcho {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	e := &routedEcho{ln: ln}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				e.mu.Lock()
				tag := e.route
				e.mu.Unlock()
				fmt.Fprintf(c, "%s\n", tag)
			}(conn)
		}
	}()
	return e
}

type routedDialer struct {
	echo *routedEcho
	mu   sync.Mutex
	last vmprotocol.RouteKey
}

func (d *routedDialer) Connect(route vmprotocol.RouteKey) (net.Conn, error) {
	d.mu.Lock()
	d.last = route
	d.echo.mu.Lock()
	d.echo.route = fmt.Sprintf("%s/%d", route.SessionID, route.Port)
	d.echo.mu.Unlock()
	d.mu.Unlock()
	return net.Dial("tcp", d.echo.ln.Addr().String())
}

// Shared mode (reserved block unavailable: plain containers, stock
// macOS/Windows): every host answers 127.0.0.1, one listener per PORT,
// and the HTTP Host header picks the session. Raw TCP without any host
// identity falls back to the port's sole session.
func TestProxySharedModeDemultiplexesByHostHeader(t *testing.T) {
	echo := startRoutedEcho(t)
	dialer := &routedDialer{echo: echo}
	vips := NewVIPAllocator()
	vips.mode.Store(int32(modeShared))
	routes := &staticRoutes{routes: []vmprotocol.LiveRoute{
		{RouteKey: vmprotocol.RouteKey{DeviceID: "dev_a", SessionID: "sess-claude-a", Port: 53087}, DeviceSlug: "a"},
		{RouteKey: vmprotocol.RouteKey{DeviceID: "dev_b", SessionID: "sess-codex-b", Port: 53087}, DeviceSlug: "b"},
	}}
	p := NewProxy(vips, dialer, routes, nil, nil, "room1", "dev_me", nil)
	defer p.Close()
	if err := p.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	addrs := p.LiveAddrs()
	if len(addrs) != 1 {
		t.Fatalf("shared mode must bind ONE listener per port, got %+v", addrs)
	}

	ask := func(host string) string {
		t.Helper()
		conn, err := net.DialTimeout("tcp", addrs[0], 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(3 * time.Second))
		fmt.Fprintf(conn, "GET / HTTP/1.1\r\nHost: %s\r\n\r\n", host)
		line, _, err := bufio.NewReader(conn).ReadLine()
		if err != nil {
			t.Fatal(err)
		}
		return string(line)
	}
	if got := ask("claude-a.a.tutti:53087"); got != "sess-claude-a/53087" {
		t.Fatalf("host a routed to %s", got)
	}
	if got := ask("codex-b.b.tutti:53087"); got != "sess-codex-b/53087" {
		t.Fatalf("host b routed to %s", got)
	}

	// Raw TCP carries no Host identity; the sole-session fallback does
	// not apply here (two hosts), so the connection must close without
	// piping to a wrong session.
	conn, err := net.DialTimeout("tcp", addrs[0], 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write([]byte("raw bytes")); err != nil {
		t.Fatal(err)
	}
	if _, err := bufio.NewReader(conn).ReadString('\n'); err == nil {
		t.Fatal("raw TCP on a multi-host port must not reach a session")
	}
}
