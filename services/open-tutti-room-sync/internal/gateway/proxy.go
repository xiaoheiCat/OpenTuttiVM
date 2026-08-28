// The .tutti interception surface: one listener per live route on its
// synthetic VIP, TLS-terminated by the room CA, piping every connection
// through the server relay. Device-level short addresses resolve at
// connect time and render the H5 session selector when ambiguous.
package gateway

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

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

// routeBinding is one bound virtual address.
type routeBinding struct {
	ln net.Listener
	// route is the session-level target for session bindings, and the
	// device/port template for device-level bindings.
	route vmprotocol.RouteKey
	// host is the full .tutti hostname backing the listener (leaf cert
	// fallback when the client sends no SNI).
	host string
	// deviceLevel bindings re-resolve on every connection: occupancy may
	// change while the listener lives.
	deviceLevel bool
	deviceSlug  string
}

// Proxy owns the .tutti virtual-network listeners.
type Proxy struct {
	vips     *VIPAllocator
	dialer   TunneledDialer
	routes   RouteSource
	lookup   RouteLookup
	ca       *LocalCA
	log      *slog.Logger
	roomID   string
	deviceID string

	mu        sync.Mutex
	listeners map[string]*routeBinding

	// listen is injectable: dev machines may not own the synthetic block,
	// while container networks route it natively.
	listen func(addr string) (net.Listener, error)
}

// NewProxy wires the proxy. The allocator must be the process-wide one so
// hostname→VIP mappings stay stable; ca and lookup enable TLS termination
// and connect-time device-address resolution (both optional for tests).
func NewProxy(vips *VIPAllocator, dialer TunneledDialer, routes RouteSource, lookup RouteLookup, ca *LocalCA, roomID, deviceID string, log *slog.Logger) *Proxy {
	if log == nil {
		log = slog.Default()
	}
	return &Proxy{
		vips: vips, dialer: dialer, routes: routes, lookup: lookup, ca: ca,
		roomID: roomID, deviceID: deviceID, log: log,
		listeners: map[string]*routeBinding{},
		listen:    func(addr string) (net.Listener, error) { return net.Listen("tcp", addr) },
	}
}

// Sync reconciles listeners with the room's live routes: one listener per
// session address, plus one per distinct device address so short names
// work. Called on ports_changed events and periodically.
func (p *Proxy) Sync(ctx context.Context) error {
	live, err := p.routes.RoomRoutes(ctx)
	if err != nil {
		return err
	}
	want := map[string]*routeBinding{}
	for _, r := range live {
		if r.SessionID == "" || r.Port == 0 {
			continue
		}
		sessHost := vmprotocol.TuttiHost{Device: p.slugOf(r), Session: strings.TrimPrefix(r.SessionID, "sess-")}
		b := &routeBinding{
			route: r,
			host:  sessHost.String(),
		}
		want[net.JoinHostPort(p.vips.Assign(sessHost).String(), fmt.Sprintf("%d", r.Port))] = b

		// Device-level short address (slug.tutti:port): re-resolve per
		// connection; ambiguity renders the H5 selector over HTTP(S).
		// Requires a lookup; without one (tests/dev) only session
		// listeners bind.
		if p.lookup != nil {
			devHost := vmprotocol.TuttiHost{Device: p.slugOf(r)}
			addr := net.JoinHostPort(p.vips.Assign(devHost).String(), fmt.Sprintf("%d", r.Port))
			if _, taken := want[addr]; !taken {
				want[addr] = &routeBinding{
					route: vmprotocol.RouteKey{RoomID: r.RoomID, DeviceID: r.DeviceID, Port: r.Port},
					host:  devHost.String(), deviceLevel: true, deviceSlug: p.slugOf(r),
				}
			}
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	for addr, b := range p.listeners {
		if _, ok := want[addr]; !ok {
			b.ln.Close()
			delete(p.listeners, addr)
		}
	}
	for addr, b := range want {
		if _, exists := p.listeners[addr]; exists {
			continue
		}
		ln, err := p.listen(addr)
		if err != nil {
			p.log.Warn("tutti vip listen failed", "addr", addr, "err", err)
			continue
		}
		b.ln = ln
		p.listeners[addr] = b
		go p.accept(b)
		p.log.Info("tutti route live", "addr", addr, "host", b.host, "session", b.route.SessionID, "port", b.route.Port)
	}
	return nil
}

// Close drops every listener (room teardown).
func (p *Proxy) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for addr, b := range p.listeners {
		b.ln.Close()
		delete(p.listeners, addr)
	}
}

// LiveAddrs exposes the actually-bound listener addresses (status/tests).
func (p *Proxy) LiveAddrs() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, 0, len(p.listeners))
	for _, b := range p.listeners {
		out = append(out, b.ln.Addr().String())
	}
	return out
}

func (p *Proxy) slugOf(r vmprotocol.RouteKey) string {
	if r.DeviceID == p.deviceID {
		return "self"
	}
	return vmprotocol.SlugifyHostname(r.DeviceID)
}

func (p *Proxy) accept(b *routeBinding) {
	for {
		conn, err := b.ln.Accept()
		if err != nil {
			return
		}
		go p.handle(conn, b)
	}
}

// handle serves one virtual-address connection: optional TLS termination,
// connect-time device-address resolution with the H5 selector on
// ambiguity, then a tunnel pipe to the owning session.
func (p *Proxy) handle(conn net.Conn, b *routeBinding) {
	defer conn.Close()
	client := &bufferedConn{Conn: conn, r: bufio.NewReader(conn)}

	// TLS ClientHello starts with 0x16; the room CA terminates it so
	// .tutti names get trusted certificates inside room runtimes.
	if head, err := client.r.Peek(1); err == nil && head[0] == 0x16 && p.ca != nil {
		cfg := &tls.Config{
			MinVersion: tls.VersionTLS12,
			GetCertificate: func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
				host := hello.ServerName
				if host == "" {
					host = b.host
				}
				cert, err := p.ca.LeafFor(strings.ToLower(host))
				if err != nil {
					return nil, err
				}
				return &cert, nil
			},
		}
		tlsConn := tls.Server(client, cfg)
		handshake := make(chan error, 1)
		go func() { handshake <- tlsConn.Handshake() }()
		select {
		case err := <-handshake:
			if err != nil {
				return
			}
		case <-time.After(10 * time.Second):
			tlsConn.Close()
			return
		}
		client = &bufferedConn{Conn: tlsConn, r: bufio.NewReader(tlsConn)}
	}

	route := b.route
	if b.deviceLevel {
		res, err := Resolve(p.lookup, fmt.Sprintf("%s:%d", b.host, b.route.Port))
		if err != nil || res.SessionID == "" {
			// Ambiguous occupancy: HTTP(S) callers get the H5 picker
			// with the live candidates; raw TCP fails fast.
			if len(res.Candidates) > 0 && isHTTP(client.r) {
				p.serveSelector(client, res.Candidates)
				return
			}
			p.log.Warn("tutti device address unresolved", "host", b.host, "port", b.route.Port, "err", err)
			return
		}
		route.SessionID = res.SessionID
	}
	p.pipe(client, route)
}

// serveSelector renders the localized H5 picker for an ambiguous device
// address; the visitor's language comes from the request headers.
func (p *Proxy) serveSelector(client *bufferedConn, candidates []vmprotocol.SessionCandidate) {
	body := SessionSelectorPage(httpAcceptLanguage(client.r), candidates)
	page := "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n" +
		fmt.Sprintf("Content-Length: %d\r\nConnection: close\r\n\r\n%s", len(body), body)
	client.SetWriteDeadline(time.Now().Add(10 * time.Second))
	io.WriteString(client, page)
}

// httpAcceptLanguage drains the buffered HTTP request head (the selector
// answers instead of proxying, so consuming it is safe) and returns the
// Accept-Language value, if any.
func httpAcceptLanguage(r *bufio.Reader) string {
	for i := 0; i < 128; i++ { // bounded: never spin on a hostile head
		line, err := r.ReadString('\n')
		if err != nil {
			return ""
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			return "" // end of headers
		}
		name, val, ok := strings.Cut(line, ":")
		if ok && strings.EqualFold(strings.TrimSpace(name), "Accept-Language") {
			return strings.TrimSpace(val)
		}
	}
	return ""
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

// isHTTP reports whether the buffered stream starts with an HTTP method.
func isHTTP(r *bufio.Reader) bool {
	head, err := r.Peek(8)
	if err != nil {
		return false
	}
	for _, m := range [...]string{"GET ", "HEAD", "POST", "PUT ", "DELE", "OPTI", "PATC"} {
		if strings.HasPrefix(string(head), m) {
			return true
		}
	}
	return false
}

// bufferedConn replays bytes already read from the underlying connection.
type bufferedConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) { return c.r.Read(p) }
