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
	RoomRoutes(ctx context.Context) ([]vmprotocol.LiveRoute, error)
}

// routeTarget is one route's identity within a binding.
type routeTarget struct {
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

// routeBinding is one bound address. VIP mode binds one address per
// .tutti host and carries a single target. Shared mode (reserved block
// unavailable: plain containers, stock macOS/Windows) binds 127.0.0.1
// per PORT and carries every host of that port, demultiplexed by TLS
// SNI or the HTTP Host header.
type routeBinding struct {
	ln     net.Listener
	target *routeTarget
	shared map[string]*routeTarget
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
		listen: func(addr string) (net.Listener, error) {
			if vips.NeedsFreebind() {
				// Linux containers: claim the reserved address without
				// any interface configuration.
				return freebindListen(addr)
			}
			return net.Listen("tcp", addr)
		},
	}
}

// Sync reconciles listeners with the room's live routes: one listener per
// session address, plus one per distinct device address so short names
// work. VIP mode gives every host its own address; shared mode collapses
// hosts onto 127.0.0.1 per port. Called on ports_changed events and
// periodically.
func (p *Proxy) Sync(ctx context.Context) error {
	live, err := p.routes.RoomRoutes(ctx)
	if err != nil {
		return err
	}
	sharedMode := p.vips.Shared()
	want := map[string]*routeBinding{}
	addWant := func(host string, port int, t *routeTarget) {
		var addr string
		if sharedMode {
			// Bind exactly the interface the DNS answer points at
			// (container bridge on Linux, loopback on native runs):
			// wildcard-binding every interface would expose
			// unauthenticated room services to whatever else the host
			// is attached to.
			addr = net.JoinHostPort(probeSharedAddr().String(), fmt.Sprintf("%d", port))
		} else {
			h, err := vmprotocol.ParseTuttiHost(host)
			if err != nil {
				return // malformed .tutti host cannot own an address
			}
			ip, err := p.vips.AssignWithError(h)
			if err != nil {
				p.log.Error("tutti VIP allocation failed", "host", host, "port", port, "err", err)
				return
			}
			addr = net.JoinHostPort(ip.String(), fmt.Sprintf("%d", port))
		}
		b, ok := want[addr]
		if !ok {
			b = &routeBinding{}
			if sharedMode {
				b.shared = map[string]*routeTarget{}
			}
			want[addr] = b
		}
		if sharedMode {
			// First session target of the port doubles as the raw-TCP
			// fallback (no SNI/Host to demultiplex with).
			if _, exists := b.shared[host]; !exists {
				b.shared[host] = t
			}
			if b.target == nil {
				b.target = t
			}
			return
		}
		if b.target == nil {
			b.target = t
		}
	}
	for _, r := range live {
		if r.SessionID == "" || r.Port == 0 {
			continue
		}
		sessHost := vmprotocol.TuttiHost{Device: p.slugOf(r), Session: strings.TrimPrefix(r.SessionID, "sess-")}
		addWant(sessHost.String(), r.Port, &routeTarget{route: r.RouteKey, host: sessHost.String()})

		// Device-level short address (slug.tutti:port): re-resolve per
		// connection; ambiguity renders the H5 selector over HTTP(S).
		// Requires a lookup; without one (tests/dev) only session
		// listeners bind.
		if p.lookup != nil {
			devHost := vmprotocol.TuttiHost{Device: p.slugOf(r)}
			addWant(devHost.String(), r.Port, &routeTarget{
				route: r.RouteKey, host: devHost.String(), deviceLevel: true, deviceSlug: p.slugOf(r),
			})
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
		if cur, exists := p.listeners[addr]; exists {
			// The listener stays, but its ROUTES may have changed: in
			// shared mode a session newly advertising this port must
			// become selectable and removed hosts must stop being.
			// accept() re-reads the current binding per connection, so
			// swapping the struct is enough.
			b.ln = cur.ln
			p.listeners[addr] = b
			continue
		}
		ln, err := p.listen(addr)
		if err != nil {
			p.log.Warn("tutti vip listen failed", "addr", addr, "err", err)
			continue
		}
		b.ln = ln
		p.listeners[addr] = b
		go p.accept(addr, ln)
		p.log.Info("tutti route live", "addr", addr, "host", b.target.host, "session", b.target.route.SessionID, "port", b.target.route.Port)
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

// slugOf returns the .tutti hostname identity for a route: the
// SERVER-provided slug when the route list carries one (raw device ids
// can differ from the enrolled-hostname slugs canonical hosts use);
// otherwise derive locally (tests/dev without the slug field). "self" is
// the conventional local shortcut for the caller's own device.
func (p *Proxy) slugOf(r vmprotocol.LiveRoute) string {
	if r.DeviceSlug != "" {
		return r.DeviceSlug
	}
	if r.DeviceID == p.deviceID {
		return "self"
	}
	return vmprotocol.SlugifyHostname(r.DeviceID)
}

// accept serves one bound address for the binding's lifetime. It looks
// the CURRENT binding up per connection: Sync swaps bindings in place
// when shared-mode routes change under a live listener.
func (p *Proxy) accept(addr string, ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go func() {
			defer conn.Close()
			p.mu.Lock()
			b := p.listeners[addr]
			p.mu.Unlock()
			if b == nil {
				return
			}
			p.handle(conn, b)
		}()
	}
}

// KnownHosts reports the .tutti hostnames with a live listener binding:
// the DNS responder answers ONLY these (plus their shared-mode aliases)
// so an unregistered name cannot permanently consume a VIP allocation.
func (p *Proxy) KnownHosts() map[string]bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[string]bool, len(p.listeners))
	for _, b := range p.listeners {
		if b.target != nil {
			out[b.target.host] = true
		}
		for host := range b.shared {
			out[host] = true
		}
	}
	return out
}

// handle serves one virtual-address connection: optional TLS termination,
// connect-time device-address resolution with the H5 selector on
// ambiguity, then a tunnel pipe to the owning session. Shared-mode
// listeners demultiplex the port's hosts by TLS SNI, then by the HTTP
// Host header.
func (p *Proxy) handle(conn net.Conn, b *routeBinding) {
	defer conn.Close()
	client := &bufferedConn{Conn: conn, r: bufio.NewReader(conn)}

	// Bound protocol detection: a peer connecting and sending NOTHING
	// would block the initial Peek forever (one goroutine and fd per
	// attempt — a cheap resource-exhaustion vector on any preview
	// listener). The deadline covers the TLS sniff, handshake, and the
	// later eight-byte HTTP sniff, and is cleared once routing begins.
	if dl, ok := conn.(interface{ SetReadDeadline(time.Time) error }); ok {
		_ = dl.SetReadDeadline(time.Now().Add(15 * time.Second))
		defer func() { _ = dl.SetReadDeadline(time.Time{}) }()
	}

	// SNI seen during the handshake selects the shared-mode target.
	sniHost := ""
	// TLS ClientHello starts with 0x16; the room CA terminates it so
	// .tutti names get trusted certificates inside room runtimes.
	if head, err := client.r.Peek(1); err == nil && head[0] == 0x16 && p.ca != nil {
		cfg := &tls.Config{
			MinVersion: tls.VersionTLS12,
			GetCertificate: func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
				host := hello.ServerName
				if host == "" {
					host = b.target.host
				}
				host = strings.ToLower(host)
				// Validate BEFORE issuing: LeafFor mints AND CACHES a fresh
				// ECDSA key + certificate per name, so an unauthenticated
				// client cycling unique SNI values against a preview
				// listener drove unbounded CPU and heap growth before
				// pickShared ever checked registration. Unknown names fail
				// the handshake immediately.
				if host != "" && host != b.target.host {
					if _, aliased := b.shared[host]; !aliased {
						return nil, fmt.Errorf("unknown sni host %q", host)
					}
				}
				if host != "" {
					sniHost = host
				}
				cert, err := p.ca.LeafFor(host)
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

	t := b.target
	if b.shared != nil {
		t = p.pickShared(client, b, sniHost)
		if t == nil {
			return
		}
	}

	route := t.route
	if t.deviceLevel {
		res, err := Resolve(p.lookup, fmt.Sprintf("%s:%d", t.host, t.route.Port))
		if err != nil || res.SessionID == "" {
			// Ambiguous occupancy: HTTP(S) callers get the H5 picker
			// with the live candidates; raw TCP fails fast.
			if len(res.Candidates) > 0 && isHTTP(client.r) {
				p.serveSelector(client, res.Candidates)
				return
			}
			p.log.Warn("tutti device address unresolved", "host", t.host, "port", t.route.Port, "err", err)
			return
		}
		route.SessionID = res.SessionID
	}
	// Routing decided: CLEAR the sniff deadline before piping. The
	// deferred clear runs only AFTER pipe returns, and the SNI
	// early-return path never reaches it — an absolute 15s deadline
	// surviving into the pipe severed every VIP-mode session (browser
	// previews, terminals, websockets, long downloads) exactly 15s
	// after accept when a read crossed the mark.
	if dl, ok := client.Conn.(interface{ SetReadDeadline(time.Time) error }); ok {
		_ = dl.SetReadDeadline(time.Time{})
	}
	p.pipe(client, route)
}

// pickShared selects the shared-mode target for one connection: SNI when
// TLS carried it, else the HTTP Host header. Plain-TCP connections with
// no host identity fall back to the port's sole target, or fail when the
// port genuinely multiplexes several hosts.
func (p *Proxy) pickShared(client *bufferedConn, b *routeBinding, sniHost string) *routeTarget {
	if t, ok := b.shared[sniHost]; ok && sniHost != "" {
		return t
	}
	if host := httpHostHeader(client); host != "" {
		if t, ok := b.shared[strings.ToLower(host)]; ok {
			return t
		}
		// Port suffix in the Host header ("host:3000") is not part of
		// the registered key.
		if bare, _, ok := strings.Cut(host, ":"); ok {
			if t, found := b.shared[strings.ToLower(bare)]; found {
				return t
			}
		}
	}
	// Sole-target fallback for hostless raw TCP: count distinct ROUTES,
	// not hostnames — one advertised route registers its session host
	// AND its device-level alias, so hostname counting would reject
	// every raw connection even when it can only land on one session.
	targets := map[vmprotocol.RouteKey]*routeTarget{}
	for _, t := range b.shared {
		targets[t.route] = t
	}
	if len(targets) == 1 {
		for _, t := range targets {
			return t
		}
	}
	p.log.Warn("tutti shared listener cannot identify host (no SNI/Host header) and port serves multiple routes")
	return nil
}

// httpHostHeader peeks the HTTP request head for the Host header WITHOUT
// consuming it — the request still proxies downstream after target
// selection. The head may straddle TCP segments: Peek(n) blocks for the
// missing bytes, bounded by a read deadline (restored for the pipe).
func httpHostHeader(c *bufferedConn) string {
	r := c.r
	if !isHTTP(r) {
		return ""
	}
	c.SetReadDeadline(time.Now().Add(3 * time.Second))
	defer c.SetReadDeadline(time.Time{})
	for i := 0; i < 8192; i++ {
		buf, err := r.Peek(r.Buffered())
		if err != nil && r.Buffered() == 0 {
			return ""
		}
		head := string(buf)
		done := strings.Contains(head, "\r\n\r\n") || strings.Contains(head, "\n\n")
		if host, ok := hostLine(head); ok {
			return host
		}
		if done {
			return "" // full head buffered, no Host line
		}
		if _, err := r.Peek(r.Buffered() + 1); err != nil {
			return "" // peer closed / deadline: work with what arrived
		}
	}
	return ""
}

func hostLine(head string) (string, bool) {
	for i := 0; i < 128; i++ {
		line, rest, more := strings.Cut(head, "\n")
		head = rest
		name, val, colon := strings.Cut(strings.TrimRight(line, "\r"), ":")
		if colon && strings.EqualFold(strings.TrimSpace(name), "Host") {
			return strings.TrimSpace(val), true
		}
		if !more {
			return "", false
		}
	}
	return "", false
}

// serveSelector renders the localized H5 picker for an ambiguous device
// address; the visitor's language comes from the request headers.
func (p *Proxy) serveSelector(client *bufferedConn, candidates []vmprotocol.SessionCandidate) {
	// Terminated or direct TLS: candidate links must keep the caller's
	// scheme — forcing http:// sends plaintext to TLS backends and
	// breaks HTTPS-only sessions. Read the request head ONCE (streaming
	// past it twice would miss headers preceding the other scan).
	headers := readRequestHead(client.r)
	scheme := "http"
	if _, isTLS := client.Conn.(*tls.Conn); isTLS {
		scheme = "https"
	} else if headers["x-forwarded-proto"] == "https" {
		scheme = "https"
	}
	body := SessionSelectorPage(headers["accept-language"], scheme, candidates)
	page := "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n" +
		fmt.Sprintf("Content-Length: %d\r\nConnection: close\r\n\r\n%s", len(body), body)
	client.SetWriteDeadline(time.Now().Add(10 * time.Second))
	io.WriteString(client, page)
}

// httpAcceptLanguage drains the buffered HTTP request head (the selector
// answers instead of proxying, so consuming it is safe) and returns the
// Accept-Language value, if any.
// readRequestHead consumes the request line and headers (bounded; the
// raw pipe never materializes an http.Request) and returns lower-cased
// header values.
func readRequestHead(r *bufio.Reader) map[string]string {
	out := map[string]string{}
	for i := 0; i < 128; i++ {
		line, err := r.ReadString('\n')
		if err != nil {
			return out
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			return out
		}
		if k, v, ok := strings.Cut(line, ":"); ok {
			out[strings.ToLower(strings.TrimSpace(k))] = strings.TrimSpace(v)
		}
	}
	return out
}

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
	// BOTH legs close when EITHER copy finishes: a client that
	// disconnects while the backend stays open and quiet used to leave
	// the foreground target-to-client copy blocked forever (deferred
	// closes never ran) — one goroutine and yamux stream leaked per
	// connection. Closing both sides unblocks the surviving copy and
	// releases the stream.
	done := make(chan struct{}, 2)
	go func() {
		io.Copy(target, client)
		done <- struct{}{}
	}()
	go func() {
		io.Copy(client, target)
		done <- struct{}{}
	}()
	<-done
	client.Close()
	target.Close()
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
