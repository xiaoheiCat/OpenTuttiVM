// open-tutti-room-sync is the per-room local engine that runs inside the
// room-sync container of an open-tutti-vm-<roomId> Docker project: it keeps
// the workspace replica in the open-tutti-vm-<roomId>-workspace volume,
// bridges agent-session FUSE mounts onto the same logical workspace, and
// terminates the .tutti virtual network for this device — DNS answers,
// VIP listeners, room-CA TLS, the H5 session selector, and both tunnel
// legs (outbound dials and inbound session forwards).
//
// Environment:
//
//	OPEN_TUTTI_SERVER         server base URL (required)
//	OPEN_TUTTI_TOKEN          room session token (required)
//	OPEN_TUTTI_DEVICE_ID      this device's id (required)
//	OPEN_TUTTI_POLICY         replica policy: lazy (default) | full
//	OPEN_TUTTI_CACHE_DIR      CAS cache dir (default /data/cache)
//	OPEN_TUTTI_FS_LISTEN      room FS protocol listen address
//	                         (default unix /run/open-tutti/roomfs.sock)
//	OPEN_TUTTI_DNS_LISTEN     .tutti DNS responder bind (default :1053)
//	OPEN_TUTTI_CA_DIR         where the room CA cert is exported for
//	                         runtime injection (default /data/ca)
//	OPEN_TUTTI_SESSION_DIAL   room-network address template for session
//	                         containers (default "agent-%s" + route port)
//	OPEN_TUTTI_SESSION_LABEL  this session's registry label
//	                         (default "main"; id is "sess-"+label)
//	OPEN_TUTTI_SESSION_PORTS  comma list of session ports to advertise,
//	                         e.g. "3000:http,5432:tcp" (default none)
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	vmcas "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-cas"
	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
	vmsync "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-sync"

	roomfs "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-roomfs"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-room-sync/internal/borrowhost"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-room-sync/internal/client"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-room-sync/internal/gateway"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-room-sync/internal/replica"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-room-sync/internal/roomfsbridge"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-room-sync/internal/tunneldial"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "open-tutti-room-sync:", err)
		os.Exit(1)
	}
}

func run() error {
	serverURL := os.Getenv("OPEN_TUTTI_SERVER")
	token := os.Getenv("OPEN_TUTTI_TOKEN")
	deviceID := os.Getenv("OPEN_TUTTI_DEVICE_ID")
	if serverURL == "" || token == "" || deviceID == "" {
		return fmt.Errorf("OPEN_TUTTI_SERVER, OPEN_TUTTI_TOKEN, and OPEN_TUTTI_DEVICE_ID are required")
	}
	policy := replica.Policy(os.Getenv("OPEN_TUTTI_POLICY"))
	if policy != replica.Full {
		policy = replica.Lazy
	}
	cacheDir := os.Getenv("OPEN_TUTTI_CACHE_DIR")
	if cacheDir == "" {
		cacheDir = "/data/cache"
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cache, err := vmcas.NewLocalStore(cacheDir)
	if err != nil {
		return err
	}
	c := client.New(client.Server{BaseURL: serverURL}, deviceID)
	// The token arrives via the container environment; adopt it for
	// authenticated calls.
	if err := c.AdoptToken(token); err != nil {
		return err
	}

	snap, ops, err := c.Bootstrap(ctx)
	if err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}
	mgr := replica.New(deviceID, cache, policy, c)
	if err := mgr.Bootstrap(ctx, snap, ops); err != nil {
		return fmt.Errorf("replica bootstrap: %w", err)
	}

	ca, err := gateway.NewLocalCA()
	if err != nil {
		return err
	}
	vips := gateway.NewVIPAllocator()

	// The room CA cert is exported for the runtime to inject into the
	// Tutti Browser and session containers (never the host OS store).
	caDir := os.Getenv("OPEN_TUTTI_CA_DIR")
	if caDir == "" {
		caDir = "/data/ca"
	}
	if err := os.MkdirAll(caDir, 0o700); err == nil {
		if err := os.WriteFile(filepath.Join(caDir, "room-ca.pem"), ca.CACertPEM(), 0o600); err != nil {
			fmt.Fprintf(os.Stderr, "room-sync: write ca: %v\n", err)
		}
	}

	// .tutti DNS: containers in the room network point their resolver at
	// this UDP socket and every virtual name resolves onto its VIP.
	dnsAddr := os.Getenv("OPEN_TUTTI_DNS_LISTEN")
	if dnsAddr == "" {
		dnsAddr = ":1053"
	}
	dns := gateway.NewDNSServer(vips)
	go func() {
		if err := dns.ListenAndServe(dnsAddr); err != nil {
			fmt.Fprintf(os.Stderr, "room-sync: dns %s: %v\n", dnsAddr, err)
		}
	}()

	// The session and tunnel are hot-swapped across reconnects; the bridge
	// and proxy hold these stable references.
	sessionRef := &liveSession{}
	tunnelRef := &liveTunnel{}

	// Cross-device streams ride the yamux tunnel; the .tutti proxy binds a
	// synthetic-VIP listener per live room route, TLS-terminates with the
	// room CA, and pipes through the relay.
	proxy := gateway.NewProxy(vips, tunnelRef, c, c, ca, c.RoomID(), deviceID,
		slog.New(slog.NewTextHandler(os.Stderr, nil)))
	defer proxy.Close()

	// Host the Room FS Protocol for agent-session mounts (open-tutti-fs).
	roomfsAddr := os.Getenv("OPEN_TUTTI_FS_LISTEN")
	if roomfsAddr == "" {
		roomfsAddr = "/run/open-tutti/roomfs.sock"
	}
	if dir := filepath.Dir(roomfsAddr); !strings.Contains(roomfsAddr, ":") {
		os.MkdirAll(dir, 0o755)
	}
	roomfsSrv := roomfs.NewServer(nil, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	// Borrowing host: the owning device's execution adapter. Room-sync
	// never runs agent code; hosts inject a real adapter (Agent Host
	// delegation) and Noop keeps routed events observable until then.
	borrowHost := borrowhost.Host(&borrowhost.Noop{
		Log: slog.New(slog.NewTextHandler(os.Stderr, nil)),
	})
	// The bridge submits under the same session id the port announcements
	// use ("sess-"+label): barrier resolver assignment compares against
	// AgentSessionID, and a mismatched id would never lift a fence.
	sessionLabel := os.Getenv("OPEN_TUTTI_SESSION_LABEL")
	if sessionLabel == "" {
		sessionLabel = "main"
	}
	bridge := roomfsbridge.New(mgr, sessionRef, c, deviceID, "sess-"+sessionLabel, roomfsSrv.BroadcastInvalidate)
	bridge.SetResolver(sessionRef)
	roomfsSrv.SetHandler(bridge)
	ln, err := listenRoomFS(roomfsAddr)
	if err != nil {
		return err
	}
	defer ln.Close()
	go roomfsSrv.Serve(ln)
	fmt.Fprintf(os.Stderr, "room-fs listening on %s\n", roomfsAddr)

	if err := proxy.Sync(ctx); err != nil {
		return fmt.Errorf("gateway initial sync: %w", err)
	}
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := proxy.Sync(ctx); err != nil {
					fmt.Fprintf(os.Stderr, "room-sync: gateway sync: %v\n", err)
				}
			}
		}
	}()

	fmt.Fprintf(os.Stderr, "room-sync ready: device=%s policy=%s paths=%d vip-block=%s\n",
		deviceID, policy, len(mgr.Replica.State.Paths()), gateway.ReservedBlock)

	// Session loop: transient socket failures reconnect with backoff and
	// re-bootstrap the replica instead of taking the mount offline.
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		sess, err := c.Dial(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "room-sync: dial: %v\n", err)
			if !sleepCtx(ctx, backoff) {
				return ctx.Err()
			}
			backoff = minDuration(backoff*2, 30*time.Second)
			continue
		}
		backoff = time.Second
		sess.OnGap = func() error {
			snap, ops, err := c.Bootstrap(ctx)
			if err != nil {
				return err
			}
			return mgr.Bootstrap(ctx, snap, ops)
		}
		sess.OnEvent = func(ev vmprotocol.Event) {
			switch ev.Topic {
			case vmprotocol.TopicPortsChanged, vmprotocol.TopicRoomEnding:
				if err := proxy.Sync(ctx); err != nil {
					fmt.Fprintf(os.Stderr, "room-sync: gateway sync: %v\n", err)
				}
			case vmprotocol.TopicOperation:
				// A remote participant's operation applied locally:
				// connected mounts must drop cached attributes/content.
				var env vmprotocol.Envelope
				if json.Unmarshal(ev.Payload, &env) == nil {
					bridge.InvalidateRemote(env.Operation.Path)
					// Renames must drop BOTH sides: the source entry
					// disappears, the destination appears (a cached
					// negative lookup there would keep hiding it), and
					// a moved directory takes its descendants along.
					if r := env.Operation.Rename; env.Operation.Kind == vmprotocol.OpRename && r != nil {
						bridge.InvalidateRemote(r.NewPath)
						var moved []string
						mgr.WithState(func(state *vmsync.WorkspaceState) {
							for _, p := range state.Paths() {
								if strings.HasPrefix(p, r.NewPath+"/") {
									moved = append(moved, p)
								}
							}
						})
						for _, p := range moved {
							bridge.InvalidateRemote(p)
						}
					}
				}
			case vmprotocol.TopicOperationRejected:
				var rej vmprotocol.RejectionPayload
				if json.Unmarshal(ev.Payload, &rej) == nil {
					mgr.NotifyRejected(rej.OperationID, rej.Reason)
				}
			case vmprotocol.TopicConflictDetected:
				// A semantic conflict fenced a path; when this
				// session is the assigned resolver, the next
				// accepted fix on the path lifts the barrier.
				var cp vmprotocol.ConflictPayload
				if json.Unmarshal(ev.Payload, &cp) == nil {
					bridge.OnConflictDetected(cp)
				}
			case vmprotocol.TopicAgentShared:
				var p vmprotocol.AgentSharedPayload
				if json.Unmarshal(ev.Payload, &p) == nil {
					borrowHost.Shared(p)
				}
			case vmprotocol.TopicBorrowCommand:
				// A borrower's instruction routed to this owning
				// device: execution belongs to the host's agent
				// runtime, never to room-sync itself.
				var p vmprotocol.BorrowCommandPayload
				if json.Unmarshal(ev.Payload, &p) == nil {
					borrowHost.ExecuteCommand(p)
				}
			case vmprotocol.TopicBorrowRevoked:
				var p vmprotocol.BorrowRevokedPayload
				if json.Unmarshal(ev.Payload, &p) == nil {
					borrowHost.Revoked(p)
				}
			case vmprotocol.TopicApprovalRequest:
				var p vmprotocol.ApprovalRequestPayload
				if json.Unmarshal(ev.Payload, &p) == nil {
					borrowHost.ApprovalRequest(p)
				}
			case vmprotocol.TopicApprovalDecision:
				var p vmprotocol.ApprovalDecisionPayload
				if json.Unmarshal(ev.Payload, &p) == nil {
					borrowHost.ApprovalDecision(p)
				}
			}
		}
		sessionRef.set(sess)
		// Announce this device's session ports so the preview registry
		// (and through it /routes, the selector, and relay
		// authorization) knows what the session serves. The room
		// compose injects the list; without it the virtual network has
		// nothing to route.
		if err := announceSessionPorts(sess, deviceID); err != nil {
			fmt.Fprintf(os.Stderr, "room-sync: announce ports: %v\n", err)
		}
		tun, err := tunneldial.Dial(ctx, serverURL, token)
		if err != nil {
			fmt.Fprintf(os.Stderr, "room-sync: tunnel dial: %v\n", err)
		} else {
			tunnelRef.set(tun)
			// Inbound leg: another device connected to one of this
			// device's advertised routes; forward each relayed stream
			// to the owning session container on the room network.
			go serveTunnel(ctx, tun, c.RoomID(), deviceID)
		}
		runErr := sess.Run(mgr)
		sess.Close()
		sessionRef.clear(sess)
		// Tear this iteration's tunnel down before reconnecting: a
		// leaked tunnel keeps a yamux session alive, and its
		// server-side unregister would later delete the replacement
		// tunnel's relay entry, stranding advertised routes.
		if tun != nil {
			tunnelRef.clearLocked(tun)
			tun.Close()
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		fmt.Fprintf(os.Stderr, "room-sync: session lost (%v); reconnecting\n", runErr)
		// Reconnect path: resync the replica from a fresh bootstrap.
		for {
			snap, ops, err := c.Bootstrap(ctx)
			if err == nil {
				if err := mgr.Bootstrap(ctx, snap, ops); err == nil {
					break
				}
			}
			if !sleepCtx(ctx, backoff) {
				return ctx.Err()
			}
			backoff = minDuration(backoff*2, 30*time.Second)
		}
		backoff = time.Second
	}
}

// announceSessionPorts publishes OPEN_TUTTI_SESSION_PORTS (e.g.
// "3000:http,5432:tcp") under the OPEN_TUTTI_SESSION_LABEL (default
// "main"). The registry id is "sess-"+label — the same convention the
// gateway and room FS bridge use.
func announceSessionPorts(sess *client.Session, deviceID string) error {
	label := os.Getenv("OPEN_TUTTI_SESSION_LABEL")
	if label == "" {
		label = "main"
	}
	sess.SessionLabel = label
	spec := os.Getenv("OPEN_TUTTI_SESSION_PORTS")
	if spec == "" {
		return nil
	}
	for _, entry := range strings.Split(spec, ",") {
		entry = strings.TrimSpace(entry)
		portProto := strings.SplitN(entry, ":", 2)
		port, err := strconv.Atoi(strings.TrimSpace(portProto[0]))
		if err != nil || port <= 0 || port > 65535 {
			return fmt.Errorf("invalid session port %q", entry)
		}
		proto := "http"
		if len(portProto) == 2 && strings.TrimSpace(portProto[1]) != "" {
			proto = strings.ToLower(strings.TrimSpace(portProto[1]))
		}
		if err := sess.AnnouncePorts(vmprotocol.PortsChangedPayload{
			DeviceID: deviceID, SessionID: "sess-" + label, SessionLabel: label,
			Port: port, Protocol: proto, Listening: true,
		}); err != nil {
			return err
		}
	}
	return nil
}

// sessionDialTemplate formats the room-network address of a session
// container from its session id; the room compose names agent services
// agent-<session>.
var sessionDialTemplate = func() string {
	if t := os.Getenv("OPEN_TUTTI_SESSION_DIAL"); t != "" {
		return t
	}
	return "agent-%s"
}()

// serveTunnel consumes inbound relayed streams for this device and
// forwards each to the requested local session port.
func serveTunnel(ctx context.Context, tun *tunneldial.Tunnel, roomID, deviceID string) {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	for {
		stream, header, err := tun.Accept()
		if err != nil {
			return
		}
		go func(stream net.Conn, header *vmprotocol.TunnelHeader) {
			defer stream.Close()
			if header == nil || header.Action != vmprotocol.TunnelConnect {
				return
			}
			// Routes are room-scoped by the relay; a stray cross-room
			// header is dropped here regardless.
			if header.Route.RoomID != "" && header.Route.RoomID != roomID {
				log.Warn("tutti inbound stream for foreign room", "room", header.Route.RoomID)
				return
			}
			if header.Route.SessionID == "" || header.Route.Port == 0 {
				return
			}
			addr := fmt.Sprintf(sessionDialTemplate+":"+fmt.Sprint(header.Route.Port), header.Route.SessionID)
			d := net.Dialer{Timeout: 5 * time.Second}
			local, err := d.DialContext(ctx, "tcp", addr)
			if err != nil {
				log.Warn("tutti inbound dial failed", "addr", addr, "err", err)
				return
			}
			defer local.Close()
			go io.Copy(local, stream)
			io.Copy(stream, local)
		}(stream, header)
	}
}

// liveSession is the hot-swappable WS session reference held by the Room
// FS bridge across reconnects.
type liveSession struct {
	mu sync.Mutex
	s  *client.Session
}

func (l *liveSession) set(s *client.Session) { l.mu.Lock(); l.s = s; l.mu.Unlock() }
func (l *liveSession) clear(s *client.Session) {
	l.mu.Lock()
	if l.s == s {
		l.s = nil
	}
	l.mu.Unlock()
}

// ResolveBarrier lifts a conflict barrier through the live session.
func (l *liveSession) ResolveBarrier(path string) error {
	l.mu.Lock()
	s := l.s
	l.mu.Unlock()
	if s == nil {
		return fmt.Errorf("room socket reconnecting")
	}
	return s.ResolveBarrier(path)
}

func (l *liveSession) Submit(env vmprotocol.Envelope) error {
	l.mu.Lock()
	s := l.s
	l.mu.Unlock()
	if s == nil {
		return fmt.Errorf("room socket reconnecting")
	}
	return s.Submit(env)
}

// liveTunnel is the hot-swappable tunnel dialer held by the .tutti proxy.
type liveTunnel struct {
	mu sync.Mutex
	t  *tunneldial.Tunnel
}

func (l *liveTunnel) set(t *tunneldial.Tunnel) { l.mu.Lock(); l.t = t; l.mu.Unlock() }

// clearLocked drops the reference only if it still points at t, so
// closing a stale iteration's tunnel cannot unregister a newer one.
func (l *liveTunnel) clearLocked(t *tunneldial.Tunnel) {
	l.mu.Lock()
	if l.t == t {
		l.t = nil
	}
	l.mu.Unlock()
}

func (l *liveTunnel) Connect(route vmprotocol.RouteKey) (net.Conn, error) {
	l.mu.Lock()
	t := l.t
	l.mu.Unlock()
	if t == nil {
		return nil, fmt.Errorf("tunnel reconnecting")
	}
	return t.Connect(route)
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// listenRoomFS binds the unix socket (or TCP) for mounts.
func listenRoomFS(addr string) (net.Listener, error) {
	if strings.Contains(addr, ":") {
		return net.Listen("tcp", addr)
	}
	os.Remove(addr)
	return net.Listen("unix", addr)
}
