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
//	OPEN_TUTTI_SEED_DIR       owner-only: seed an EMPTY room by
//	                         submitting this directory tree through the
//	                         normal OT path at startup (optional)
//	OPEN_TUTTI_CACHE_DIR      CAS cache dir (default /data/cache on
//	                         Linux, per-user cache dir on Windows)
//	OPEN_TUTTI_FS_LISTEN      room FS protocol listen address (default
//	                         unix /run/open-tutti/roomfs.sock; Windows
//	                         uses loopback TCP 127.0.0.1:5266)
//	OPEN_TUTTI_DNS_LISTEN     .tutti DNS responder bind (default :1053)
//	OPEN_TUTTI_CA_DIR         where the room CA key+cert persist across
//	                         restarts (default /data/ca on Linux,
//	                         per-user on Windows); the cert is exported
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
	"io/fs"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	borrowagent "github.com/xiaoheiCat/OpenTuttiVM/packages/agent/borrow"
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
	seedDir := os.Getenv("OPEN_TUTTI_SEED_DIR")
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
	seeded := false
	isOwner := false
	cacheDir := os.Getenv("OPEN_TUTTI_CACHE_DIR")
	if cacheDir == "" {
		cacheDir = platformCacheDir()
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

	boot, err := c.Bootstrap(ctx)
	if err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}
	// The room OWNER keeps a full replica (owner-survival contract):
	// nothing else sets OPEN_TUTTI_POLICY for owners, and a lazy owner
	// would never fetch untouched snapshot blobs — after a server
	// failure the final workspace would be unrecoverable. An explicit
	// OPEN_TUTTI_POLICY still wins (operators can force lazy owners in
	// throwaway rooms).
	isOwner = boot.OwnerDeviceID == deviceID
	if os.Getenv("OPEN_TUTTI_POLICY") == "" && isOwner {
		policy = replica.Full
	}
	mgr := replica.New(deviceID, cache, policy, c)
	// Apply-to-Workspace lifecycle hook: the server requires owners to
	// assert workspace_applied before leaving, and this process owns the
	// authoritative replica on the owner device — the assertion must run
	// the real final mirror here, not be submitted as an unverified
	// boolean by some other client. Two triggers: SIGUSR1 (POSIX) and
	// the one-shot OPEN_TUTTI_APPLY_AND_LEAVE=1 bootstrap mode (Windows
	// has no SIGUSR1).
	applyOnce := func() error {
		wsDir := os.Getenv("OPEN_TUTTI_WORKSPACE_DIR")
		if wsDir == "" {
			return fmt.Errorf("apply-and-leave: OPEN_TUTTI_WORKSPACE_DIR not set")
		}
		// The leave fence needs the sequence the mirror captured at;
		// an edit landing after it makes the server reject the leave
		// (its only authoritative copy must not vanish at disband), so
		// the whole capture-mirror-leave cycle retries.
		const attempts = 3
		for i := 0; i < attempts; i++ {
			baseSeq := mgr.Replica.AppliedSeq
			if err := mgr.ApplyToWorkspace(context.Background(), wsDir); err != nil {
				return fmt.Errorf("apply-to-workspace: %w", err)
			}
			disband := os.Getenv("OPEN_TUTTI_DISBAND") == "1"
			if err := c.Leave(context.Background(), true, disband, baseSeq); err != nil {
				if i < attempts-1 && strings.Contains(err.Error(), "workspace changed since apply") {
					fmt.Fprintln(os.Stderr, "room-sync: workspace changed during apply; retrying")
					continue
				}
				return fmt.Errorf("owner leave: %w", err)
			}
			fmt.Fprintln(os.Stderr, "room-sync: workspace applied; owner left")
			return nil
		}
		return fmt.Errorf("apply-and-leave: workspace kept changing across %d attempts", attempts)
	}
	if err := mgr.Bootstrap(ctx, boot.Snapshot, boot.Ops); err != nil {
		return fmt.Errorf("replica bootstrap: %w", err)
	}
	// Register the destructive hook only AFTER bootstrap completed: a
	// SIGUSR1 racing the initial materialization would mirror the
	// manager's still-empty state, prune the host workspace, and leave
	// asserting an apply that never happened.
	registerApplySignal(func() {
		if err := applyOnce(); err != nil {
			fmt.Fprintf(os.Stderr, "room-sync: %v\n", err)
		}
	})
	// One-shot mode propagates failures: exiting 0 after a transient
	// error would strand an active, unapplied room with no signal to
	// the launcher that anything needs retrying.
	if os.Getenv("OPEN_TUTTI_APPLY_AND_LEAVE") == "1" {
		return applyOnce()
	}

	// The room CA persists in device-private storage and reloads across
	// restarts: a regenerated CA would invalidate every issued .tutti
	// certificate and break consumers still trusting the old bundle.
	// The exported cert is injected into the Tutti Browser and session
	// containers (never the host OS store).
	caDir := os.Getenv("OPEN_TUTTI_CA_DIR")
	if caDir == "" {
		caDir = platformCADir()
	}
	ca, err := gateway.LoadOrCreateLocalCA(caDir)
	if err != nil {
		return err
	}
	vips := gateway.NewVIPAllocator()
	// Decide the addressing mode BEFORE any assignment: inside the room
	// VM image the 100.96/12 block is configured and real VIPs bind; on
	// plain containers or Windows runtimes it is not, and listeners
	// would fail with EADDRNOTAVAIL — fall back to 127.127/16.
	vips.Probe()

	// .tutti DNS: containers in the room network point their resolver at
	// this UDP socket and every virtual name resolves onto its VIP.
	dnsAddr := os.Getenv("OPEN_TUTTI_DNS_LISTEN")
	if dnsAddr == "" {
		dnsAddr = ":1053"
	}
	dns := gateway.NewDNSServer(vips)
	// Bind SYNCHRONOUSLY: session containers resolve .tutti names
	// through this socket, and a background bind failure previously
	// left "room-sync ready" printed with every virtual-network lookup
	// broken.
	dnsConn, err := dns.Bind(dnsAddr)
	if err != nil {
		return fmt.Errorf("dns: %w", err)
	}
	go func() {
		if err := dns.Serve(dnsConn); err != nil {
			fmt.Fprintf(os.Stderr, "room-sync: dns serve: %v\n", err)
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
		roomfsAddr = platformFSListen()
	}
	if dir := filepath.Dir(roomfsAddr); !strings.Contains(roomfsAddr, ":") {
		os.MkdirAll(dir, 0o755)
	}
	roomfsSrv := roomfs.NewServer(nil, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	// Borrowing host: the owning device's execution adapter. Room-sync
	// never runs agent code; the host application (Agent Host
	// integration) injects a real adapter. Until one exists this build
	// OBSERVES routed events without executing them, and sharing stays
	// disabled by default — room-sync itself never sends agent_share,
	// so nothing advertises an agent as borrowable that cannot execute.
	fmt.Fprintln(os.Stderr, "room-sync: borrowing execution DISABLED (no host adapter); routed borrow events are observed and logged only")
	borrowHost := borrowhost.Host(&borrowhost.Noop{
		Log: slog.New(slog.NewTextHandler(os.Stderr, nil)),
	})
	// The bridge submits under the same session id the port announcements
	// use ("sess-"+label): barrier resolver assignment compares against
	// AgentSessionID, and a mismatched id would never lift a fence.
	sessionLabel := vmprotocol.SlugifyLabel(os.Getenv("OPEN_TUTTI_SESSION_LABEL"))
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

	// resyncFromServer bootstraps the replica from a fresh checkpoint and
	// drops every cached mount entry: operations absorbed through
	// reconnect or sequence-gap resync never crossed the live event path,
	// so connected Room FS mounts would keep serving pre-disconnect
	// buffers indefinitely (and a later flush could overwrite the freshly
	// bootstrapped authoritative content). A conservative whole-tree
	// invalidation is the only correct granularity — the bootstrap may
	// have moved anything.
	resyncFromServer := func(ctx context.Context) error {
		boot, err := c.Bootstrap(ctx)
		if err != nil {
			return err
		}
		if err := mgr.Bootstrap(ctx, boot.Snapshot, boot.Ops); err != nil {
			return err
		}
		// Ownership reconciliation on EVERY bootstrap: succession may
		// have promoted THIS device while its socket could not receive
		// events, and the missed live IsOwner broadcast would leave the
		// new owner lazy (stale policy report, no full copy to survive
		// server loss).
		if boot.OwnerDeviceID == deviceID && mgr.Policy != replica.Full {
			if err := mgr.PromoteToFull(ctx); err != nil {
				fmt.Fprintf(os.Stderr, "room-sync: bootstrap owner promotion: %v\n", err)
			} else if sess := sessionRef.get(); sess != nil {
				if err := sess.ReportPolicy("full"); err != nil {
					fmt.Fprintf(os.Stderr, "room-sync: report promoted policy: %v\n", err)
				}
			}
		}
		bridge.InvalidateRemote("")
		return nil
	}
	// resubmitPending re-sends operations still awaiting acknowledgement.
	// Same operation id means the server's at-least-once dedup returns the
	// ORIGINAL acknowledgement (whose sequence is at-or-below the restored
	// state): the replica recognizes it and clears the pending entry. When
	// the operation never reached the server, this delivers it for real.
	resubmitPending := func(sess *client.Session) {
		for _, env := range mgr.PendingEnvelopes() {
			if err := sess.Submit(env); err != nil {
				fmt.Fprintf(os.Stderr, "room-sync: pending resubmit %s: %v\n", env.OperationID, err)
				return // socket is dying; the next session retries
			}
		}
	}

	// Tunnel loop: the relay tunnel has its OWN lifecycle. A transient
	// tunnel handshake failure while the business WebSocket stays healthy
	// must not strand every .tutti proxy connection and advertised route
	// until an unrelated socket drop — so it retries independently of
	// the session loop, with backoff, until ctx ends.
	go tunnelLoop(ctx, serverURL, token, tunnelRef, c.RoomID(), deviceID)

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
			if err := resyncFromServer(ctx); err != nil {
				return err
			}
			resubmitPending(sess)
			return nil
		}
		sess.OnEvent = func(ev vmprotocol.Event) {
			switch ev.Topic {
			case vmprotocol.TopicPortsChanged, vmprotocol.TopicRoomEnding:
				if err := proxy.Sync(ctx); err != nil {
					fmt.Fprintf(os.Stderr, "room-sync: gateway sync: %v\n", err)
				}
			case vmprotocol.TopicOperation:
				// A REMOTE participant's operation applied locally:
				// connected mounts must drop cached attributes/content.
				// Our own acknowledgement is skipped: the submitting
				// mount already holds the new content, and discarding
				// its buffer would let an already-open handle read
				// empty bytes (a duplicate flush could then truncate
				// the authoritative file).
				var env vmprotocol.Envelope
				if json.Unmarshal(ev.Payload, &env) == nil && env.AuthorDeviceID != deviceID {
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
						// Resolver duties follow the MOVED barriers: the
						// authoritative fence now lives at the new prefix,
						// and reconnect retries must not keep targeting
						// the obsolete old path.
						bridge.RekeyDuty(r.OldPath, r.NewPath)
						for _, p := range moved {
							bridge.RekeyDuty(r.OldPath+"/"+strings.TrimPrefix(p, r.NewPath+"/"), p)
						}
					}
				}
			case vmprotocol.TopicPresence:
				// Ownership transferred or succession landed on THIS
				// device: promote to the owner policy (eager blobs, full
				// materialization) so the final workspace survives a
				// server failure — a lazily-promoted owner would keep
				// skipping blob fetches.
				var pd vmprotocol.PresenceDevice
				if json.Unmarshal(ev.Payload, &pd) == nil && pd.DeviceID == deviceID && pd.IsOwner {
					if mgr.Policy != replica.Full {
						if err := mgr.PromoteToFull(context.Background()); err != nil {
							fmt.Fprintf(os.Stderr, "room-sync: owner promotion: %v\n", err)
						} else if sess := sessionRef.get(); sess != nil {
							if err := sess.ReportPolicy("full"); err != nil {
								fmt.Fprintf(os.Stderr, "room-sync: report promoted policy: %v\n", err)
							}
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
			case vmprotocol.TopicConflictResolved:
				// The SERVER confirmed a barrier lift: only this
				// acknowledgement may retire the resolver duty (a
				// fire-and-forget resolve frame can die with a
				// dropped socket; the duty must survive to retry
				// after reconnect).
				var cp vmprotocol.ConflictPayload
				if json.Unmarshal(ev.Payload, &cp) == nil {
					bridge.OnConflictResolved(cp.Path)
				}
			case borrowagent.TopicAgentShared:
				var p borrowagent.AgentSharedPayload
				if json.Unmarshal(ev.Payload, &p) == nil {
					borrowHost.Shared(p)
				}
			case borrowagent.TopicBorrowCommand:
				// A borrower's instruction routed to this owning
				// device: execution belongs to the host's agent
				// runtime, never to room-sync itself.
				var p borrowagent.BorrowCommandPayload
				if json.Unmarshal(ev.Payload, &p) == nil {
					if err := borrowHost.ExecuteCommand(p); err != nil {
						// Visible failure, never a silent fake success:
						// the borrower learns the owner runtime cannot
						// execute (no Agent Host adapter wired).
						fmt.Fprintf(os.Stderr, "room-sync: borrow command %s: %v\n", p.CommandID, err)
					}
				}
			case borrowagent.TopicBorrowRevoked:
				var p borrowagent.BorrowRevokedPayload
				if json.Unmarshal(ev.Payload, &p) == nil {
					borrowHost.Revoked(p)
				}
			case borrowagent.TopicApprovalRequest:
				var p borrowagent.ApprovalRequestPayload
				if json.Unmarshal(ev.Payload, &p) == nil {
					borrowHost.ApprovalRequest(p)
				}
			case borrowagent.TopicApprovalDecision:
				var p borrowagent.ApprovalDecisionPayload
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
		// Deliver operations whose acknowledgement was lost with the
		// previous socket (deduplicated server-side when they actually
		// committed before the disconnect).
		resubmitPending(sess)
		// Replica policy feeds automatic succession: only full replicas
		// may inherit a lost owner (owner-survival contract). Report
		// the MANAGER's current policy, not the startup-local value:
		// a bootstrap-detected promotion (or an IsOwner event) set
		// mgr.Policy = Full after startup, and reporting the stale
		// startup value overwrote the server's full record — later
		// successions would skip an actually-materialized replica.
		if err := sess.ReportPolicy(string(mgr.Policy)); err != nil {
			fmt.Fprintf(os.Stderr, "room-sync: report policy: %v\n", err)
		}
		// Barrier resolutions whose confirmation was lost with the old
		// socket: the fences are still up server-side, and only this
		// session (the assigned resolver) can lift them.
		bridge.RetryDuties()
		// The reader runs BEFORE/WITH the seeder: sess.Run is the only
		// consumer of operation acknowledgements, so seeding before it
		// would time out every SubmitAndWait and permanently drop the
		// rest of the workspace. Seed asynchronously; Run blocks below.
		runErrCh := make(chan error, 1)
		go func() { runErrCh <- sess.Run(mgr) }()
		if seedDir != "" && !seeded && isOwner {
			if err := seedWorkspace(ctx, bridge, seedDir); err != nil {
				// A PARTIAL seed (some entries accepted, then failure)
				// must not be marked complete: the room would sit as a
				// silent partial workspace forever (AppliedSeq is
				// already > 0, so nothing re-triggers). Leave the flag
				// unset — the next session retries, and per-entry Stat
				// preflight skips what already landed.
				fmt.Fprintf(os.Stderr, "room-sync: seed (will retry): %v\n", err)
			} else {
				seeded = true
			}
		}
		runErr := <-runErrCh
		sess.Close()
		sessionRef.clear(sess)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		fmt.Fprintf(os.Stderr, "room-sync: session lost (%v); reconnecting\n", runErr)
		// Reconnect path: resync the replica from a fresh bootstrap.
		for {
			if err := resyncFromServer(ctx); err == nil {
				break
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
	// Canonicalize with the SAME SlugifyLabel the bridge identity uses:
	// announcing the raw value ("Claude Code") produced a route whose
	// .tutti label is invalid — VIP listeners skip it, DNS parsing
	// rejects it, and the conflict resolver no longer matches the
	// bridge's identity.
	label := vmprotocol.SlugifyLabel(os.Getenv("OPEN_TUTTI_SESSION_LABEL"))
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
// tunnelLoop keeps a relay tunnel alive independently of the business
// WebSocket: dial, publish through tunnelRef, serve the inbound leg
// until the tunnel dies, then redial with backoff. Replacing the
// reference BEFORE closing the old tunnel is what keeps the
// server-side relay entry correct (a leaked unregister would delete
// the replacement's entry and strand advertised routes).
func tunnelLoop(ctx context.Context, serverURL, token string, tunnelRef *liveTunnel, roomID, deviceID string) {
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		tun, err := tunneldial.Dial(ctx, serverURL, token)
		if err != nil {
			fmt.Fprintf(os.Stderr, "room-sync: tunnel dial: %v\n", err)
			if !sleepCtx(ctx, backoff) {
				return
			}
			backoff = minDuration(backoff*2, 30*time.Second)
			continue
		}
		backoff = time.Second
		tunnelRef.set(tun)
		// Inbound leg: another device connected to one of this device's
		// advertised routes; forward each relayed stream to the owning
		// session container on the room network. Returns when the
		// tunnel closes.
		serveTunnel(ctx, tun, roomID, deviceID)
		tunnelRef.clearLocked(tun)
		tun.Close()
		if ctx.Err() != nil {
			return
		}
		// The tunnel dropped (relay restart, network blip): redial after
		// a short pause; the business socket is unaffected.
		if !sleepCtx(ctx, backoff) {
			return
		}
	}
}

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
			// BOTH legs close when EITHER copy finishes (same shape as
			// the gateway pipe): the initiating device disconnecting
			// while the local target stays open and quiet used to
			// leave the foreground copy blocked on local forever — the
			// goroutine, local connection, and yamux stream all leaked.
			done := make(chan struct{}, 2)
			go func() {
				io.Copy(local, stream)
				done <- struct{}{}
			}()
			go func() {
				io.Copy(stream, local)
				done <- struct{}{}
			}()
			<-done
			stream.Close()
			local.Close()
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
func (l *liveSession) get() *client.Session  { l.mu.Lock(); defer l.mu.Unlock(); return l.s }
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
		return fmt.Errorf("room socket reconnecting: %w", replica.ErrNotSent)
	}
	return s.ResolveBarrier(path)
}

func (l *liveSession) Submit(env vmprotocol.Envelope) error {
	l.mu.Lock()
	s := l.s
	l.mu.Unlock()
	if s == nil {
		return fmt.Errorf("room socket reconnecting: %w", replica.ErrNotSent)
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
// platformCacheDir / platformCADir / platformFSListen are the single
// platform adapter for native defaults: the Linux room container uses
// POSIX-rooted /data and a unix socket, but the published Windows
// binary runs as a normal user, where drive-root paths are unwritable
// and unix sockets do not exist — per-user storage and a loopback TCP
// listener keep the artifact working without overrides. Explicit env
// always wins.
func platformUserBase() string {
	if dir, err := os.UserCacheDir(); err == nil {
		return filepath.Join(dir, "open-tutti")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ".open-tutti"
	}
	return filepath.Join(home, ".open-tutti")
}

func platformCacheDir() string {
	if runtime.GOOS == "windows" {
		return filepath.Join(platformUserBase(), "cache")
	}
	return "/data/cache"
}

func platformCADir() string {
	if runtime.GOOS == "windows" {
		return filepath.Join(platformUserBase(), "ca")
	}
	return "/data/ca"
}

func platformFSListen() string {
	if runtime.GOOS == "windows" {
		return "127.0.0.1:5266"
	}
	return "/run/open-tutti/roomfs.sock"
}

func listenRoomFS(addr string) (net.Listener, error) {
	if strings.Contains(addr, ":") {
		return net.Listen("tcp", addr)
	}
	os.Remove(addr)
	return net.Listen("unix", addr)
}

// seedWorkspace submits an entire host directory tree into the empty
// room through the normal OT path (creates, mkdirs, writes) so the
// shared workspace starts as the owner's real workspace.
func seedWorkspace(ctx context.Context, bridge *roomfsbridge.Handler, root string) error {
	return filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Windows separators must normalize away: WalkDir joins with
		// "\" there, and a backslash-prefixed path fails workspace
		// path validation, blocking every Windows owner's seed.
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if rel == "." || rel == "" {
			return nil
		}
		// The protocol has no symlink kind, and dereferencing one would
		// upload whatever it points at (potentially OUTSIDE the
		// selected workspace, e.g. credentials -> ../.ssh/id_rsa) as an
		// ordinary shared file. Skip with a loud log line.
		if d.Type()&fs.ModeSymlink != 0 {
			fmt.Fprintf(os.Stderr, "room-sync: seed: skipping symlink %s\n", rel)
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if d.IsDir() {
			// Preflight makes the seed IDEMPOTENT (resumable after a
			// partial failure): already-landed entries skip.
			if st, err := bridge.Stat(rel); err == nil && st != nil && st.Exists && st.Dir {
				return nil
			}
			return bridge.Mkdir(rel, uint32(info.Mode().Perm()))
		}
		exists := false
		if st, err := bridge.Stat(rel); err == nil && st != nil && st.Exists && !st.Dir {
			if st.Size == info.Size() {
				return nil
			}
			// A PARTIAL earlier attempt (create acked, then the read,
			// CAS upload, or write failed) leaves an authoritative
			// zero-length entry: calling Create again would fail
			// "already exists" and pin the seed at this entry forever.
			// Continue straight to the write instead.
			exists = true
		}
		if !exists {
			if err := bridge.Create(rel, uint32(info.Mode().Perm())); err != nil {
				return err
			}
		}
		content, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		// bridge.Write itself routes oversized content through the CAS
		// blob-replacement path: skipping here would leave the created
		// entry EMPTY, and a later Apply-to-Workspace would mirror that
		// empty authoritative entry over the owner's real file.
		return bridge.Write(rel, content)
	})
}
