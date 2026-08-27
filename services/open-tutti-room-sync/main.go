// open-tutti-room-sync is the per-room local engine that runs inside the
// room-sync container of an open-tutti-vm-<roomId> Docker project: it keeps
// the workspace replica in the open-tutti-vm-<roomId>-workspace volume,
// bridges agent-session FUSE mounts onto the same logical workspace, and
// terminates the .tutti virtual network for this device.
//
// Environment:
//
//	OPEN_TUTTI_SERVER        server base URL (required)
//	OPEN_TUTTI_TOKEN         room session token (required)
//	OPEN_TUTTI_DEVICE_ID     this device's id (required)
//	OPEN_TUTTI_POLICY        replica policy: lazy (default) | full
//	OPEN_TUTTI_CACHE_DIR     CAS cache dir (default /data/cache)
//	OPEN_TUTTI_FS_LISTEN     room FS protocol listen address
//	                        (default unix /run/open-tutti/roomfs.sock)
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	vmcas "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-cas"
	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"

	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-fs/roomfs"
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
	_ = ca // injected into Tutti Browser and session containers by the runtime

	// The session and tunnel are hot-swapped across reconnects; the bridge
	// and proxy hold these stable references.
	sessionRef := &liveSession{}
	tunnelRef := &liveTunnel{}

	// Cross-device streams ride the yamux tunnel; the .tutti proxy binds a
	// synthetic-VIP listener per live room route and pipes through it.
	proxy := gateway.NewProxy(vips, tunnelRef, c, c.RoomID(), deviceID,
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
	bridge := roomfsbridge.New(mgr, sessionRef, c, deviceID, "local", roomfsSrv.BroadcastInvalidate)
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
				}
			case vmprotocol.TopicOperationRejected:
				var rej vmprotocol.RejectionPayload
				if json.Unmarshal(ev.Payload, &rej) == nil {
					mgr.NotifyRejected(rej.OperationID, rej.Reason)
				}
			}
		}
		sessionRef.set(sess)
		tun, err := tunneldial.Dial(ctx, serverURL, token)
		if err != nil {
			fmt.Fprintf(os.Stderr, "room-sync: tunnel dial: %v\n", err)
		} else {
			tunnelRef.set(tun)
			defer tun.Close()
		}
		runErr := sess.Run(mgr)
		sess.Close()
		sessionRef.clear(sess)
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

// liveSession is the hot-swappable WS session reference held by the Room
// FS bridge across reconnects.
type liveSession struct {
	mu sync.Mutex
	s  *client.Session
}

func (l *liveSession) set(s *client.Session)    { l.mu.Lock(); l.s = s; l.mu.Unlock() }
func (l *liveSession) clear(s *client.Session) {
	l.mu.Lock()
	if l.s == s {
		l.s = nil
	}
	l.mu.Unlock()
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
