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
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	vmcas "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-cas"

	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-fs/roomfs"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-room-sync/internal/client"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-room-sync/internal/gateway"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-room-sync/internal/replica"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-room-sync/internal/roomfsbridge"
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

	sess, err := c.Dial(ctx)
	if err != nil {
		return fmt.Errorf("room socket: %w", err)
	}
	defer sess.Close()
	sess.OnGap = func() error {
		snap, ops, err := c.Bootstrap(ctx)
		if err != nil {
			return err
		}
		return mgr.Bootstrap(ctx, snap, ops)
	}

	fmt.Fprintf(os.Stderr, "room-sync ready: device=%s policy=%s paths=%d vip-block=%s\n",
		deviceID, policy, len(mgr.Replica.State.Paths()), gateway.ReservedBlock)
	_ = vips

	// Host the Room FS Protocol for agent-session mounts (open-tutti-fs).
	roomfsAddr := os.Getenv("OPEN_TUTTI_FS_LISTEN")
	if roomfsAddr == "" {
		roomfsAddr = "/run/open-tutti/roomfs.sock"
	}
	if dir := filepath.Dir(roomfsAddr); !strings.Contains(roomfsAddr, ":") {
		os.MkdirAll(dir, 0o755)
	}
	roomfsSrv := roomfs.NewServer(nil, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	bridge := roomfsbridge.New(mgr, sess, c, deviceID, "local", roomfsSrv.BroadcastInvalidate)
	roomfsSrv.SetHandler(bridge)
	ln, err := listenRoomFS(roomfsAddr)
	if err != nil {
		return err
	}
	defer ln.Close()
	go roomfsSrv.Serve(ln)

	fmt.Fprintf(os.Stderr, "room-fs listening on %s\n", roomfsAddr)

	errCh := make(chan error, 1)
	go func() { errCh <- sess.Run(mgr.Replica) }()
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return sess.Close()
	}
}

// listenRoomFS binds the unix socket (or TCP) for mounts.
func listenRoomFS(addr string) (net.Listener, error) {
	if strings.Contains(addr, ":") {
		return net.Listen("tcp", addr)
	}
	os.Remove(addr)
	return net.Listen("unix", addr)
}
