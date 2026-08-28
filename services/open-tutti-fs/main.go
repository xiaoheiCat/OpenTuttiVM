// open-tutti-fs mounts the room workspace inside an agent-session
// container. It connects to open-tutti-room-sync over the Room FS Protocol
// (unix socket by default) and serves a FUSE tree whose reads come from
// the local replica and whose writes become Room File Operations.
//
// Environment:
//
//	OPEN_TUTTI_ROOMFS_ADDR  room-sync listen address; unix socket path or
//	                        host:port (default /run/open-tutti/roomfs.sock)
//	OPEN_TUTTI_MOUNT        mount point (default /workspace)
//	OPEN_TUTTI_DEVICE_ID    this device's id, stamped on submissions
package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	roomfs "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-roomfs"
)

// dialRoomFS is this host's transport policy (the shared protocol package
// stays transport-neutral): a bare path means the room-sync unix socket;
// anything with a port falls back to TCP.
func dialRoomFS(addr string) (*roomfs.Client, error) {
	var (
		conn net.Conn
		err  error
	)
	if _, _, perr := net.SplitHostPort(addr); perr != nil {
		conn, err = net.Dial("unix", addr)
	} else {
		conn, err = net.Dial("tcp", addr)
	}
	if err != nil {
		return nil, fmt.Errorf("roomfs dial %s: %w", addr, err)
	}
	return roomfs.NewClient(conn), nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "open-tutti-fs:", err)
		os.Exit(1)
	}
}

func run() error {
	addr := os.Getenv("OPEN_TUTTI_ROOMFS_ADDR")
	if addr == "" {
		addr = "/run/open-tutti/roomfs.sock"
	}
	mountPoint := os.Getenv("OPEN_TUTTI_MOUNT")
	if mountPoint == "" {
		mountPoint = "/workspace"
	}
	deviceID := os.Getenv("OPEN_TUTTI_DEVICE_ID")
	if deviceID == "" {
		return fmt.Errorf("OPEN_TUTTI_DEVICE_ID is required")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	client, err := dialRoomFS(addr)
	if err != nil {
		return fmt.Errorf("connect room-sync at %s: %w", addr, err)
	}
	defer client.Close()

	if err := os.MkdirAll(mountPoint, 0o755); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "open-tutti-fs: mounting room at %s via %s\n", mountPoint, addr)
	_ = deviceID // stamped by room-sync at submission
	return mount(ctx, mountPoint, client)
}
