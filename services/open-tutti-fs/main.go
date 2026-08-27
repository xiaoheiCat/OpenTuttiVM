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
	"os"
	"os/signal"
	"syscall"

	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-fs/roomfs"
)

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

	client, err := roomfs.Dial(addr)
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
