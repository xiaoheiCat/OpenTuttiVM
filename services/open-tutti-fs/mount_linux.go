//go:build linux

package main

import (
	"context"

	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-fs/internal/fsmount"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-fs/roomfs"
)

// mount serves the FUSE tree; only Linux containers can mount /dev/fuse —
// desktop hosts never run this binary.
func mount(ctx context.Context, mountPoint string, client *roomfs.Client) error {
	return fsmount.Mount(ctx, mountPoint, client)
}
