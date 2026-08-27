//go:build !linux

package main

import (
	"context"
	"fmt"

	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-fs/roomfs"
)

// mount is Linux-only: the FUSE layer runs inside agent-session containers
// on the Docker Desktop VM. Non-Linux builds keep compiling for dev boxes.
func mount(ctx context.Context, mountPoint string, client *roomfs.Client) error {
	return fmt.Errorf("open-tutti-fs mounts only inside Linux containers (this build is not linux)")
}
