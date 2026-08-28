//go:build !windows

package main

import (
	"os"
	"os/signal"
	"syscall"
)

// registerApplySignal wires SIGUSR1 to the apply-and-leave hook (POSIX
// only; Windows uses the one-shot OPEN_TUTTI_APPLY_AND_LEAVE mode).
func registerApplySignal(run func()) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGUSR1)
	go func() {
		for range ch {
			run()
		}
	}()
}
