//go:build windows

package main

// registerApplySignal is a no-op on Windows (no SIGUSR1): the
// apply-and-leave lifecycle runs through OPEN_TUTTI_APPLY_AND_LEAVE=1.
func registerApplySignal(run func()) {}
