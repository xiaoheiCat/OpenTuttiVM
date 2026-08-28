//go:build !linux

package gateway

import "net"

// freebindListen: IP_FREEBIND is Linux-only. On platforms without it the
// allocator falls back to the shared loopback address (see vip.go).
func freebindListen(addr string) (net.Listener, error) {
	return nil, &net.OpError{Op: "listen", Net: "tcp", Err: errNoFreebind}
}

type noFreebindError struct{}

func (noFreebindError) Error() string { return "IP_FREEBIND unavailable on this platform" }

var errNoFreebind error = noFreebindError{}
