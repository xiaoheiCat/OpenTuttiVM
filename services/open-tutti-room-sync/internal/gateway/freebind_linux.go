//go:build linux

package gateway

import (
	"net"
	"syscall"
)

// freebindListen binds a reserved address without any interface
// configuration: IP_FREEBIND (like IP_TRANSPARENT) lets the socket claim
// an address the host does not own. The room network keeps per-host VIPs
// even in plain containers.
func freebindListen(addr string) (net.Listener, error) {
	lc := net.ListenConfig{Control: func(network, address string, c syscall.RawConn) error {
		var sockErr error
		err := c.Control(func(fd uintptr) {
			sockErr = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, syscall.IP_FREEBIND, 1)
		})
		if err != nil {
			return err
		}
		return sockErr
	}}
	return lc.Listen(nil, "tcp", addr)
}
