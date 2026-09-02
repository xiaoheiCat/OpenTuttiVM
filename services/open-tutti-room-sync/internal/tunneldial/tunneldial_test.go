package tunneldial

import (
	"net"
	"testing"
)

func TestTunnelStreamAdmissionReleaseAfterClose(t *testing.T) {
	tun := &Tunnel{slots: make(chan struct{}, 1)}
	if !tun.tryAcquire() {
		t.Fatal("first stream was not admitted")
	}
	if tun.tryAcquire() {
		t.Fatal("second stream exceeded the limit")
	}
	peer, conn := net.Pipe()
	defer peer.Close()
	wrapped := &admittedConn{Conn: conn, release: tun.release}
	if err := wrapped.Close(); err != nil {
		t.Fatal(err)
	}
	if !tun.tryAcquire() {
		t.Fatal("stream was not reusable after close")
	}
	wrapped.Close()
}

func TestAdmittedConnCloseIsIdempotent(t *testing.T) {
	tun := &Tunnel{slots: make(chan struct{}, 1)}
	if !tun.tryAcquire() {
		t.Fatal("stream was not admitted")
	}
	peer, conn := net.Pipe()
	defer peer.Close()
	wrapped := &admittedConn{Conn: conn, release: tun.release}
	if err := wrapped.Close(); err != nil {
		t.Fatal(err)
	}
	_ = wrapped.Close()
	if tun.tryAcquire() == false {
		t.Fatal("admission token was not released")
	}
}
