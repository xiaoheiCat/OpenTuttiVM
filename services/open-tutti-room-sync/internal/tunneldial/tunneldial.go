// Package tunneldial is the device side of the server relay: one WebSocket
// carries a yamux session, and each logical stream targets a room route.
// All cross-device traffic flows device → server → device.
package tunneldial

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/coder/websocket"
	"github.com/hashicorp/yamux"
	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
)

const defaultMaxStreams = 256

var ErrStreamLimit = fmt.Errorf("tunnel stream limit reached")

// Tunnel is one multiplexed connection to the server relay.
type Tunnel struct {
	sess  *yamux.Session
	slots chan struct{}
}

// Dial connects the device tunnel using the room session token.
func Dial(ctx context.Context, serverURL, token string) (*Tunnel, error) {
	wsURL := strings.Replace(serverURL, "http://", "ws://", 1)
	wsURL = strings.Replace(wsURL, "https://", "wss://", 1)
	// Header credentials: the URL (with the token) is what proxies and
	// access loggers record.
	wsURL += "/api/tunnel"
	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": []string{"Bearer " + token}},
	})
	if err != nil {
		return nil, fmt.Errorf("tunnel dial: %w", err)
	}
	netConn := websocket.NetConn(ctx, conn, websocket.MessageBinary)
	sess, err := yamux.Client(netConn, nil)
	if err != nil {
		netConn.Close()
		return nil, fmt.Errorf("tunnel yamux client: %w", err)
	}
	return &Tunnel{sess: sess, slots: make(chan struct{}, defaultMaxStreams)}, nil
}

// Connect opens a stream to one room route. The first frame identifies the
// target; everything after is raw bytes.
func (t *Tunnel) Connect(route vmprotocol.RouteKey) (net.Conn, error) {
	if !t.tryAcquire() {
		return nil, ErrStreamLimit
	}
	stream, err := t.sess.Open()
	if err != nil {
		t.release()
		return nil, err
	}
	header := vmprotocol.TunnelHeader{Action: vmprotocol.TunnelConnect, RoomID: route.RoomID, Route: route}
	if err := writeHeaderFrame(stream, header); err != nil {
		stream.Close()
		t.release()
		return nil, err
	}
	return &admittedConn{Conn: stream, release: t.release}, nil
}

// Accept answers a server-initiated stream (another device connecting to a
// route on this device).
func (t *Tunnel) Accept() (net.Conn, *vmprotocol.TunnelHeader, error) {
	for {
		stream, err := t.sess.Accept()
		if err != nil {
			return nil, nil, err
		}
		if !t.tryAcquire() {
			_ = stream.Close()
			continue
		}
		header, err := readHeaderFrame(stream)
		if err != nil {
			_ = stream.Close()
			t.release()
			return nil, nil, err
		}
		return &admittedConn{Conn: stream, release: t.release}, header, nil
	}
}

func (t *Tunnel) tryAcquire() bool {
	select {
	case t.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (t *Tunnel) release() { <-t.slots }

type admittedConn struct {
	net.Conn
	once    sync.Once
	release func()
}

func (c *admittedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}

// Close shuts the tunnel down.
func (t *Tunnel) Close() error { return t.sess.Close() }

func readHeaderFrame(conn net.Conn) (*vmprotocol.TunnelHeader, error) {
	var lenBuf [4]byte
	if _, err := readFull(conn, lenBuf[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(lenBuf[:])
	if n == 0 || n > 1<<20 {
		return nil, fmt.Errorf("bad header length %d", n)
	}
	buf := make([]byte, n)
	if _, err := readFull(conn, buf); err != nil {
		return nil, err
	}
	h, err := vmprotocol.DecodeTunnelHeader(buf)
	if err != nil {
		return nil, err
	}
	return &h, nil
}

func readFull(conn net.Conn, buf []byte) (int, error) {
	return io.ReadFull(conn, buf)
}

func writeHeaderFrame(conn net.Conn, h vmprotocol.TunnelHeader) error {
	data, err := h.Encode()
	if err != nil {
		return err
	}
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))
	if _, err := conn.Write(lenBuf[:]); err != nil {
		return err
	}
	_, err = conn.Write(data)
	return err
}
