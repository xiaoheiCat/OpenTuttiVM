// Package tunnel implements the multiplexed relay: every device holds one
// WebSocket tunnel to the server, and yamux multiplexes logical streams
// over it. Preview and raw-TCP traffic always flows device → server →
// device; there is no peer-to-peer path.
package tunnel

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"

	"github.com/coder/websocket"
	"github.com/hashicorp/yamux"
	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
)

// Relay tracks one yamux session per online device per room and stitches
// connect streams between them.
type Relay struct {
	log *slog.Logger

	mu       sync.Mutex
	sessions map[string]map[string]*yamux.Session // roomID → deviceID → session
}

// NewRelay wires the relay.
func NewRelay(log *slog.Logger) *Relay {
	return &Relay{log: log, sessions: map[string]map[string]*yamux.Session{}}
}

// ServeTunnel upgrades an authenticated device websocket into a yamux
// server session and pumps streams until close. The caller has already
// validated the session token into (roomID, deviceID).
func (r *Relay) ServeTunnel(ctx context.Context, ws *websocket.Conn, roomID, deviceID string) error {
	netConn := websocket.NetConn(ctx, ws, websocket.MessageBinary)
	defer netConn.Close()

	sess, err := yamux.Server(netConn, nil)
	if err != nil {
		return fmt.Errorf("tunnel yamux server: %w", err)
	}
	defer sess.Close()

	r.register(roomID, deviceID, sess)
	defer r.unregister(roomID, deviceID)

	for {
		stream, err := sess.Accept()
		if err != nil {
			return nil // session closed
		}
		go r.handleStream(stream)
	}
}

func (r *Relay) register(roomID, deviceID string, sess *yamux.Session) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sessions[roomID] == nil {
		r.sessions[roomID] = map[string]*yamux.Session{}
	}
	r.sessions[roomID][deviceID] = sess
}

func (r *Relay) unregister(roomID, deviceID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if devs := r.sessions[roomID]; devs != nil {
		delete(devs, deviceID)
		if len(devs) == 0 {
			delete(r.sessions, roomID)
		}
	}
}

func (r *Relay) handleStream(stream net.Conn) {
	header, err := readHeader(stream)
	if err != nil {
		stream.Close()
		return
	}
	if header.Action != vmprotocol.TunnelConnect || header.Route.Port == 0 || header.Route.SessionID == "" {
		stream.Close()
		return
	}
	target := r.dial(header.Route)
	if target == nil {
		writeHeaderError(stream, fmt.Sprintf("route %s:%d unreachable", header.Route.DeviceID, header.Route.Port))
		stream.Close()
		return
	}
	if err := writeHeader(target, *header); err != nil {
		target.Close()
		stream.Close()
		return
	}
	pipe(stream, target)
}

func (r *Relay) dial(route vmprotocol.RouteKey) net.Conn {
	r.mu.Lock()
	defer r.mu.Unlock()
	sess := r.sessions[route.RoomID][route.DeviceID]
	if sess == nil {
		return nil
	}
	stream, err := sess.Open()
	if err != nil {
		return nil
	}
	return stream
}

func pipe(a, b net.Conn) {
	done := make(chan struct{}, 2)
	go func() { io.Copy(a, b); done <- struct{}{} }()
	go func() { io.Copy(b, a); done <- struct{}{} }()
	<-done
	a.Close()
	b.Close()
}

// readHeader reads one length-prefixed JSON header frame.
func readHeader(conn net.Conn) (*vmprotocol.TunnelHeader, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(lenBuf[:])
	if n == 0 || n > 1<<20 {
		return nil, fmt.Errorf("bad header length %d", n)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return nil, err
	}
	return decodeHeader(buf)
}

// writeHeader writes one length-prefixed JSON header frame.
func writeHeader(conn net.Conn, h vmprotocol.TunnelHeader) error {
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

func writeHeaderError(conn net.Conn, msg string) {
	_ = writeHeader(conn, vmprotocol.TunnelHeader{Action: "error", Route: vmprotocol.RouteKey{}, DeviceID: msg})
}

func decodeHeader(data []byte) (*vmprotocol.TunnelHeader, error) {
	h, err := vmprotocol.DecodeTunnelHeader(data)
	if err != nil {
		return nil, err
	}
	return &h, nil
}
