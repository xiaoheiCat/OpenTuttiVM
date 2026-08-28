// Package roomfs is the Room FS Protocol: the seam between open-tutti-fs
// (FUSE mount inside an agent-session container) and open-tutti-room-sync
// (the replica engine). One unix-socket stream carries length-framed JSON
// requests with optional binary bodies, plus server-pushed invalidations
// when remote operations change paths the mount has cached.
package roomfs

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"sync/atomic"
)

// Request is one mount → replica call.
type Request struct {
	ID   uint64 `json:"id"`
	Type string `json:"type"`
	Path string `json:"path,omitempty"`
	// Body carries write content for "write".
	Body []byte `json:"-"`
	// To for renames; Mode for create/mkdir.
	To   string `json:"to,omitempty"`
	Mode uint32 `json:"mode,omitempty"`
}

// Response answers a request by id. Push responses (Push=true) carry
// server notifications instead: Type "invalidate" with Path set.
type Response struct {
	ID      uint64          `json:"id"`
	OK      bool            `json:"ok"`
	Error   string          `json:"error,omitempty"`
	Body    []byte          `json:"-"`
	Entries []DirEntry      `json:"entries,omitempty"`
	Stat    *Stat           `json:"stat,omitempty"`
	Seq     uint64          `json:"seq,omitempty"`
	Extra   json.RawMessage `json:"extra,omitempty"`
	Push    bool            `json:"push,omitempty"`
	Type    string          `json:"type,omitempty"`
	Path    string          `json:"path,omitempty"`
}

// DirEntry is one child of a directory listing.
type DirEntry struct {
	Name string `json:"name"`
	Dir  bool   `json:"dir"`
}

// Stat describes one path.
type Stat struct {
	Dir    bool   `json:"dir"`
	Size   int64  `json:"size"`
	Mode   uint32 `json:"mode"`
	Exists bool   `json:"exists"`
}

// ServerPush notifies the mount of remote changes (correlated responses
// arrive as ordinary Responses by id).
type ServerPush struct {
	Type string `json:"type"` // "invalidate"
	Path string `json:"path"`
}

// Request types.
const (
	TypeStat   = "stat"
	TypeRead   = "read"
	TypeList   = "list"
	TypeWrite  = "write" // whole-file write; replica converts to an operation
	TypeCreate = "create"
	TypeMkdir  = "mkdir"
	TypeRemove = "remove"
	TypeRename = "rename"
)

// Frame wire format: 4-byte big-endian header length, JSON header, then a
// 4-byte body length and raw body bytes. Bodies stay out of JSON so
// content bytes never round-trip through base64.
type Frame struct {
	Header any
	Body   []byte
}

var frameCounter atomic.Uint64

// NextID hands out request ids.
func NextID() uint64 { return frameCounter.Add(1) }

// WriteFrame writes one framed message.
func WriteFrame(w *bufio.Writer, header any, body []byte) error {
	headerJSON, err := json.Marshal(header)
	if err != nil {
		return err
	}
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(headerJSON)))
	if _, err := w.Write(lenBuf[:]); err != nil {
		return err
	}
	if _, err := w.Write(headerJSON); err != nil {
		return err
	}
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(body)))
	if _, err := w.Write(lenBuf[:]); err != nil {
		return err
	}
	_, err = w.Write(body)
	return err
}

// ReadFrame reads one framed message into header (pointer) and returns the
// raw body.
func ReadFrame(r *bufio.Reader, header any) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(lenBuf[:])
	if n > 1<<22 {
		return nil, fmt.Errorf("frame header too large: %d", n)
	}
	headerJSON := make([]byte, n)
	if _, err := io.ReadFull(r, headerJSON); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(headerJSON, header); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	n = binary.BigEndian.Uint32(lenBuf[:])
	if n > 64<<20 {
		return nil, fmt.Errorf("frame body too large: %d", n)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}
	return body, nil
}
