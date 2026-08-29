package roomfs

import (
	"bufio"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"
)

// Handler bridges the protocol onto the local replica. open-tutti-room-sync
// implements it over its replica.Manager and operation client.
type Handler interface {
	Stat(path string) (*Stat, error)
	Read(path string) ([]byte, error)
	// ReadWithHash returns the content and its content hash from ONE
	// consistent replica snapshot (a separate Stat could label older
	// bytes with a newer hash and defeat flush baselines).
	ReadWithHash(path string) ([]byte, string, error)
	// WriteGuarded applies a whole-file write whose submitted baseline
	// is validated ATOMICALLY with the write preparation; returns the
	// acknowledged post-write hash.
	WriteGuarded(path string, content []byte, baseHash string) (string, error)
	List(path string) ([]DirEntry, error)
	// Write applies a whole-file write through the ops converter and
	// submits the resulting operation; implementations signal room-level
	// rejections with the wire error "rejected" (client → ErrRejected).
	Write(path string, content []byte) error
	Create(path string, mode uint32) error
	Mkdir(path string, mode uint32) error
	Remove(path string) error
	Rename(from, to string, noReplace bool) error
	// Chmod submits a permission-bit change as a metadata operation so
	// it reaches the authoritative workspace and every participant.
	Chmod(path string, mode uint32) error
}

// Server hosts the protocol on a listener and can push invalidations to
// every connected mount when remote operations land.
type Server struct {
	handler Handler
	log     *slog.Logger

	mu    sync.Mutex
	conns map[*serverConn]struct{}
}

type serverConn struct {
	conn   net.Conn
	writer *bufio.Writer
	mu     sync.Mutex
}

// NewServer creates a protocol server over a handler. The handler may be
// attached later with SetHandler when it needs the server first (e.g. to
// broadcast invalidations).
func NewServer(h Handler, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{handler: h, log: log, conns: map[*serverConn]struct{}{}}
}

// SetHandler attaches or replaces the handler.
func (s *Server) SetHandler(h Handler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handler = h
}

// Serve accepts mount connections until the listener closes.
func (s *Server) Serve(ln net.Listener) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		// Register synchronously with accept: a client that connected
		// earlier must be visible to any later broadcast, instead of
		// racing the serve goroutine's registration.
		sc := &serverConn{conn: conn, writer: bufio.NewWriter(conn)}
		s.mu.Lock()
		s.conns[sc] = struct{}{}
		s.mu.Unlock()
		go s.serveConn(conn, sc)
	}
}

func (s *Server) serveConn(conn net.Conn, sc *serverConn) {
	defer conn.Close()
	defer func() {
		s.mu.Lock()
		delete(s.conns, sc)
		s.mu.Unlock()
	}()

	reader := bufio.NewReader(conn)
	for {
		var req Request
		body, err := ReadFrame(reader, &req)
		if err != nil {
			return
		}
		res := s.dispatch(req, body)
		sc.mu.Lock()
		err = WriteFrame(sc.writer, res, res.Body)
		if err == nil {
			err = sc.writer.Flush()
		}
		sc.mu.Unlock()
		if err != nil {
			return
		}
	}
}

func (s *Server) dispatch(req Request, body []byte) Response {
	res := Response{ID: req.ID, OK: true}
	switch req.Type {
	case TypeStat:
		st, err := s.handler.Stat(req.Path)
		if err != nil {
			return errorResponse(req.ID, err)
		}
		if st == nil {
			st = &Stat{}
		}
		res.Stat = st
	case TypeRead:
		content, hash, err := s.handler.ReadWithHash(req.Path)
		if err != nil {
			return errorResponse(req.ID, err)
		}
		// Preflight the frame bound BEFORE writing: an oversized body
		// would have the client reject the advertised length and shut
		// down the mount's single shared connection, breaking every
		// later operation. Fail THIS read instead (CAS-backed files
		// beyond the protocol bound surface a per-read error, not a
		// dead mount).
		if uint64(len(content)) > MaxBodyBytes {
			return errorResponse(req.ID, fmt.Errorf("roomfs: read of %s exceeds protocol body limit (%d bytes)", req.Path, MaxBodyBytes))
		}
		res.Body = content
		res.Hash = hash
	case TypeList:
		entries, err := s.handler.List(req.Path)
		if err != nil {
			return errorResponse(req.ID, err)
		}
		res.Entries = entries
	case TypeWrite:
		// Optimistic-concurrency guard, ATOMIC with write preparation
		// (WriteGuarded): a flush carrying a superseded revision's hash
		// fails HERE (EAGAIN upstream) instead of diffing stale bytes
		// against current content and silently overwriting the other
		// participant's accepted edit.
		newHash, err := s.handler.WriteGuarded(req.Path, body, req.BaseHash)
		if err != nil {
			return errorResponse(req.ID, err)
		}
		res.Hash = newHash
	case TypeCreate:
		if err := s.handler.Create(req.Path, req.Mode); err != nil {
			return errorResponse(req.ID, err)
		}
	case TypeMkdir:
		if err := s.handler.Mkdir(req.Path, req.Mode); err != nil {
			return errorResponse(req.ID, err)
		}
	case TypeRemove:
		if err := s.handler.Remove(req.Path); err != nil {
			return errorResponse(req.ID, err)
		}
	case TypeRename:
		if err := s.handler.Rename(req.Path, req.To, req.RenameNoReplace); err != nil {
			return errorResponse(req.ID, err)
		}
	case TypeChmod:
		if err := s.handler.Chmod(req.Path, req.Mode); err != nil {
			return errorResponse(req.ID, err)
		}
	default:
		return errorResponse(req.ID, fmt.Errorf("unknown request type %q", req.Type))
	}
	return res
}

// BroadcastInvalidate pushes a path invalidation to every connected mount
// BroadcastRename tells every connected mount that a REMOTE rename
// moved path to newPath: open inodes rekey (their handles stay
// valid), both directory entries invalidate. Same bounded-write and
// drop-on-failure policy as BroadcastInvalidate.
func (s *Server) BroadcastRename(path, newPath string) {
	s.pushAll(Response{Push: true, Type: PushRename, Path: path, NewPath: newPath})
}

func (s *Server) BroadcastInvalidate(path string) {
	s.pushAll(Response{Push: true, Type: "invalidate", Path: path})
}

// pushAll writes one push frame to every connected mount with the
// bounded-write policy the direct broadcasts use (5s deadline, drop
// the mount on failure so the event pump never blocks).
func (s *Server) pushAll(r Response) {
	s.mu.Lock()
	conns := make([]*serverConn, 0, len(s.conns))
	for sc := range s.conns {
		conns = append(conns, sc)
	}
	s.mu.Unlock()
	for _, sc := range conns {
		sc.mu.Lock()
		// Bounded push writes with a deadline: this broadcast runs
		// directly from the room WebSocket's OnEvent callback, and one
		// mount that stops reading while holding its socket open would
		// block the flush indefinitely — stalling the event pump for
		// EVERY room participant. A timed-out or failing push DROPS the
		// connection (its serve loop notices the closed socket and
		// unregisters; the mount reconnects and re-reads).
		if dl, ok := sc.conn.(interface{ SetWriteDeadline(time.Time) error }); ok {
			_ = dl.SetWriteDeadline(time.Now().Add(5 * time.Second))
		}
		err := WriteFrame(sc.writer, r, nil)
		if err == nil {
			err = sc.writer.Flush()
		}
		if dl, ok := sc.conn.(interface{ SetWriteDeadline(time.Time) error }); ok {
			_ = dl.SetWriteDeadline(time.Time{})
		}
		sc.mu.Unlock()
		if err != nil {
			s.log.Warn("roomfs push failed; dropping mount", "path", r.Path, "err", err)
			_ = sc.conn.Close()
		}
	}
}

func errorResponse(id uint64, err error) Response {
	msg := err.Error()
	if msg == "" {
		msg = "unknown error"
	}
	return Response{ID: id, OK: false, Error: msg}
}
