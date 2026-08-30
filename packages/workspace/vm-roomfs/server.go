package roomfs

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net"
	"strings"
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

// ContextHandler is the production request boundary. Implementations should
// stop slow CAS and network work when the mount request is canceled.
type ContextHandler interface {
	StatContext(context.Context, string) (*Stat, error)
	ReadWithHashContext(context.Context, string) ([]byte, string, error)
	WriteGuardedContext(context.Context, string, []byte, string) (string, error)
	ListContext(context.Context, string) ([]DirEntry, error)
	CreateContext(context.Context, string, uint32) error
	MkdirContext(context.Context, string, uint32) error
	RemoveContext(context.Context, string) error
	RenameContext(context.Context, string, string, bool) error
	ChmodContext(context.Context, string, uint32) error
}

// Server hosts the protocol on a listener and can push invalidations to
// every connected mount when remote operations land.
type Server struct {
	handler    Handler
	log        *slog.Logger
	capability string

	mu    sync.Mutex
	conns map[*serverConn]struct{}
	auth  chan struct{}
}

const maxActiveRequests = 1024

type serverConn struct {
	conn       net.Conn
	writer     *bufio.Writer
	mu         sync.Mutex
	requestsMu sync.Mutex
	requests   map[uint64]context.CancelFunc
}

// NewServer creates a protocol server over a handler. The handler may be
// attached later with SetHandler when it needs the server first (e.g. to
// broadcast invalidations).
func NewServer(h Handler, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		panic(fmt.Sprintf("roomfs capability generation: %v", err))
	}
	return &Server{handler: h, log: log, capability: hex.EncodeToString(raw[:]), conns: map[*serverConn]struct{}{}, auth: make(chan struct{}, 64)}
}

// NewServerWithCapability provisions a capability at the process boundary so
// a separately launched mount can receive it through a private channel.
func NewServerWithCapability(h Handler, log *slog.Logger, capability string) *Server {
	if capability == "" {
		panic("roomfs capability required")
	}
	return &Server{handler: h, log: log, capability: capability, conns: map[*serverConn]struct{}{}, auth: make(chan struct{}, 64)}
}

// Capability is the per-process secret required by every mount connection.
func (s *Server) Capability() string { return s.capability }

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
		sc := &serverConn{conn: conn, writer: bufio.NewWriter(conn), requests: map[uint64]context.CancelFunc{}}
		go s.authenticateAndServe(conn, sc)
	}
}

func (s *Server) authenticateAndServe(conn net.Conn, sc *serverConn) {
	select {
	case s.auth <- struct{}{}:
	default:
		_ = conn.Close()
		return
	}
	defer func() { <-s.auth }()
	reader := bufio.NewReader(conn)
	var hello capabilityHello
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if err := ReadCapabilityHello(reader, &hello); err != nil || hello.Type != capabilityHelloType || hello.Token == "" || hello.Token != s.capability {
		_ = conn.Close()
		return
	}
	_ = conn.SetReadDeadline(time.Time{})
	s.mu.Lock()
	s.conns[sc] = struct{}{}
	s.mu.Unlock()
	s.serveConnReader(conn, sc, reader)
}

func (s *Server) serveConn(conn net.Conn, sc *serverConn) {
	s.serveConnReader(conn, sc, bufio.NewReader(conn))
}

func (s *Server) serveConnReader(conn net.Conn, sc *serverConn, reader *bufio.Reader) {
	connCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer sc.cancelRequests()
	defer conn.Close()
	defer func() {
		s.mu.Lock()
		delete(s.conns, sc)
		s.mu.Unlock()
	}()

	var wg sync.WaitGroup
	for {
		var req Request
		body, err := ReadFrame(reader, &req)
		if err != nil {
			cancel()
			wg.Wait()
			return
		}
		reqCopy := req
		bodyCopy := append([]byte(nil), body...)
		if req.Type == TypeCancel {
			sc.cancelRequest(req.ID)
			continue
		}
		requestCtx, requestCancel := context.WithCancel(connCtx)
		if !sc.addRequest(req.ID, requestCancel) {
			requestCancel()
			go func() {
				if err := s.writeResponse(sc, errorResponse(req.ID, fmt.Errorf("roomfs: too many active requests"))); err != nil {
					cancel()
				}
			}()
			continue
		}
		wg.Add(1)
		go func(req Request, body []byte, requestCtx context.Context) {
			defer wg.Done()
			defer sc.removeRequest(req.ID)
			res := s.dispatch(requestCtx, req, body)
			writeErr := s.writeResponse(sc, res)
			if writeErr != nil {
				cancel()
				_ = conn.Close()
			}
		}(reqCopy, bodyCopy, requestCtx)
	}
}

func (sc *serverConn) addRequest(id uint64, cancel context.CancelFunc) bool {
	sc.requestsMu.Lock()
	defer sc.requestsMu.Unlock()
	if len(sc.requests) >= maxActiveRequests {
		return false
	}
	if _, exists := sc.requests[id]; exists {
		return false
	}
	sc.requests[id] = cancel
	return true
}

func (sc *serverConn) removeRequest(id uint64) {
	sc.requestsMu.Lock()
	delete(sc.requests, id)
	sc.requestsMu.Unlock()
}

func (sc *serverConn) cancelRequest(id uint64) {
	sc.requestsMu.Lock()
	cancel := sc.requests[id]
	sc.requestsMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (sc *serverConn) cancelRequests() {
	sc.requestsMu.Lock()
	requests := sc.requests
	sc.requests = map[uint64]context.CancelFunc{}
	sc.requestsMu.Unlock()
	for _, cancel := range requests {
		cancel()
	}
}

func (s *Server) writeResponse(sc *serverConn, res Response) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if dl, ok := sc.conn.(interface{ SetWriteDeadline(time.Time) error }); ok {
		_ = dl.SetWriteDeadline(time.Now().Add(30 * time.Second))
		defer func() { _ = dl.SetWriteDeadline(time.Time{}) }()
	}
	err := WriteFrame(sc.writer, res, res.Body)
	if err == nil {
		err = sc.writer.Flush()
	}
	return err
}

func (s *Server) dispatch(ctx context.Context, req Request, body []byte) Response {
	res := Response{ID: req.ID, OK: true}
	if h, ok := s.handler.(ContextHandler); ok {
		switch req.Type {
		case TypeStat:
			st, err := h.StatContext(ctx, req.Path)
			if err != nil {
				return errorResponse(req.ID, err)
			}
			if st == nil {
				st = &Stat{}
			}
			res.Stat = st
		case TypeRead:
			content, hash, err := h.ReadWithHashContext(ctx, req.Path)
			if err != nil {
				return errorResponse(req.ID, err)
			}
			if uint64(len(content)) > MaxBodyBytes {
				return errorResponse(req.ID, fmt.Errorf("roomfs: read of %s exceeds protocol body limit (%d bytes)", req.Path, MaxBodyBytes))
			}
			res.Body, res.Hash = content, hash
		case TypeList:
			entries, err := h.ListContext(ctx, req.Path)
			if err != nil {
				return errorResponse(req.ID, err)
			}
			res.Entries = entries
		case TypeWrite:
			newHash, err := h.WriteGuardedContext(ctx, req.Path, body, req.BaseHash)
			if err != nil {
				return errorResponse(req.ID, err)
			}
			res.Hash = newHash
		case TypeCreate:
			if err := h.CreateContext(ctx, req.Path, req.Mode); err != nil {
				return errorResponse(req.ID, err)
			}
		case TypeMkdir:
			if err := h.MkdirContext(ctx, req.Path, req.Mode); err != nil {
				return errorResponse(req.ID, err)
			}
		case TypeRemove:
			if err := h.RemoveContext(ctx, req.Path); err != nil {
				return errorResponse(req.ID, err)
			}
		case TypeRename:
			if err := h.RenameContext(ctx, req.Path, req.To, req.RenameNoReplace); err != nil {
				return errorResponse(req.ID, err)
			}
		case TypeChmod:
			if err := h.ChmodContext(ctx, req.Path, req.Mode); err != nil {
				return errorResponse(req.ID, err)
			}
		default:
			return errorResponse(req.ID, fmt.Errorf("unknown request type %q", req.Type))
		}
		return res
	}
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
	code := ErrorIO
	for _, candidate := range []string{ErrorNotFound, ErrorExists, ErrorNotEmpty, ErrorAgain, ErrorIO} {
		if strings.HasPrefix(msg, candidate+":") {
			code = candidate
			break
		}
	}
	return Response{ID: id, OK: false, Error: msg, ErrorCode: code}
}
