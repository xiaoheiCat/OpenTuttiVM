package roomfs

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

// ErrRejected reports a room-level rejection (base mismatch, barrier
// fencing); the mount surfaces it as EAGAIN so editors retry.
var ErrRejected = errors.New("room rejected the operation")
var ErrCallTimeout = errors.New("roomfs call timed out")

const defaultCallTimeout = 30 * time.Minute

// Client is the mount's connection to room-sync.
type Client struct {
	conn net.Conn
	rw   *bufio.ReadWriter

	mu            sync.Mutex
	writeMu       sync.Mutex
	pending       map[uint64]chan Response
	next          uint64
	closed        error
	capability    string
	handshakeDone chan struct{}
	handshakeErr  error

	// OnInvalidate receives remote-change notifications (path-level).
	OnInvalidate func(path string)
	// OnRename handles the rename-aware push: rekey open inodes.
	OnRename func(oldPath, newPath string)
}

// NewClient builds the protocol client over an established connection:
// transport selection (unix socket vs TCP, and the host-specific address
// forms) is the consuming service's adapter concern, not this shared
// workspace package's policy.
func NewClient(conn net.Conn, capability string) (*Client, error) {
	if capability == "" {
		_ = conn.Close()
		return nil, errors.New("roomfs capability required")
	}
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	c := &Client{
		conn:          conn,
		rw:            rw,
		pending:       map[uint64]chan Response{},
		capability:    capability,
		handshakeDone: make(chan struct{}),
	}
	go c.pump()
	go c.sendCapability()
	return c, nil
}

func (c *Client) sendCapability() {
	c.writeMu.Lock()
	err := WriteFrame(c.rw.Writer, capabilityHello{Type: capabilityHelloType, Token: c.capability}, nil)
	if err == nil {
		err = c.rw.Writer.Flush()
	}
	c.writeMu.Unlock()
	c.mu.Lock()
	c.handshakeErr = err
	close(c.handshakeDone)
	c.mu.Unlock()
}

// call issues one request and awaits its response.
func (c *Client) call(req Request, body []byte) (*Response, error) {
	return c.callContext(context.Background(), req, body)
}

// callContext bounds a single FUSE request. The default wrapper remains
// compatible, while callers with a request deadline can fail a half-open
// connection without waiting for the peer forever.
func (c *Client) callContext(ctx context.Context, req Request, body []byte) (*Response, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultCallTimeout)
		defer cancel()
	}
	if deadline, ok := ctx.Deadline(); ok {
		if dl, ok := c.conn.(interface{ SetWriteDeadline(time.Time) error }); ok {
			_ = dl.SetWriteDeadline(deadline)
			defer func() { _ = dl.SetWriteDeadline(time.Time{}) }()
		}
	}
	select {
	case <-c.handshakeDone:
		c.mu.Lock()
		err := c.handshakeErr
		c.mu.Unlock()
		if err != nil {
			if ctx.Err() != nil || isTimeout(err) {
				c.timeout()
				return nil, fmt.Errorf("%w: %v", ErrCallTimeout, ctx.Err())
			}
			return nil, err
		}
	case <-ctx.Done():
		c.timeout()
		return nil, fmt.Errorf("%w: %v", ErrCallTimeout, ctx.Err())
	}
	c.mu.Lock()
	if c.closed != nil {
		err := c.closed
		c.mu.Unlock()
		return nil, err
	}
	c.next++
	req.ID = c.next
	ch := make(chan Response, 1)
	c.pending[req.ID] = ch
	writeDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			// A blocked net.Conn write does not observe context cancellation;
			// close the single shared connection so it unblocks and all pending
			// callers are failed together.
			_ = c.conn.Close()
		case <-writeDone:
		}
	}()
	defer close(writeDone)
	// The complete write+flush sequence stays under the lock: FUSE issues
	// concurrent calls, and interleaved frames would corrupt the shared
	// bufio writer and misframe every response.
	c.writeMu.Lock()
	err := WriteFrame(c.rw.Writer, req, body)
	if err == nil {
		err = c.rw.Writer.Flush()
	}
	c.writeMu.Unlock()
	if err != nil {
		c.mu.Unlock()
		c.fail(req.ID, err)
		if ctx.Err() != nil || isTimeout(err) {
			c.timeout()
			return nil, fmt.Errorf("%w: %v", ErrCallTimeout, ctx.Err())
		}
		return nil, err
	}
	c.mu.Unlock()

	var res Response
	select {
	case res = <-ch:
	case <-ctx.Done():
		c.timeout()
		return nil, fmt.Errorf("%w: %v", ErrCallTimeout, ctx.Err())
	}
	if !res.OK {
		if res.Error == ErrCallTimeout.Error() {
			return &res, ErrCallTimeout
		}
		if res.ErrorCode == "EAGAIN" || res.Error == "rejected" {
			return &res, fmt.Errorf("%w: %s", ErrRejected, res.Error)
		}
		if res.ErrorCode == ErrorNotFound || res.Error == ErrorNotFound {
			return &res, fmt.Errorf("%s: %s", ErrorNotFound, res.Error)
		}
		return &res, fmt.Errorf("roomfs: %s", res.Error)
	}
	return &res, nil
}

// timeout fences the connection before closing it. Closing alone would leave
// concurrent callers waiting until pump observes EOF, and could briefly let a
// new request reuse a connection that has already timed out.
func (c *Client) timeout() {
	c.shutdown(ErrCallTimeout)
	_ = c.conn.Close()
}

func isTimeout(err error) bool {
	var timeout net.Error
	return errors.As(err, &timeout) && timeout.Timeout()
}

// CallContext is the context-aware form for integrations that need a
// per-operation deadline. Existing operation methods retain their API.
func (c *Client) CallContext(ctx context.Context, req Request, body []byte) (*Response, error) {
	return c.callContext(ctx, req, body)
}

func (c *Client) fail(id uint64, err error) {
	c.mu.Lock()
	ch := c.pending[id]
	delete(c.pending, id)
	c.mu.Unlock()
	if ch != nil {
		ch <- Response{ID: id, OK: false, Error: err.Error()}
	}
}

// pump reads frames: responses by id and pushes for invalidation.
func (c *Client) pump() {
	for {
		var res Response
		body, err := ReadFrame(c.rw.Reader, &res)
		if err != nil {
			c.shutdown(err)
			return
		}
		if res.Push {
			// Dispatch OFF the pump: the callbacks take inode locks,
			// and a pending read can hold the same lock while it waits
			// for a response only this loop can deliver — a synchronous
			// call would deadlock the connection.
			if res.Type == "invalidate" && c.OnInvalidate != nil {
				go c.OnInvalidate(res.Path)
			}
			if res.Type == PushRename && c.OnRename != nil && res.Path != "" && res.NewPath != "" {
				go c.OnRename(res.Path, res.NewPath)
			}
			continue
		}
		res.Body = body
		c.mu.Lock()
		ch := c.pending[res.ID]
		delete(c.pending, res.ID)
		c.mu.Unlock()
		if ch != nil {
			ch <- res
		}
	}
}

func (c *Client) shutdown(err error) {
	c.mu.Lock()
	if c.closed == nil {
		c.closed = err
	}
	pending := c.pending
	c.pending = map[uint64]chan Response{}
	c.mu.Unlock()
	for id, ch := range pending {
		ch <- Response{ID: id, OK: false, Error: err.Error()}
	}
}

// Stat fetches one path's metadata.
func (c *Client) Stat(path string) (*Stat, error) {
	return c.StatContext(context.Background(), path)
}
func (c *Client) StatContext(ctx context.Context, path string) (*Stat, error) {
	res, err := c.callContext(ctx, Request{Type: TypeStat, Path: path}, nil)
	if err != nil && !errors.Is(err, ErrRejected) {
		return nil, err
	}
	if res.Stat == nil {
		return nil, fmt.Errorf("stat %s: malformed response", path)
	}
	return res.Stat, nil
}

// Read returns one file's content.
func (c *Client) Read(path string) ([]byte, error) {
	content, _, err := c.ReadWithHash(path)
	return content, err
}

// ReadWithHash also returns the content hash the read observed (flush
// baselines for optimistic-concurrency writes).
func (c *Client) ReadWithHash(path string) ([]byte, string, error) {
	return c.ReadWithHashContext(context.Background(), path)
}
func (c *Client) ReadWithHashContext(ctx context.Context, path string) ([]byte, string, error) {
	res, err := c.callContext(ctx, Request{Type: TypeRead, Path: path}, nil)
	if err != nil {
		return nil, "", err
	}
	return res.Body, res.Hash, nil
}

// List returns one directory's children.
func (c *Client) List(path string) ([]DirEntry, error) {
	return c.ListContext(context.Background(), path)
}
func (c *Client) ListContext(ctx context.Context, path string) ([]DirEntry, error) {
	res, err := c.callContext(ctx, Request{Type: TypeList, Path: path}, nil)
	if err != nil {
		return nil, err
	}
	return res.Entries, nil
}

// Write submits a whole-file write; room-sync converts it into an
// operation against its local replica.
func (c *Client) Write(path string, content []byte, baseHash string) (string, error) {
	return c.WriteContext(context.Background(), path, content, baseHash)
}
func (c *Client) WriteContext(ctx context.Context, path string, content []byte, baseHash string) (string, error) {
	// Preflight the protocol bound: sending an oversized frame would
	// have the server reject the body length and close the stream,
	// poisoning the mount's single shared connection for every later
	// operation. Fail THIS operation instead.
	if uint64(len(content)) > MaxBodyBytes {
		return "", fmt.Errorf("roomfs: write of %s exceeds protocol body limit (%d bytes)", path, MaxBodyBytes)
	}
	res, err := c.callContext(ctx, Request{Type: TypeWrite, Path: path, BaseHash: baseHash}, content)
	if err != nil {
		return "", err
	}
	return res.Hash, nil
}

// Create adds an empty file.
func (c *Client) Create(path string, mode uint32) error {
	return c.CreateContext(context.Background(), path, mode)
}
func (c *Client) CreateContext(ctx context.Context, path string, mode uint32) error {
	_, err := c.callContext(ctx, Request{Type: TypeCreate, Path: path, Mode: mode}, nil)
	return err
}

// Mkdir adds a directory.
func (c *Client) Mkdir(path string, mode uint32) error {
	return c.MkdirContext(context.Background(), path, mode)
}
func (c *Client) MkdirContext(ctx context.Context, path string, mode uint32) error {
	_, err := c.callContext(ctx, Request{Type: TypeMkdir, Path: path, Mode: mode}, nil)
	return err
}

// Remove deletes a file or empty directory.
func (c *Client) Remove(path string) error {
	return c.RemoveContext(context.Background(), path)
}
func (c *Client) RemoveContext(ctx context.Context, path string) error {
	_, err := c.callContext(ctx, Request{Type: TypeRemove, Path: path}, nil)
	return err
}

// Rename moves a path within the workspace.
func (c *Client) Rename(from, to string, noReplace bool) error {
	return c.RenameContext(context.Background(), from, to, noReplace)
}
func (c *Client) RenameContext(ctx context.Context, from, to string, noReplace bool) error {
	_, err := c.callContext(ctx, Request{Type: TypeRename, Path: from, To: to, RenameNoReplace: noReplace}, nil)
	return err
}

// Chmod submits a permission-bit change.
func (c *Client) Chmod(path string, mode uint32) error {
	return c.ChmodContext(context.Background(), path, mode)
}
func (c *Client) ChmodContext(ctx context.Context, path string, mode uint32) error {
	_, err := c.callContext(ctx, Request{Type: TypeChmod, Path: path, Mode: mode}, nil)
	return err
}

// Close ends the connection.
func (c *Client) Close() error { return c.conn.Close() }
