package roomfs

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"sync"
)

// ErrRejected reports a room-level rejection (base mismatch, barrier
// fencing); the mount surfaces it as EAGAIN so editors retry.
var ErrRejected = errors.New("room rejected the operation")

// Client is the mount's connection to room-sync.
type Client struct {
	conn net.Conn
	rw   *bufio.ReadWriter

	mu      sync.Mutex
	pending map[uint64]chan Response
	next    uint64
	closed  error

	// OnInvalidate receives remote-change notifications (path-level).
	OnInvalidate func(path string)
}

// NewClient builds the protocol client over an established connection:
// transport selection (unix socket vs TCP, and the host-specific address
// forms) is the consuming service's adapter concern, not this shared
// workspace package's policy.
func NewClient(conn net.Conn) *Client {
	c := &Client{
		conn:    conn,
		rw:      bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)),
		pending: map[uint64]chan Response{},
	}
	go c.pump()
	return c
}

// call issues one request and awaits its response.
func (c *Client) call(req Request, body []byte) (*Response, error) {
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
	// The complete write+flush sequence stays under the lock: FUSE issues
	// concurrent calls, and interleaved frames would corrupt the shared
	// bufio writer and misframe every response.
	if err := WriteFrame(c.rw.Writer, req, body); err != nil {
		c.mu.Unlock()
		c.fail(req.ID, err)
		return nil, err
	}
	if err := c.rw.Writer.Flush(); err != nil {
		c.mu.Unlock()
		c.fail(req.ID, err)
		return nil, err
	}
	c.mu.Unlock()

	res := <-ch
	if !res.OK {
		if res.Error == "rejected" {
			return &res, fmt.Errorf("%w: %s", ErrRejected, res.Error)
		}
		return &res, fmt.Errorf("roomfs: %s", res.Error)
	}
	return &res, nil
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
			if res.Type == "invalidate" && c.OnInvalidate != nil {
				c.OnInvalidate(res.Path)
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
	res, err := c.call(Request{Type: TypeStat, Path: path}, nil)
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
	res, err := c.call(Request{Type: TypeRead, Path: path}, nil)
	if err != nil {
		return nil, err
	}
	return res.Body, nil
}

// List returns one directory's children.
func (c *Client) List(path string) ([]DirEntry, error) {
	res, err := c.call(Request{Type: TypeList, Path: path}, nil)
	if err != nil {
		return nil, err
	}
	return res.Entries, nil
}

// Write submits a whole-file write; room-sync converts it into an
// operation against its local replica.
func (c *Client) Write(path string, content []byte) error {
	_, err := c.call(Request{Type: TypeWrite, Path: path}, content)
	return err
}

// Create adds an empty file.
func (c *Client) Create(path string, mode uint32) error {
	_, err := c.call(Request{Type: TypeCreate, Path: path, Mode: mode}, nil)
	return err
}

// Mkdir adds a directory.
func (c *Client) Mkdir(path string, mode uint32) error {
	_, err := c.call(Request{Type: TypeMkdir, Path: path, Mode: mode}, nil)
	return err
}

// Remove deletes a file or empty directory.
func (c *Client) Remove(path string) error {
	_, err := c.call(Request{Type: TypeRemove, Path: path}, nil)
	return err
}

// Rename moves a path within the workspace.
func (c *Client) Rename(from, to string) error {
	_, err := c.call(Request{Type: TypeRename, Path: from, To: to}, nil)
	return err
}

// Chmod submits a permission-bit change.
func (c *Client) Chmod(path string, mode uint32) error {
	_, err := c.call(Request{Type: TypeChmod, Path: path, Mode: mode}, nil)
	return err
}

// Close ends the connection.
func (c *Client) Close() error { return c.conn.Close() }
