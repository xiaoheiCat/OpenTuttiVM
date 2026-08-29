package roomfs

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// memHandler is a reference Handler for protocol tests: an in-memory tree
// with POSIX-ish semantics, standing in for room-sync's replica-backed
// adapter.
type memHandler struct {
	mu       sync.Mutex
	files    map[string][]byte
	dirs     map[string]bool
	rejectOn string // path whose writes are rejected by the room
	srv      *Server
}

// testContentHash mirrors the replica's content hash format without
// crossing the module boundary.
func testContentHash(content []byte) string {
	sum := sha256.Sum256(content)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func newMemHandler() *memHandler {
	return &memHandler{files: map[string][]byte{}, dirs: map[string]bool{}}
}

func (m *memHandler) Chmod(path string, mode uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.files[path]; !ok && !m.dirs[path] {
		return fmt.Errorf("no such path %s", path)
	}
	return nil
}

func (m *memHandler) Stat(path string) (*Stat, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if content, ok := m.files[path]; ok {
		return &Stat{Exists: true, Size: int64(len(content)), Mode: 0o644}, nil
	}
	if m.dirs[path] {
		return &Stat{Exists: true, Dir: true, Mode: 0o755}, nil
	}
	return &Stat{}, nil
}

func (m *memHandler) Read(path string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	content, ok := m.files[path]
	if !ok {
		return nil, fmt.Errorf("no such file %s", path)
	}
	return content, nil
}

func (m *memHandler) List(path string) ([]DirEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	prefix := ""
	if path != "" {
		prefix = path + "/"
	}
	type child struct {
		name string
		dir  bool
	}
	children := map[string]child{}
	// Files imply their ancestor directories.
	for name := range m.files {
		if !strings.HasPrefix(name, prefix) || len(name) == len(prefix) {
			continue
		}
		seg, isDir := firstSegment(name[len(prefix):])
		children[seg] = child{seg, isDir}
	}
	for name := range m.dirs {
		if !strings.HasPrefix(name, prefix) || len(name) == len(prefix) {
			continue
		}
		seg, _ := firstSegment(name[len(prefix):])
		c := children[seg]
		c.dir = true
		children[seg] = c
	}
	out := make([]DirEntry, 0, len(children))
	for _, c := range children {
		out = append(out, DirEntry{Name: c.name, Dir: c.dir})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func firstSegment(rest string) (string, bool) {
	for i := 0; i < len(rest); i++ {
		if rest[i] == '/' {
			return rest[:i], true
		}
	}
	return rest, false
}

func (m *memHandler) ReadWithHash(path string) ([]byte, string, error) {
	content, err := m.Read(path)
	if err != nil {
		return nil, "", err
	}
	return content, testContentHash(content), nil
}

func (m *memHandler) WriteGuarded(path string, content []byte, baseHash string) (string, error) {
	if baseHash != "" {
		cur, err := m.Read(path)
		if err == nil && testContentHash(cur) != baseHash {
			return "", fmt.Errorf("roomfs: stale base for %s; re-read and retry", path)
		}
	}
	if err := m.Write(path, content); err != nil {
		return "", err
	}
	return testContentHash(content), nil
}

func (m *memHandler) Write(path string, content []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.rejectOn == path {
		return fmt.Errorf("rejected")
	}
	m.files[path] = append([]byte(nil), content...)
	m.notify(path)
	return nil
}

func (m *memHandler) notify(path string) {
	if m.srv != nil {
		m.srv.BroadcastInvalidate(path)
	}
}

func (m *memHandler) Create(path string, mode uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.files[path] = []byte{}
	return nil
}

func (m *memHandler) Mkdir(path string, mode uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dirs[path] = true
	return nil
}

func (m *memHandler) Remove(path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.files, path)
	delete(m.dirs, path)
	return nil
}

func (m *memHandler) Rename(from, to string, noReplace bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if content, ok := m.files[from]; ok {
		if noReplace {
			if _, exists := m.files[to]; exists {
				return fmt.Errorf("EEXIST: %s", to)
			}
		}
		delete(m.files, from)
		m.files[to] = content
		return nil
	}
	return fmt.Errorf("no such file %s", from)
}

// startServer runs Serve on an ephemeral unix socket. macOS caps unix
// socket paths at ~104 bytes, so tests avoid the long TMPDIR layout.
func startServer(t *testing.T, h *memHandler) string {
	t.Helper()
	// The Room FS Protocol rides unix sockets inside the Linux room
	// containers; Windows hosts reach rooms over the network instead.
	if runtime.GOOS == "windows" {
		t.Skip("roomfs unix-socket protocol targets Linux containers")
	}
	dir, err := os.MkdirTemp("/tmp", "roomfs-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "r.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	srv := NewServer(h, slog.New(slog.DiscardHandler))
	h.srv = srv
	go srv.Serve(ln)
	return sock
}

func TestProtocolRoundTrip(t *testing.T) {
	h := newMemHandler()
	h.files["src/app.ts"] = []byte("export const ready = true\n")
	h.files["README.md"] = []byte("# room\n")
	h.dirs["src"] = true
	sock := startServer(t, h)

	c, err := dialTest(sock)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Stat a file and a missing path.
	st, err := c.Stat("src/app.ts")
	if err != nil || !st.Exists || st.Size != int64(len("export const ready = true\n")) {
		t.Fatalf("stat: %+v err=%v", st, err)
	}
	missing, err := c.Stat("nope.txt")
	if err != nil || missing.Exists {
		t.Fatalf("missing stat: %+v err=%v", missing, err)
	}

	// Read bytes back unharmed (no JSON/base64 round-trip of content).
	content, err := c.Read("src/app.ts")
	if err != nil || string(content) != "export const ready = true\n" {
		t.Fatalf("read: %q err=%v", content, err)
	}

	// Directory listing collapses children.
	entries, err := c.List("")
	if err != nil || len(entries) != 2 {
		t.Fatalf("list: %+v err=%v", entries, err)
	}

	// Mutations flow through.
	if err := c.Create("notes/todo.txt", 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write("notes/todo.txt", []byte("buy milk"), ""); err != nil {
		t.Fatal(err)
	}
	back, _ := c.Read("notes/todo.txt")
	if string(back) != "buy milk" {
		t.Fatalf("write round-trip: %q", back)
	}
	if err := c.Rename("notes/todo.txt", "notes/done.txt", false); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Stat("notes/todo.txt"); err != nil {
		t.Fatal(err)
	}
	if err := c.Remove("notes/done.txt"); err != nil {
		t.Fatal(err)
	}
	if st, _ := c.Stat("notes/done.txt"); st.Exists {
		t.Fatal("remove did not stick")
	}
}

func TestBinaryBodiesSurviveFraming(t *testing.T) {
	h := newMemHandler()
	sock := startServer(t, h)
	c, err := dialTest(sock)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Bytes with zeros, high bytes, and invalid UTF-8.
	blob := []byte{0x00, 0xff, 0xfe, 0x89, 'a', 0x00, 0x80}
	if _, err := c.Write("bin/logo.png", blob, ""); err != nil {
		t.Fatal(err)
	}
	back, err := c.Read("bin/logo.png")
	if err != nil || len(back) != len(blob) {
		t.Fatalf("binary round-trip len=%d err=%v", len(back), err)
	}
	for i := range blob {
		if back[i] != blob[i] {
			t.Fatalf("byte %d: %02x != %02x", i, back[i], blob[i])
		}
	}
}

func TestRejectionSurfacesAsErrRejected(t *testing.T) {
	h := newMemHandler()
	h.rejectOn = "guarded.txt"
	sock := startServer(t, h)
	c, err := dialTest(sock)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if _, err := c.Write("guarded.txt", []byte("x"), ""); err == nil || err.Error() == "" {
		t.Fatalf("expected rejection error, got %v", err)
	}
}

func TestInvalidationPush(t *testing.T) {
	h := newMemHandler()
	sock := startServer(t, h)

	watcher, err := dialTest(sock)
	if err != nil {
		t.Fatal(err)
	}
	defer watcher.Close()

	got := make(chan string, 1)
	watcher.OnInvalidate = func(path string) { got <- path }

	// A second connection writes; the watcher gets pushed.
	writer, err := dialTest(sock)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if _, err := writer.Write("shared/plan.md", []byte("v2"), ""); err != nil {
		t.Fatal(err)
	}
	select {
	case path := <-got:
		if path != "shared/plan.md" {
			t.Fatalf("invalidated %q", path)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("invalidation push never arrived")
	}
}

// dialTest connects over the unix socket the tests serve; transport
// selection lives with consumers now.
func dialTest(addr string) (*Client, error) {
	conn, err := net.Dial("unix", addr)
	if err != nil {
		return nil, err
	}
	return NewClient(conn), nil
}
