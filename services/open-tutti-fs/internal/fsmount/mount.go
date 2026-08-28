//go:build linux

// Package fsmount mounts the room workspace as a FUSE filesystem inside
// agent-session containers. Reads pull from the local replica (lazily via
// room-sync); writes buffer in memory and flush as whole-file protocol
// writes — room-sync converts them into Room File Operations — so editors
// and CLIs see a normal POSIX tree while the room keeps a single
// server-authoritative history.
package fsmount

import (
	"context"
	"strings"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	roomfs "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-roomfs"
)

// Mount serves the workspace tree at dir until ctx ends.
func Mount(ctx context.Context, dir string, client *roomfs.Client) error {
	root := &roomNode{client: client}
	// Remote invalidations drop kernel caches for the touched path.
	client.OnInvalidate = func(path string) {
		root.invalidatePath(path)
	}
	server, err := fs.Mount(dir, root, &fs.Options{
		MountOptions: fuse.MountOptions{FsName: "open-tutti"},
	})
	if err != nil {
		return err
	}
	root.mu.Lock()
	root.server = server
	root.mu.Unlock()

	done := make(chan struct{})
	go func() {
		server.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		_ = server.Unmount()
		<-done
	case <-done:
	}
	return nil
}

// roomNode is the workspace root and every directory.
type roomNode struct {
	fs.Inode
	client *roomfs.Client
	// dirMode is the authoritative directory permission set
	// (server-reported or the creating mkdir): reporting a widened 0755
	// for a 0700 directory would let local processes rely on
	// permissions the room never granted.
	dirMode uint32

	mu     sync.Mutex
	server *fuse.Server
}

func (n *roomNode) invalidatePath(path string) {
	segs := strings.Split(strings.Trim(path, "/"), "/")
	if len(segs) == 0 || segs[0] == "" {
		n.RmAllChildren()
		return
	}
	parent := &n.Inode
	for i := 0; i < len(segs)-1; i++ {
		next := parent.GetChild(segs[i])
		if next == nil {
			return
		}
		parent = next
	}
	n.mu.Lock()
	server := n.server
	n.mu.Unlock()
	if server == nil {
		return
	}
	// Entry invalidation alone does not touch an already-open file: the
	// Go-side buffer and the kernel data cache would keep serving the
	// pre-edit bytes (and a later local flush would clobber the accepted
	// remote edit). Drop both caches for file targets.
	if child := parent.GetChild(segs[len(segs)-1]); child != nil {
		if fn, ok := child.Operations().(*fileNode); ok {
			fn.invalidate()
			server.InodeNotify(child.StableAttr().Ino, 0, -1)
		}
	}
	server.EntryNotify(parent.StableAttr().Ino, segs[len(segs)-1])
}

func (n *roomNode) path(child string) string {
	parent := n.Path(nil)
	if parent == "/" {
		return strings.TrimPrefix(child, "/")
	}
	return strings.TrimPrefix(parent+"/"+child, "/")
}

var (
	_ fs.NodeStatfser  = (*roomNode)(nil)
	_ fs.NodeLookuper  = (*roomNode)(nil)
	_ fs.NodeReaddirer = (*roomNode)(nil)
	_ fs.NodeMkdirer   = (*roomNode)(nil)
	_ fs.NodeCreater   = (*roomNode)(nil)
	_ fs.NodeUnlinker  = (*roomNode)(nil)
	_ fs.NodeRmdirer   = (*roomNode)(nil)
	_ fs.NodeRenamer   = (*roomNode)(nil)
	_ fs.NodeOpendirer = (*roomNode)(nil)
	_ fs.NodeGetattrer = (*roomNode)(nil)

	_ fs.NodeOpener    = (*fileNode)(nil)
	_ fs.FileReader    = (*fileNode)(nil)
	_ fs.NodeWriter    = (*fileNode)(nil)
	_ fs.NodeFlusher   = (*fileNode)(nil)
	_ fs.NodeGetattrer = (*fileNode)(nil)
	_ fs.NodeSetattrer = (*fileNode)(nil)
)

func (n *roomNode) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	out.Blocks = 1 << 30
	out.Bsize = 4096
	return 0
}

func (n *roomNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Attr.Mode = n.dirPerms() | syscall.S_IFDIR
	return 0
}

// dirPerms returns the node's directory permission bits (0755 default).
func (n *roomNode) dirPerms() uint32 {
	if m := n.dirMode & 0o7777; m != 0 {
		return m
	}
	return 0o755
}

func (n *roomNode) Opendir(ctx context.Context) syscall.Errno { return 0 }

func (n *roomNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	path := n.path(name)
	st, err := n.client.Stat(path)
	if err != nil || !st.Exists {
		return nil, syscall.ENOENT
	}
	if st.Dir {
		dirMode := st.Mode & 0o7777
		if dirMode == 0 {
			dirMode = 0o755
		}
		out.Attr.Mode = dirMode | syscall.S_IFDIR
		child := &roomNode{client: n.client, dirMode: dirMode}
		return n.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFDIR}), 0
	}
	fileMode := st.Mode & 0o7777
	if fileMode == 0 {
		fileMode = 0o644
	}
	out.Attr.Mode = fileMode | syscall.S_IFREG
	out.Attr.Size = uint64(st.Size)
	child := &fileNode{client: n.client, path: path, srvMode: fileMode, srvSize: st.Size}
	return n.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFREG}), 0
}

func (n *roomNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries, err := n.client.List(n.path(""))
	if err != nil {
		return nil, syscall.EIO
	}
	out := make([]fuse.DirEntry, 0, len(entries))
	for _, e := range entries {
		mode := uint32(syscall.S_IFREG)
		if e.Dir {
			mode = syscall.S_IFDIR
		}
		out = append(out, fuse.DirEntry{Name: e.Name, Mode: mode})
	}
	return fs.NewListDirStream(out), 0
}

func (n *roomNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if err := n.client.Mkdir(n.path(name), mode); err != nil {
		return nil, syscall.EIO
	}
	perms := mode & 0o7777
	if perms == 0 {
		perms = 0o755
	}
	out.Attr.Mode = perms | syscall.S_IFDIR
	child := &roomNode{client: n.client, dirMode: perms}
	return n.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFDIR}), 0
}

func (n *roomNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	path := n.path(name)
	if err := n.client.Create(path, mode); err != nil {
		return nil, nil, 0, syscall.EIO
	}
	out.Attr.Mode = 0o644 | syscall.S_IFREG
	child := &fileNode{client: n.client, path: path, buffer: []byte{}, loaded: true}
	return n.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFREG}), nil, 0, 0
}

func (n *roomNode) Unlink(ctx context.Context, name string) syscall.Errno {
	if err := n.client.Remove(n.path(name)); err != nil {
		return syscall.EIO
	}
	return 0
}

func (n *roomNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	if err := n.client.Remove(n.path(name)); err != nil {
		return syscall.EIO
	}
	return 0
}

func (n *roomNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	from := n.path(name)
	np, ok := newParent.(interface{ path(string) string })
	if !ok {
		return syscall.EIO
	}
	to := np.path(newName)
	if err := n.client.Rename(from, to); err != nil {
		return syscall.EIO
	}
	// The FUSE inode survives its rename, but fileNode captured its
	// path at lookup/creation: without a rekey, later reads/flushes/
	// chmods target the OLD path (or an unrelated replacement there).
	if child := n.GetChild(name); child != nil {
		if fn, ok := child.Operations().(*fileNode); ok {
			fn.mu.Lock()
			if fn.path == from {
				fn.path = to
			}
			fn.mu.Unlock()
		} else {
			rekeyTree(child, from, to)
		}
	}
	return 0
}

// rekeyTree rewrites every descendant fileNode's stored path after a
// directory rename (children keyed by name under the moved inode).
func rekeyTree(ino *fs.Inode, oldPrefix, newPrefix string) {
	for name, child := range ino.Children() {
		oldPath := oldPrefix + "/" + name
		newPath := newPrefix + "/" + name
		if fn, ok := child.Operations().(*fileNode); ok && fn.path == oldPath {
			fn.mu.Lock()
			fn.path = newPath
			fn.mu.Unlock()
		}
		rekeyTree(child, oldPath, newPath)
	}
}

// fileNode buffers one open file; flush submits the whole content.
type fileNode struct {
	fs.Inode
	client *roomfs.Client
	path   string

	mu     sync.Mutex
	buffer []byte
	loaded bool
	// dirty marks buffered content not yet acknowledged by the room: a
	// clean flush (touch, repeated flush of one handle) must succeed
	// without submitting a no-op whole-file write the room would reject.
	dirty bool
	// writeGen counts buffer mutations: a write landing while a flush
	// awaits its acknowledgement bumps the generation, so the flush may
	// only clear dirty when no newer bytes exist (else the next flush
	// would skip submitting them — silent data loss for a concurrent
	// editor).
	writeGen uint64
	// mode carries a setattr mode change into the next flush.
	mode uint32
	// srvMode is the authoritative permission bits from the protocol
	// metadata (executable scripts stay executable in every mount;
	// without it Getattr would flatten everything to 0644).
	srvMode uint32
	// srvSize is the authoritative size from Lookup: a looked-up but
	// unread file has no buffer, and Getattr must not report an empty
	// file to tools that only stat it.
	srvSize int64
}

// invalidate drops the cached content so the next read reloads the
// authoritative bytes (remote invalidation path). A handle that stays
// open re-reads lazily — Read reloads when it finds the buffer gone.
func (f *fileNode) invalidate() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.dirty {
		// A completed-but-unflushed Write already acknowledged success
		// to the local process: dropping the buffer here would make
		// the later Flush report success WITHOUT submitting those
		// bytes — silent data loss. Keep the pending content; the
		// Flush submits it and the room's base-hash guard turns a
		// genuinely stale edit into EAGAIN (editor retry), never a
		// silent drop.
		return
	}
	f.buffer = nil
	f.loaded = false
	f.dirty = false
}

func (f *fileNode) load() syscall.Errno {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.loaded {
		return 0
	}
	content, err := f.client.Read(f.path)
	if err != nil {
		return syscall.EIO
	}
	f.buffer = content
	f.loaded = true
	return 0
}

func (f *fileNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if errno := f.load(); errno != 0 {
		return nil, 0, errno
	}
	return nil, fuse.FOPEN_KEEP_CACHE, 0
}

func (f *fileNode) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// An invalidated (unloaded) buffer must reload on read: reporting
	// empty bytes to an already-open handle would corrupt what the
	// caller sees after a remote edit.
	if !f.loaded {
		content, err := f.client.Read(f.path)
		if err != nil {
			return nil, syscall.EIO
		}
		f.buffer = content
		f.loaded = true
	}
	if off >= int64(len(f.buffer)) {
		return fuse.ReadResultData(nil), 0
	}
	end := off + int64(len(dest))
	if end > int64(len(f.buffer)) {
		end = int64(len(f.buffer))
	}
	return fuse.ReadResultData(append([]byte(nil), f.buffer[off:end]...)), 0
}

func (f *fileNode) Write(ctx context.Context, fh fs.FileHandle, data []byte, off int64) (uint32, syscall.Errno) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dirty = true
	f.writeGen++
	end := off + int64(len(data))
	if int64(len(f.buffer)) < end {
		grown := make([]byte, end)
		copy(grown, f.buffer)
		f.buffer = grown
	}
	copy(f.buffer[off:], data)
	return uint32(len(data)), 0
}

func (f *fileNode) Flush(ctx context.Context, fh fs.FileHandle) syscall.Errno {
	f.mu.Lock()
	content := append([]byte(nil), f.buffer...)
	dirty := f.dirty
	gen := f.writeGen
	f.mu.Unlock()
	// Clean flushes (touch with no write, a second flush of one handle)
	// are successes: the room rejects same-content whole-file writes by
	// design, and surfacing that as EAGAIN fails legitimate operations.
	if !dirty {
		return 0
	}
	if err := f.client.Write(f.path, content); err != nil {
		// Room-level rejections (base mismatch, barrier fencing) map to
		// EAGAIN so editors retry against the fresh revision.
		return syscall.EAGAIN
	}
	f.mu.Lock()
	// Clear dirty only when no write landed while we awaited the
	// acknowledgement; newer bytes keep the node dirty for the next
	// flush.
	if f.writeGen == gen {
		f.dirty = false
	}
	f.mu.Unlock()
	return 0
}

func (f *fileNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.loaded {
		out.Attr.Size = uint64(len(f.buffer))
	} else {
		// Looked up but not read (or invalidated): report the
		// authoritative size rather than pretending the file is empty.
		out.Attr.Size = uint64(f.srvSize)
	}
	switch {
	case f.mode != 0:
		// f.mode stores permission bits from Setattr: OR the regular
		// file type back in, or st_mode loses its type after chmod.
		out.Attr.Mode = (f.mode & 0o7777) | syscall.S_IFREG
	case f.srvMode != 0:
		out.Attr.Mode = f.srvMode | syscall.S_IFREG
	default:
		out.Attr.Mode = 0o644 | syscall.S_IFREG
	}
	return 0
}

func (f *fileNode) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	// Honor truncation: a requested size change resizes the buffered
	// content so a later flush cannot resurrect the old tail. Mode
	// changes ride the next flush's metadata.
	if sz, ok := in.GetSize(); ok {
		f.mu.Lock()
		if !f.loaded {
			// Standalone truncate(2) on an unopened node: load the
			// authoritative content first — resizing an empty buffer
			// would fabricate zeros or drop real bytes.
			content, err := f.client.Read(f.path)
			if err != nil {
				f.mu.Unlock()
				return syscall.EIO
			}
			f.buffer = content
			f.loaded = true
		}
		f.dirty = true
		f.writeGen++
		switch {
		case int64(sz) < int64(len(f.buffer)):
			f.buffer = append([]byte(nil), f.buffer[:sz]...)
		case int64(sz) > int64(len(f.buffer)):
			grown := make([]byte, sz)
			copy(grown, f.buffer)
			f.buffer = grown
		}
		f.srvSize = int64(sz)
		content := append([]byte(nil), f.buffer...)
		gen := f.writeGen
		f.mu.Unlock()
		// Without an open handle there is no guaranteed later flush:
		// submit the resize now or truncate(2) "succeeds" while the
		// authoritative content never changes.
		if fh == nil {
			if err := f.client.Write(f.path, content); err != nil {
				return syscall.EAGAIN
			}
			f.mu.Lock()
			// Same race as Flush: a concurrent Write while the resize
			// awaited its acknowledgement must keep the node dirty.
			if f.writeGen == gen {
				f.dirty = false
			}
			f.mu.Unlock()
		}
	}
	if mode, ok := in.GetMode(); ok {
		perms := mode & 0o7777
		f.mu.Lock()
		f.mode = perms
		f.mu.Unlock()
		// The change must reach the authoritative workspace: a local
		// assignment alone reverts on the next invalidation and never
		// reaches other participants.
		if err := f.client.Chmod(f.path, perms); err != nil {
			return syscall.EIO
		}
	}
	return f.Getattr(ctx, fh, out)
}
