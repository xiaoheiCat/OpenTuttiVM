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
	out.Attr.Mode = 0o755 | syscall.S_IFDIR
	return 0
}

func (n *roomNode) Opendir(ctx context.Context) syscall.Errno { return 0 }

func (n *roomNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	path := n.path(name)
	st, err := n.client.Stat(path)
	if err != nil || !st.Exists {
		return nil, syscall.ENOENT
	}
	if st.Dir {
		out.Attr.Mode = 0o755 | syscall.S_IFDIR
		child := &roomNode{client: n.client}
		return n.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFDIR}), 0
	}
	fileMode := st.Mode & 0o7777
	if fileMode == 0 {
		fileMode = 0o644
	}
	out.Attr.Mode = fileMode | syscall.S_IFREG
	out.Attr.Size = uint64(st.Size)
	child := &fileNode{client: n.client, path: path, srvMode: fileMode}
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
	out.Attr.Mode = 0o755 | syscall.S_IFDIR
	child := &roomNode{client: n.client}
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
	if err := n.client.Rename(from, np.path(newName)); err != nil {
		return syscall.EIO
	}
	return 0
}

// fileNode buffers one open file; flush submits the whole content.
type fileNode struct {
	fs.Inode
	client *roomfs.Client
	path   string

	mu     sync.Mutex
	buffer []byte
	loaded bool
	// mode carries a setattr mode change into the next flush.
	mode uint32
	// srvMode is the authoritative permission bits from the protocol
	// metadata (executable scripts stay executable in every mount;
	// without it Getattr would flatten everything to 0644).
	srvMode uint32
}

// invalidate drops the cached content so the next read reloads the
// authoritative bytes (remote invalidation path).
func (f *fileNode) invalidate() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.buffer = nil
	f.loaded = false
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
	f.mu.Unlock()
	if err := f.client.Write(f.path, content); err != nil {
		// Room-level rejections (base mismatch, barrier fencing) map to
		// EAGAIN so editors retry against the fresh revision.
		return syscall.EAGAIN
	}
	return 0
}

func (f *fileNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	f.mu.Lock()
	defer f.mu.Unlock()
	out.Attr.Size = uint64(len(f.buffer))
	switch {
	case f.mode != 0:
		out.Attr.Mode = f.mode
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
		switch {
		case int(sz) < len(f.buffer):
			f.buffer = append([]byte(nil), f.buffer[:sz]...)
		case int(sz) > len(f.buffer):
			grown := make([]byte, sz)
			copy(grown, f.buffer)
			f.buffer = grown
		}
		f.mu.Unlock()
	}
	if mode, ok := in.GetMode(); ok {
		f.mu.Lock()
		f.mode = mode
		f.mu.Unlock()
	}
	return f.Getattr(ctx, fh, out)
}
