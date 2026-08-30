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
	"errors"
	"strings"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	roomfs "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-roomfs"
)

func roomErrno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	if errors.Is(err, roomfs.ErrRejected) {
		return syscall.EAGAIN
	}
	// Only a stable room decision is retryable. Timeouts, EOFs, transport
	// failures, and CAS/fetch errors are not evidence that the caller's data
	// conflicted and must not be turned into an unsafe retry signal.
	for _, item := range []struct {
		code  string
		errno syscall.Errno
	}{{"ENOENT", syscall.ENOENT}, {"EEXIST", syscall.EEXIST}, {"ENOTEMPTY", syscall.ENOTEMPTY}, {"EAGAIN", syscall.EAGAIN}, {"EIO", syscall.EIO}} {
		if strings.HasPrefix(err.Error(), item.code+":") || err.Error() == item.code {
			return item.errno
		}
	}
	return syscall.EIO
}

func roomWriteErrno(err error) syscall.Errno {
	if errors.Is(err, roomfs.ErrRejected) || strings.HasPrefix(err.Error(), roomfs.ErrorAgain) {
		return syscall.EAGAIN
	}
	return syscall.EIO
}

// Mount serves the workspace tree at dir until ctx ends.
func Mount(ctx context.Context, dir string, client *roomfs.Client) error {
	root := &roomNode{client: client}
	// Remote invalidations drop kernel caches for the touched path.
	client.OnInvalidate = func(path string) {
		root.invalidatePath(path)
	}
	// Remote RENAMES rekey open inodes instead of only dropping
	// caches: a surviving handle would keep its old stored path and
	// later reload or flush through it (EIO, or onto an unrelated
	// replacement later created at the old name). This mirrors the
	// local FUSE rename path's rekey semantics.
	client.OnRename = func(oldPath, newPath string) {
		root.rekeyRemote(oldPath, newPath)
	}
	server, err := fs.Mount(dir, root, &fs.Options{
		MountOptions: fuse.MountOptions{FsName: "open-tutti"},
		// Report permission bits VERBATIM: go-fuse's default setAttr
		// widens any mode whose permission bits are 0 to 0644 (+0111
		// for directories) on every Lookup/Getattr reply, silently
		// defeating the room-authoritative chmod 0000 this mount
		// explicitly preserves (a session-container process could then
		// read a file the room locked).
		NullPermissions: true,
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
	// permissions the room never granted. dirModeSet distinguishes an
	// explicit 0000 from "not yet known" (zero is valid).
	dirMode uint32
	// dirModeSet marks dirMode as authoritative (stat or mkdir).
	dirModeSet bool

	mu     sync.Mutex
	server *fuse.Server
}

// invalidateDescendants drops the cached file state of every FILE
// inode beneath one node (recursively), without touching the tree.
func invalidateDescendants(node *fs.Inode) {
	for _, child := range node.Children() {
		invalidateDescendants(child)
		if ops := child.Operations(); ops != nil {
			if fn, ok := ops.(*fileNode); ok {
				fn.invalidate()
			}
		}
	}
}

func (n *roomNode) invalidatePath(path string) {
	segs := strings.Split(strings.Trim(path, "/"), "/")
	if len(segs) == 0 || segs[0] == "" {
		// Whole-tree resync (reconnect bootstrap): RmAllChildren alone
		// drops DENTRIES, but an OPEN file inode keeps its fileNode and
		// buffered bytes — such a handle would keep reading
		// pre-bootstrap content and could later flush it over the
		// resynchronized authority, exactly what this invalidation
		// exists to prevent. Invalidate every cached descendant's file
		// state (dirty buffers survive per the invalidation contract;
		// stale clean ones die) before dropping the tree.
		invalidateDescendants(&n.Inode)
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
		} else if dir, ok := child.Operations().(*roomNode); ok {
			// Metadata invalidation must retire the cached directory mode too;
			// otherwise a remote chmod (including explicit 0000) is hidden by
			// the roomNode's old permissions after EntryNotify re-lookup.
			if st, err := n.client.StatContext(context.Background(), path); err == nil && st.Exists && st.Dir {
				dir.mu.Lock()
				dir.dirMode = st.Mode & 0o7777
				dir.dirModeSet = true
				dir.mu.Unlock()
			} else {
				dir.mu.Lock()
				dir.dirModeSet = false
				dir.dirMode = 0
				dir.mu.Unlock()
			}
			server.InodeNotify(child.StableAttr().Ino, 0, -1)
		}
	}
	server.EntryNotify(parent.StableAttr().Ino, segs[len(segs)-1])
}

func (n *roomNode) path(child string) string {
	// go-fuse Path(nil) returns "" for the ROOT and relative paths
	// below it, so a literal "/"-prefix guard never fires: for a
	// first-level directory with an empty child this produced "a/" —
	// a trailing slash that matched no tracked path (Readdir listed
	// every subdirectory as empty) and failed workspace-path
	// validation on directory chmods. Normalize instead of guarding.
	parent := strings.Trim(n.Path(nil), "/")
	child = strings.Trim(child, "/")
	if parent == "" {
		return child
	}
	if child == "" {
		return parent
	}
	return parent + "/" + child
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
	// Plausible free space: tools that preflight capacity (installers,
	// package managers, editors writing temp/swap files) saw 0 bytes
	// and 0 inodes free and failed ENOSPC inside the workspace even
	// though writes succeed.
	out.Blocks = 1 << 30
	out.Bfree = 1 << 30
	out.Bavail = 1 << 30
	out.Bsize = 4096
	out.Files = 1 << 24
	out.Ffree = 1 << 24
	return 0
}

func (n *roomNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Attr.Mode = n.dirPerms() | syscall.S_IFDIR
	return 0
}

// Setattr forwards directory permission changes: directory modes are
// carried by the protocol and snapshots, and without this hook a chmod
// on a directory never reached RoomFS and silently stayed local.
func (n *roomNode) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if mode, ok := in.GetMode(); ok {
		if err := n.client.ChmodContext(ctx, n.path(""), mode); err != nil {
			return roomErrno(err)
		}
		n.dirMode = mode & 0o7777
		n.dirModeSet = true
		out.Attr.Mode = n.dirPerms() | syscall.S_IFDIR
		return 0
	}
	return n.Getattr(ctx, fh, out)
}

// dirPerms returns the node's directory permission bits (0755 default
// until something authoritative arrives; an EXPLICIT 0000 stays 0000).
func (n *roomNode) dirPerms() uint32 {
	if n.dirModeSet {
		return n.dirMode & 0o7777
	}
	return 0o755
}

func (n *roomNode) Opendir(ctx context.Context) syscall.Errno { return 0 }

func (n *roomNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	path := n.path(name)
	st, err := n.client.StatContext(ctx, path)
	if err != nil {
		return nil, roomErrno(err)
	}
	if st == nil || !st.Exists {
		return nil, syscall.ENOENT
	}
	if st.Dir {
		// A zero Stat mode is a REAL synchronized 0000: the bridge
		// normalizes unset modes to defaults before they reach us.
		dirMode := st.Mode & 0o7777
		out.Attr.Mode = dirMode | syscall.S_IFDIR
		child := &roomNode{client: n.client, dirMode: dirMode, dirModeSet: true}
		return n.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFDIR}), 0
	}
	// A zero mode is a REAL synchronized chmod 0000 (the bridge
	// normalizes unset modes to defaults before they reach us).
	fileMode := st.Mode & 0o7777
	out.Attr.Mode = fileMode | syscall.S_IFREG
	out.Attr.Size = uint64(st.Size)
	child := &fileNode{client: n.client, path: path, srvMode: fileMode, srvModeSet: true, srvSize: st.Size}
	return n.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFREG}), 0
}

func (n *roomNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries, err := n.client.ListContext(ctx, n.path(""))
	if err != nil {
		return nil, roomErrno(err)
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
	if err := n.client.MkdirContext(ctx, n.path(name), mode); err != nil {
		return nil, roomErrno(err)
	}
	perms := mode & 0o7777
	// An explicit mkdir(..., 0000) is a VALID POSIX permission: by the
	// time the FUSE layer sees the mode, the kernel has applied umask
	// — zero here means the caller really asked for 0000, not
	// "default". The authoritative request already carries 0000; the
	// cached inode must not widen it to 0755.
	out.Attr.Mode = perms | syscall.S_IFDIR
	child := &roomNode{client: n.client, dirMode: perms, dirModeSet: true}
	return n.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFDIR}), 0
}

func (n *roomNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	path := n.path(name)
	if err := n.client.CreateContext(ctx, path, mode); err != nil {
		return nil, nil, 0, roomErrno(err)
	}
	// Report the REQUESTED permission bits, not a widened 0644: the
	// authoritative create received them, and local stat/permission
	// checks must not expose more than the creator asked for.
	perms := mode & 0o7777 // zero is a valid explicit 0000 (see Mkdir)
	out.Attr.Mode = perms | syscall.S_IFREG
	child := &fileNode{client: n.client, path: path, buffer: []byte{}, loaded: true, srvMode: perms, srvModeSet: true}
	return n.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFREG}), nil, 0, 0
}

func (n *roomNode) Unlink(ctx context.Context, name string) syscall.Errno {
	if err := n.client.RemoveContext(ctx, n.path(name)); err != nil {
		return roomErrno(err)
	}
	return 0
}

func (n *roomNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	if err := n.client.RemoveContext(ctx, n.path(name)); err != nil {
		return roomErrno(err)
	}
	return 0
}

func (n *roomNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	// renameat2 flags must not degrade into a plain replace: the
	// authoritative rename replaces an existing destination, so
	// RENAME_NOREPLACE would silently lose content the caller expected
	// to protect, and RENAME_EXCHANGE would perform half of a swap.
	// The RoomFS protocol has no flag representation — reject both
	// faithfully (EEXIST / EINVAL) until it grows one.
	const (
		renameNoreplace = 0x1
		renameExchange  = 0x2
		renameWhiteout  = 0x4
	)
	if flags&renameExchange != 0 || flags&renameWhiteout != 0 {
		return syscall.EINVAL
	}
	from := n.path(name)
	np, ok := newParent.(interface{ path(string) string })
	if !ok {
		return syscall.EIO
	}
	to := np.path(newName)
	// The no-replace condition travels to the AUTHORITATIVE rename
	// (flag in the protocol): a local Stat preflight raced a concurrent
	// create at the destination between the check and the submit, and a
	// plain rename would then replace content the caller forbade
	// replacing.
	if err := n.client.RenameContext(ctx, from, to, flags&renameNoreplace != 0); err != nil {
		return roomErrno(err)
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

// rekeyRemote handles a REMOTE rename push: rekey every cached inode
// whose stored path sits under oldPath (the FUSE inode survives, only
// its pathname moved), then invalidate both directory entries so both
// names re-lookup against the resynchronized authority.
func (n *roomNode) rekeyRemote(oldPath, newPath string) {
	oldPrefix := strings.Trim(oldPath, "/")
	if oldPrefix == "" {
		// Whole-tree resync equivalent: nothing sensible to rekey.
		return
	}
	segs := strings.Split(oldPrefix, "/")
	parent := &n.Inode
	for i := 0; i < len(segs)-1; i++ {
		if next := parent.GetChild(segs[i]); next != nil {
			parent = next
		} else {
			parent = nil
			break
		}
	}
	name := segs[len(segs)-1]
	if parent != nil {
		if child := parent.GetChild(name); child != nil {
			if fn, ok := child.Operations().(*fileNode); ok {
				fn.mu.Lock()
				if strings.Trim(fn.path, "/") == oldPrefix {
					fn.path = newPath
				}
				fn.mu.Unlock()
			} else {
				rekeyTree(child, oldPath, newPath)
			}
		}
	}
	// Both entries re-lookup (source disappears, destination appears;
	// a cached negative lookup there would keep hiding it).
	n.invalidatePath(oldPath)
	n.invalidatePath(newPath)
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
	// mode carries a setattr mode change into the next flush; modeSet
	// distinguishes an EXPLICIT chmod 0000 from "no local mode yet"
	// (zero is a valid synchronized permission).
	mode    uint32
	modeSet bool
	// srvMode is the authoritative permission bits from the protocol
	// metadata (executable scripts stay executable in every mount;
	// without it Getattr would flatten everything to 0644). srvModeSet
	// distinguishes an authoritative 0000 from "no server mode yet"
	// (zero is a valid synchronized permission).
	srvMode uint32
	// srvModeSet marks srvMode as authoritative (Lookup carried a mode).
	srvModeSet bool
	// srvSize is the authoritative size from Lookup: a looked-up but
	// unread file has no buffer, and Getattr must not report an empty
	// file to tools that only stat it.
	srvSize int64
	// bufBase is the content hash the CURRENT buffer was loaded from
	// (flush baselines): Flush submits it as the optimistic-concurrency
	// guard so a buffer invalidated mid-edit by a remote change fails
	// with EAGAIN instead of silently overwriting the newer revision.
	bufBase string
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
	// Retire the LOCAL mode cache too: a chmod cached before the
	// invalidation would keep outranking the remote change in
	// Getattr's preference order even after the re-stat refreshed the
	// authoritative bits.
	f.modeSet = false
	// Drop the cached authoritative size too: InodeNotify forces a
	// Getattr before any reload, and the stale pre-edit size would keep
	// answering stat/fstat with the old length after the accepted edit.
	// -1 is unstatable; the next Lookup/load restores the real value.
	f.srvSize = -1
}

func (f *fileNode) load() syscall.Errno {
	return f.loadContext(context.Background())
}

func (f *fileNode) loadContext(ctx context.Context) syscall.Errno {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.loaded {
		return 0
	}
	content, base, err := f.client.ReadWithHashContext(ctx, f.path)
	if err != nil {
		return roomErrno(err)
	}
	f.buffer = content
	f.bufBase = base
	f.loaded = true
	return 0
}

func (f *fileNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if errno := f.loadContext(ctx); errno != 0 {
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
		content, base, err := f.client.ReadWithHashContext(ctx, f.path)
		if err != nil {
			return nil, roomErrno(err)
		}
		f.buffer = content
		f.bufBase = base // invalidated reload refreshes the flush baseline too
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
	// Preflight BEFORE any allocation: the roomfs protocol caps a body
	// at MaxBodyBytes, and growing the buffer past it would OOM the
	// mount (taking the whole workspace offline) before the write path
	// could ever return EFBIG. The OFFSET is compared against the cap
	// first and the end derived by SUBTRACTION: near MaxInt64 the
	// addition wraps negative, slips past the check, and the later
	// buffer slice with the wrapped offset panics the FUSE process.
	if off < 0 || off > int64(roomfs.MaxBodyBytes) || int64(roomfs.MaxBodyBytes)-off < int64(len(data)) {
		return 0, syscall.EFBIG
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	// A partial write on an invalidated (unloaded) handle must RELOAD
	// first, exactly like Read: growing the nil buffer fabricates zero
	// padding, and the flush would then submit NUL bytes plus the
	// written span as the whole file — silent data loss accepted by
	// the base-hash guard (invalidations often keep the hash).
	if !f.loaded {
		content, base, err := f.client.ReadWithHashContext(ctx, f.path)
		if err != nil {
			return 0, roomErrno(err)
		}
		f.buffer = content
		f.bufBase = base
		f.loaded = true
	}
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
	// bufBase is guarded by f.mu everywhere else (load/Read-reload/
	// Setattr): snapshot it under the lock so the unlocked write call
	// below never races a concurrent reload handing a torn base hash
	// to the optimistic-concurrency guard.
	base := f.bufBase
	// path joins the same snapshot: rename rekeys write it under f.mu,
	// and reading it unlocked here could flush through the pre-rename
	// pathname — onto the old name or an unrelated later replacement.
	p := f.path
	f.mu.Unlock()
	// Clean flushes (touch with no write, a second flush of one handle)
	// are successes: the room rejects same-content whole-file writes by
	// design, and surfacing that as EAGAIN fails legitimate operations.
	if !dirty {
		return 0
	}
	newBase, err := f.client.WriteContext(ctx, p, content, base)
	if err != nil {
		return roomWriteErrno(err)
	}
	f.mu.Lock()
	if newBase != "" {
		f.bufBase = newBase
	}
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
	} else if f.srvSize >= 0 {
		// Looked up but not read: report the authoritative size rather
		// than pretending the file is empty.
		out.Attr.Size = uint64(f.srvSize)
	} else {
		// Invalidated by a remote edit: the cached size predates the
		// edit, so ask the room now (a stat-sized round trip, but
		// never a stale length on an accepted edit).
		st, err := f.client.StatContext(ctx, f.path)
		if err == nil && st != nil && st.Exists {
			f.srvSize = int64(st.Size)
			// Zero IS authoritative here: the bridge resolves unset
			// modes to readable defaults before the wire (an explicit
			// remote chmod 0000 is the only way Mode arrives 0), so
			// guarding on != 0 kept serving the pre-chmod permissions
			// forever after the invalidation.
			f.srvMode = st.Mode & 0o7777
			f.srvModeSet = true
			out.Attr.Size = uint64(f.srvSize)
		} else {
			// Preserve the last useful mode/size already placed in out, but
			// never report a successful stat for a failed refresh.
			if err == nil {
				if st == nil || !st.Exists {
					return syscall.ENOENT
				}
				return syscall.EIO
			}
			return roomErrno(err)
		}
	}
	switch {
	case f.modeSet:
		// f.mode stores permission bits from Setattr: OR the regular
		// file type back in, or st_mode loses its type after chmod.
		out.Attr.Mode = (f.mode & 0o7777) | syscall.S_IFREG
	case f.srvModeSet:
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
		if f.loaded && !f.dirty && int64(sz) == int64(len(f.buffer)) {
			// Same-size truncate is a POSIX no-op ONLY over a CLEAN
			// buffer: after a rejected standalone write (EAGAIN lost a
			// base-hash race, say) the buffer is resized but dirty and
			// fh == nil guarantees no later flush — a retry entering
			// this branch would report success for content the
			// authority never accepted. Dirty same-size retries fall
			// through and resubmit.
			f.mu.Unlock()
			return 0
		}
		if !f.loaded {
			// Standalone truncate(2) on an unopened node: load the
			// authoritative content first — resizing an empty buffer
			// would fabricate zeros or drop real bytes.
			content, base, err := f.client.ReadWithHashContext(ctx, f.path)
			if err != nil {
				f.mu.Unlock()
				return roomErrno(err)
			}
			f.buffer = content
			f.bufBase = base
			f.loaded = true
		}
		if sz < 0 || uint64(sz) > roomfs.MaxBodyBytes {
			f.mu.Unlock()
			return syscall.EFBIG
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
		// bufBase snapshot stays under the lock (same race as Flush).
		base := f.bufBase
		tp := f.path
		f.mu.Unlock()
		// Without an open handle there is no guaranteed later flush:
		// submit the resize now or truncate(2) "succeeds" while the
		// authoritative content never changes.
		if fh == nil {
			newBase, err := f.client.WriteContext(ctx, tp, content, base)
			if err != nil {
				return roomWriteErrno(err)
			}
			f.mu.Lock()
			if newBase != "" {
				f.bufBase = newBase
			}
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
		// path snapshot under the lock (rename rekeys race this read).
		f.mu.Lock()
		cp := f.path
		f.mu.Unlock()
		// The change must reach the authoritative workspace: a local
		// assignment alone reverts on the next invalidation and never
		// reaches other participants. Commit the cached mode ONLY
		// after acceptance — caching first kept reporting uncommitted
		// (possibly WIDENED) permissions after a rejected Chmod.
		if err := f.client.ChmodContext(ctx, cp, perms); err != nil {
			return roomErrno(err)
		}
		f.mu.Lock()
		f.mode = perms
		f.modeSet = true
		f.mu.Unlock()
	}
	return f.Getattr(ctx, fh, out)
}
