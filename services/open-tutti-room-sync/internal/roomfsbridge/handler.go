// Package roomfsbridge adapts the Room FS Protocol onto the local replica:
// reads come from the replica (lazy CAS fetch), writes convert into Room
// File Operations and submit through the room socket, and mutations report
// success only after the authoritative acknowledgement.
package roomfsbridge

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	vmcas "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-cas"
	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
	roomfs "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-roomfs"
	vmsync "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-sync"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-room-sync/internal/replica"
)

// Submitter sends operations to the server (the WS session).
type Submitter interface {
	Submit(env vmprotocol.Envelope) error
}

// BarrierResolver lifts conflict barriers on the live session once this
// session's fix committed (the WS "conflict_resolved" message).
type BarrierResolver interface {
	ResolveBarrier(path string) error
}

// ChunkUploader uploads blob chunks before a blob-replace op goes out.
type ChunkUploader interface {
	EnsureChunks(ctx context.Context, manifest vmcas.Manifest, chunks [][]byte) error
}

// Handler implements roomfs.Handler over the replica manager.
type Handler struct {
	mu        sync.Mutex
	mgr       *replica.Manager
	submitter Submitter
	resolver  BarrierResolver
	uploader  ChunkUploader
	deviceID  string
	sessionID string
	opCounter int
	// bootID makes operation ids restart-unique: the server
	// deduplicates by (AuthorDeviceID, OperationID), so a restarted
	// room-sync replaying "sess-main-1" could otherwise receive the
	// pre-restart operation's acknowledgement and report success
	// without applying the requested mutation.
	bootID string
	inval  func(path string)
	// resolverDuty marks paths whose conflict barrier assigned THIS
	// session to resolve; the next accepted operation on the path lifts
	// the fence (without an explicit resolve, the first conflict fences
	// the path for every other participant forever).
	resolverDuty map[string]bool
}

// randomBootID returns a per-process random suffix for operation ids.
func randomBootID() string {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Crypto failure degrades to a time-based component: still
		// unique across restarts in practice.
		return fmt.Sprintf("t%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

// New wires the bridge. inval (optional) is invoked with each changed path
// so the host broadcasts invalidations to connected mounts.
func New(mgr *replica.Manager, submitter Submitter, uploader ChunkUploader, deviceID, sessionID string, inval func(path string)) *Handler {
	return &Handler{
		mgr: mgr, submitter: submitter, uploader: uploader,
		deviceID: deviceID, sessionID: sessionID, bootID: randomBootID(), inval: inval,
		resolverDuty: map[string]bool{},
	}
}

// SetResolver attaches the barrier lifter (the live WS session).
func (h *Handler) SetResolver(r BarrierResolver) {
	h.mu.Lock()
	h.resolver = r
	h.mu.Unlock()
}

// OnConflictDetected records resolver duty when the server's conflict
// barrier assigned this session to fix the path.
func (h *Handler) OnConflictDetected(p vmprotocol.ConflictPayload) {
	if p.ResolverAgent == "" || p.ResolverAgent != h.sessionID {
		return
	}
	h.mu.Lock()
	h.resolverDuty[p.Path] = true
	h.mu.Unlock()
}

// InvalidateRemote drops mount caches for a path changed by another
// participant; called after the operation applied locally.
func (h *Handler) InvalidateRemote(path string) {
	if h.inval != nil {
		h.inval(path)
	}
}

func (h *Handler) nextOpID() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.opCounter++
	return fmt.Sprintf("%s-%s-%d", h.sessionID, h.bootID, h.opCounter)
}

// Stat implements roomfs.Handler.
func (h *Handler) Stat(path string) (*roomfs.Stat, error) {
	var stat roomfs.Stat
	h.mgr.WithState(func(state *vmsync.WorkspaceState) {
		info, ok := state.EntryInfo(path)
		if !ok {
			return
		}
		mode := info.Mode
		// Only unset modes take a readable default: an explicit chmod
		// 0000 is real and must round-trip (zero stays zero).
		if !info.ModeSet {
			if info.IsDir {
				mode = 0o755
			} else {
				mode = 0o644
			}
		}
		stat = roomfs.Stat{Dir: info.IsDir, Mode: mode, Exists: true, Size: info.Size, Hash: state.CurrentHash(path)}
	})
	if !stat.Exists {
		return &roomfs.Stat{}, nil
	}
	if !stat.Dir {
		// Cheap for materialized text; lazy blobs read on demand.
		if content, err := h.mgr.Read(context.Background(), path); err == nil {
			stat.Size = int64(len(content))
		}
	}
	return &stat, nil
}

// Read implements roomfs.Handler.
func (h *Handler) Read(path string) ([]byte, error) {
	return h.mgr.Read(context.Background(), path)
}

// List implements roomfs.Handler.
func (h *Handler) List(path string) ([]roomfs.DirEntry, error) {
	prefix := ""
	if path != "" {
		prefix = path + "/"
	}
	var out []roomfs.DirEntry
	h.mgr.WithState(func(state *vmsync.WorkspaceState) {
		type childInfo struct{ dir bool }
		children := map[string]*childInfo{}
		mark := func(name string, dir bool) {
			c := children[name]
			if c == nil {
				c = &childInfo{}
				children[name] = c
			}
			if dir {
				c.dir = true
			}
		}
		for _, p := range state.Paths() {
			if !strings.HasPrefix(p, prefix) || len(p) == len(prefix) {
				continue
			}
			rest := p[len(prefix):]
			if idx := strings.IndexByte(rest, '/'); idx >= 0 {
				// A deeper path proves the child is a directory even
				// when the directory entry itself is absent.
				mark(rest[:idx], true)
				continue
			}
			if info, ok := state.EntryInfo(p); ok {
				// Direct entry: empty directories carry their type
				// only here — a zero-value would emit S_IFREG.
				mark(rest, info.IsDir)
			} else {
				mark(rest, false)
			}
		}
		out = make([]roomfs.DirEntry, 0, len(children))
		for name, c := range children {
			out = append(out, roomfs.DirEntry{Name: name, Dir: c.dir})
		}
	})
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Write implements roomfs.Handler: whole-file write → File Operation.
// ReadWithHash returns content and its hash from ONE replica snapshot.
func (h *Handler) ReadWithHash(path string) ([]byte, string, error) {
	var content []byte
	var hash string
	h.mgr.WithState(func(state *vmsync.WorkspaceState) {
		if info, ok := state.EntryInfo(path); ok && !info.IsDir && info.IsText {
			content = info.Content
		}
		hash = state.CurrentHash(path)
	})
	if content == nil {
		// Blob or lazily-unmaterialized text: the snapshot hash above
		// was computed over EMPTY bytes — materialize and re-read the
		// hash in a second snapshot so the pair is consistent (the
		// first flush of an untouched lazy-restored file otherwise
		// fails EAGAIN against its own baseline).
		c, err := h.Read(path)
		if err != nil {
			return nil, "", err
		}
		h.mgr.WithState(func(state *vmsync.WorkspaceState) {
			hash = state.CurrentHash(path)
		})
		if hash == "" {
			hash = vmsync.ContentHash(c)
		}
		return c, hash, nil
	}
	return content, hash, nil
}

// WriteGuarded validates the flush baseline and prepares the write in
// ONE critical section, then submits; the returned hash is the
// post-write content hash for the mount's next baseline.
func (h *Handler) WriteGuarded(path string, content []byte, baseHash string) (string, error) {
	old, base, baseSeq, err := h.mgr.PrepareWriteGuarded(context.Background(), path, baseHash)
	if err != nil {
		if errors.Is(err, replica.ErrStaleBase) {
			return "", fmt.Errorf("roomfs: stale base for %s; re-read and retry", path)
		}
		return "", err
	}
	op, convErr := vmsync.ConvertChange(h.nextOpID(), path, base, h.mgr.TrackedAsBlob(path), old, content,
		func(manifest vmcas.Manifest, chunks [][]byte) error {
			if h.uploader == nil {
				return fmt.Errorf("no chunk uploader configured")
			}
			return h.uploader.EnsureChunks(context.Background(), manifest, chunks)
		})
	if convErr != nil {
		return "", convErr
	}
	if err := h.submitAtSeq(op, baseSeq); err != nil {
		return "", err
	}
	// Post-write baseline for the mount's next flush: the manifest for
	// blob replacements, the content hash for text.
	if op.Blob != nil {
		return op.Blob.Manifest, nil
	}
	return vmsync.ContentHash(content), nil
}

func (h *Handler) Write(path string, content []byte) error {
	// Old content, tracked base hash, and base sequence come from ONE
	// locked snapshot: a remote operation landing between separate reads
	// would splice stale offsets onto a fresh revision's hash and seq.
	old, base, baseSeq, _ := h.mgr.PrepareWrite(context.Background(), path)
	op, err := vmsync.ConvertChange(h.nextOpID(), path, base, h.mgr.TrackedAsBlob(path), old, content,
		func(manifest vmcas.Manifest, chunks [][]byte) error {
			if h.uploader == nil {
				return fmt.Errorf("no chunk uploader configured")
			}
			return h.uploader.EnsureChunks(context.Background(), manifest, chunks)
		})
	if err != nil {
		return err
	}
	return h.submitAtSeq(op, baseSeq)
}

// Create implements roomfs.Handler.
func (h *Handler) Create(path string, mode uint32) error {
	return h.submit(vmprotocol.FileOperation{
		ID: h.nextOpID(), Path: path, Kind: vmprotocol.OpCreate,
		Mode: &vmprotocol.MetadataChange{Mode: mode},
	})
}

// Mkdir implements roomfs.Handler.
func (h *Handler) Mkdir(path string, mode uint32) error {
	return h.submit(vmprotocol.FileOperation{
		ID: h.nextOpID(), Path: path, Kind: vmprotocol.OpMkdir, IsDir: true,
		Mode: &vmprotocol.MetadataChange{Mode: mode},
	})
}

// Remove implements roomfs.Handler. Directory intent comes from replica
// state so the authoritative remove matches the target's kind.
func (h *Handler) Remove(path string) error {
	op := vmprotocol.FileOperation{
		ID: h.nextOpID(), Path: path, Kind: vmprotocol.OpRemove,
	}
	return h.submitObserved(op, func(state *vmsync.WorkspaceState) {
		if info, ok := state.EntryInfo(path); ok && info.IsDir {
			op.IsDir = true
		}
	})
}

// Rename implements roomfs.Handler.
func (h *Handler) Rename(from, to string, noReplace bool) error {
	op := vmprotocol.FileOperation{
		ID: h.nextOpID(), Path: from, Kind: vmprotocol.OpRename,
		Rename: &vmprotocol.Rename{OldPath: from, NewPath: to, NoReplace: noReplace},
	}
	return h.submitObserved(op, func(state *vmsync.WorkspaceState) {
		if info, ok := state.EntryInfo(from); ok && info.IsDir {
			op.IsDir = true
		}
	})
}

// submitObserved binds the operation's base sequence to its state
// observation: the callback and the AppliedSeq read run in ONE manager
// critical section (the server-op apply path holds the same mutex), so
// a remote remove+recreate landing after the observation always
// sequences ABOVE the stamped base — the server-side generation fence
// then rejects the stale removal/rename instead of deleting the
// replacement generation it never saw.
func (h *Handler) submitObserved(op vmprotocol.FileOperation, observe func(state *vmsync.WorkspaceState)) error {
	var baseSeq uint64
	h.mgr.WithState(func(state *vmsync.WorkspaceState) {
		observe(state)
		baseSeq = h.mgr.Replica.AppliedSeq
	})
	return h.submitAtSeq(op, baseSeq)
}

// Chmod submits a permission-bit change to the authoritative workspace:
// a local-only assignment would revert on the next invalidation and
// never reach other participants.
func (h *Handler) Chmod(path string, mode uint32) error {
	return h.submit(vmprotocol.FileOperation{
		ID: h.nextOpID(), Path: path, Kind: vmprotocol.OpMetadataChange,
		Mode: &vmprotocol.MetadataChange{Mode: mode & 0o7777},
	})
}

// submit sends one operation and reports success only after the server
// accepted it (broadcast acknowledgement); rejections surface as errors.
func (h *Handler) submit(op vmprotocol.FileOperation) error {
	return h.submitAtSeq(op, h.mgr.Replica.AppliedSeq)
}

// submitAtSeq is submit with an explicit base sequence (write path: the
// sequence captured atomically with the content the splice offsets were
// derived from).
func (h *Handler) submitAtSeq(op vmprotocol.FileOperation, baseSeq uint64) error {
	env := vmprotocol.Envelope{
		OperationID: op.ID, Operation: op,
		AuthorDeviceID: h.deviceID, AgentSessionID: h.sessionID,
		BaseSeq: baseSeq,
	}
	err := h.mgr.SubmitAndWait(context.Background(), env, func() error {
		return h.submitter.Submit(env)
	})
	if err != nil {
		return err
	}
	if h.inval != nil {
		h.inval(op.Path)
	}
	// The accepted operation may be this session's conflict fix: lift
	// the barrier so every other participant can edit the path again.
	// A rename also MOVES descendant barriers server-side (the fence
	// follows the file), so tracked duty paths under the renamed tree
	// rekey with it — otherwise the resolver could never lift a fence
	// that no longer sits at the recorded path.
	h.mu.Lock()
	r := h.resolver
	duty := h.resolverDuty[op.Path]
	var movedDuties []string
	if rn := op.Rename; op.Kind == vmprotocol.OpRename && rn != nil {
		for p := range h.resolverDuty {
			if p == rn.OldPath || strings.HasPrefix(p, rn.OldPath+"/") {
				newPath := rn.NewPath + p[len(rn.OldPath):]
				if p == op.Path {
					delete(h.resolverDuty, p)
				}
				h.resolverDuty[newPath] = true
				movedDuties = append(movedDuties, newPath)
			}
		}
	} else {
	}
	h.mu.Unlock()
	// Duty clears ONLY on the server's conflict_resolved broadcast
	// (see OnConflictResolved): a fire-and-forget ResolveBarrier frame
	// can be written to a socket that drops before the server processes
	// it — deleting the duty here would leave the server barrier locked
	// with nothing left to retry, fencing every other participant from
	// the path permanently.
	if duty && r != nil {
		if err := r.ResolveBarrier(op.Path); err != nil {
			h.mu.Lock()
			h.resolverDuty[op.Path] = true
			h.mu.Unlock()
			return fmt.Errorf("lift conflict barrier on %s: %w", op.Path, err)
		}
	}
	for _, p := range movedDuties {
		if err := r.ResolveBarrier(p); err != nil {
			h.mu.Lock()
			h.resolverDuty[p] = true
			h.mu.Unlock()
			return fmt.Errorf("lift moved conflict barrier on %s: %w", p, err)
		}
	}
	return nil
}

// RekeyDuty moves a resolver duty across a REMOTE rename (another
// participant renamed a directory containing the fenced path): the
// authoritative barrier moved to the new prefix, so a fix at the new
// path must send its resolve there — and reconnect retries must not
// keep targeting the obsolete old path.
func (h *Handler) RekeyDuty(oldPath, newPath string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.resolverDuty[oldPath] {
		return
	}
	delete(h.resolverDuty, oldPath)
	h.resolverDuty[newPath] = true
}

// OnConflictResolved drops the resolver duty once the SERVER confirmed
// the lift (conflict_resolved broadcast): that acknowledgement is the
// only authoritative evidence the barrier is down.
func (h *Handler) OnConflictResolved(path string) {
	h.mu.Lock()
	delete(h.resolverDuty, path)
	h.mu.Unlock()
}

// RetryDuties re-attempts barrier resolutions whose confirmation never
// reached the server (socket dropped mid-send): the authoritative fence
// is still locked and only the assigned resolver can lift it.
func (h *Handler) RetryDuties() {
	h.mu.Lock()
	r := h.resolver
	paths := make([]string, 0, len(h.resolverDuty))
	for p := range h.resolverDuty {
		paths = append(paths, p)
	}
	h.mu.Unlock()
	if r == nil {
		return
	}
	for _, p := range paths {
		// The duty SURVIVES a successful send: this frame is as
		// fire-and-forget as the original, and a socket dropping after
		// the write but before server processing must leave a record
		// for the NEXT reconnect. Only the server's conflict_resolved
		// acknowledgement (OnConflictResolved) retires it.
		_ = r.ResolveBarrier(p)
	}
}
