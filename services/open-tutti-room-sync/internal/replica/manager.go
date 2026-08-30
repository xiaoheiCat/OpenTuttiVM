// Package replica coordinates the local room replica: bootstrap from a
// server snapshot, CAS-backed lazy reads, full-replica policy for room
// owners and transfer candidates, and the mirror-style Apply to Workspace.
package replica

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	vmcas "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-cas"
	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
	vmsync "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-sync"
)

// Policy selects replica depth.
type Policy string

const (
	// Lazy keeps tree metadata for every path and fetches content on
	// demand — the participant default.
	Lazy Policy = "lazy"
	// Full materializes every path — required for room owners (server
	// failure survival and Apply to Workspace) and transfer candidates.
	Full Policy = "full"
)

// ChunkFetcher downloads a missing chunk into the cache.
type ChunkFetcher interface {
	FetchChunk(ctx context.Context, hash string, cache vmcas.Store) error
}

// Manager owns one room's replica and CAS cache.
type Manager struct {
	Replica *vmsync.Replica
	Cache   vmcas.Store
	Policy  Policy
	fetcher ChunkFetcher
	// mu serializes replica mutation: server events, Room FS handlers,
	// and background materialization all touch the same state maps.
	mu sync.Mutex
	// waiters correlates submitted operation ids with writers blocked on
	// the authoritative acknowledgement.
	waiters map[string]chan error
	// pendingEnvs keeps the envelopes of operations awaiting
	// acknowledgement so a reconnect can re-submit them: an operation
	// committed just before a disconnect is folded into the bootstrap
	// snapshot (no envelope in the replay window) and only a
	// deduplicated re-submit — same operation id — gets the original
	// acknowledgement broadcast back, unblocking the replica's pending
	// set (and delivering the op when it never reached the server).
	pendingEnvs map[string]vmprotocol.Envelope
}

// New wires a replica manager.
func New(deviceID string, cache vmcas.Store, policy Policy, fetcher ChunkFetcher) *Manager {
	m := &Manager{
		Replica:     vmsync.NewReplica(deviceID),
		Cache:       cache,
		Policy:      policy,
		fetcher:     fetcher,
		waiters:     map[string]chan error{},
		pendingEnvs: map[string]vmprotocol.Envelope{},
	}
	// Sequenced applies materialize CAS-referenced content through this
	// hook: restored text before its first patch, and (per Full policy)
	// accepted blob replacements immediately. The hook runs from inside
	// locked manager paths, so it must not re-acquire mu.
	m.Replica.State.Materializer = m.materializeLocked
	// Only full replicas keep the eager blob-copy promise; lazy policy
	// stays lazy on accepted replacements too (the text-base path in
	// materializeLocked is NOT gated — authoritative patches always
	// need their base).
	m.Replica.State.EagerBlobs = m.Policy == Full
	return m
}

// ackWaitTimeout bounds how long a Room FS mutation blocks on the
// authoritative acknowledgement before failing the caller.
const ackWaitTimeout = 10 * time.Second

// ErrNotSent marks definite pre-send failures: the transport wrote
// NOTHING (no live session), so the operation has no unknown fate on
// the wire and must not be replayed by the reconnect path.
var ErrNotSent = errors.New("operation was not sent")

// SubmitAndWait submits one operation and blocks until the server accepts
// (broadcast ack) or rejects it. The submit function performs the actual
// transport write; separating it keeps the manager transport-agnostic.
func (m *Manager) SubmitAndWait(ctx context.Context, env vmprotocol.Envelope, submit func() error) error {
	m.mu.Lock()
	ch := make(chan error, 1)
	m.waiters[env.OperationID] = ch
	m.Replica.Submit(env.OperationID)
	m.pendingEnvs[env.OperationID] = env
	m.mu.Unlock()
	drop := func(keepPending bool) {
		m.mu.Lock()
		delete(m.waiters, env.OperationID)
		if !keepPending {
			delete(m.pendingEnvs, env.OperationID)
			// Definite failure: the pending-ack ID dies with it (no
			// acknowledgement is ever coming for a never-sent or
			// rejected operation).
			m.Replica.Reject(env.OperationID)
		}
		// keepPending: a timeout or ambiguous write means unknown fate,
		// not rejection — the reconnect path may still reconcile it.
		m.mu.Unlock()
	}
	if err := submit(); err != nil {
		if errors.Is(err, ErrNotSent) {
			// Definite PRE-SEND failure: nothing was written, no ack
			// will ever arrive for this id, and replaying it from
			// pendingEnvs would double-submit — drop all its pending
			// state (including the replica's pending-ack entry).
			drop(false)
		} else {
			// AMBIGUOUS transport failure: the envelope may have
			// reached the server (resubmitPending reconciles with the
			// server-side dedup), so pending state stays.
			drop(true)
		}
		return err
	}
	timer := time.NewTimer(ackWaitTimeout)
	defer timer.Stop()
	select {
	case err := <-ch:
		return err
	case <-timer.C:
		drop(true)
		return fmt.Errorf("operation %s not acknowledged within %s", env.OperationID, ackWaitTimeout)
	case <-ctx.Done():
		drop(true)
		return ctx.Err()
	}
}

// NotifyRejected unblocks a waiter with the server's rejection.
func (m *Manager) NotifyRejected(operationID, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ch, ok := m.waiters[operationID]; ok {
		ch <- fmt.Errorf("operation %s rejected: %s", operationID, reason)
		delete(m.waiters, operationID)
	}
	delete(m.pendingEnvs, operationID)
	// The operation's fate is known (rejected): clear the pending-ack
	// ID too, or it lingered until process exit — an ack never comes.
	m.Replica.Reject(operationID)
}

// PendingEnvelopes snapshots the operations still awaiting acknowledgement
// (for post-reconnect re-submission) and clears their pending entries so
// each is tracked freshly by the next SubmitAndWait/ack cycle.
func (m *Manager) PendingEnvelopes() []vmprotocol.Envelope {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]vmprotocol.Envelope, 0, len(m.pendingEnvs))
	for _, env := range m.pendingEnvs {
		out = append(out, env)
	}
	return out
}

// ForgetPending drops one operation's pending bookkeeping after its
// re-submission was acknowledged or rejected.
func (m *Manager) ForgetPending(operationID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.pendingEnvs, operationID)
}

// AppliedSeq reads the replica's applied sequence UNDER the manager
// lock: ApplyServerOp writes it here, and external readers (the
// apply-and-leave capture, roomfs submissions) on their own goroutines
// raced the WS apply loop otherwise.
func (m *Manager) AppliedSeq() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.Replica.AppliedSeq
}

// WithState runs fn against the replica state under the manager lock —
// the sanctioned read path for Room FS metadata queries.
func (m *Manager) WithState(fn func(state *vmsync.WorkspaceState)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	fn(m.Replica.State)
}

func (m *Manager) materializeLocked(path string) error {
	if _, err := m.Replica.MaterializePath(path, m.Cache); err == nil {
		return nil
	} else if m.fetcher == nil {
		return err
	}
	// Content referenced but not cached. Lazy policy defers bulk
	// bootstrap materialization and ordinary reads already fetch on
	// demand, but an authoritative text patch still needs its exact
	// base: a lazy replica fetches this ONE file rather than failing
	// incremental application and forcing a whole-tree bootstrap.
	if _, err := m.readLocked(context.Background(), path); err != nil {
		return err
	}
	return nil
}

// PromoteToFull upgrades a lazy manager to the owner policy (explicit
// transfer or succession made this device the owner): eager blob
// materialization from now on, and every already-known blob fetches so
// the promised server-failure-survival copy exists immediately.
func (m *Manager) PromoteToFull(ctx context.Context) error {
	m.mu.Lock()
	// Tentatively eager for the materialization pass, REVERTED on any
	// failure: the presence handler retries promotion only while the
	// policy is not full, so a fetch failure that left Policy=Full
	// would permanently suppress retries and strand an owner that
	// cannot survive server loss.
	m.Policy = Full
	m.Replica.State.EagerBlobs = true
	// EVERY non-directory path, like full bootstrap: lazy participants
	// can hold snapshot-backed TEXT entries with only a manifest and no
	// local content; leaving them unmaterialized keeps IsFull() false
	// and loses the final workspace on server failure.
	var paths []string
	paths = append(paths, m.Replica.State.TextPaths()...)
	paths = append(paths, m.Replica.State.BlobPaths()...)
	for _, p := range paths {
		if err := m.materializeLocked(p); err != nil {
			m.Policy = Lazy
			m.Replica.State.EagerBlobs = false
			m.mu.Unlock()
			return err
		}
	}
	m.mu.Unlock()
	return nil
}

// Bootstrap loads a snapshot plus its replay window and materializes
// content per policy.
func (m *Manager) Bootstrap(ctx context.Context, snap vmprotocol.WorkspaceSnapshot, ops []vmprotocol.Envelope) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Replica.Bootstrap(snap, ops); err != nil {
		return err
	}
	if m.Policy == Full {
		return m.materializeMissingLocked(ctx)
	}
	return nil
}

// ApplyServerOp applies one sequenced envelope under the manager lock —
// the single entry point for server events, so Room FS handlers cannot
// race the state maps. Acks and sequenced rejections unblock waiters.
func (m *Manager) ApplyServerOp(env vmprotocol.Envelope) (bool, error) {
	m.mu.Lock()
	acked, err := m.Replica.ApplyServerOp(env)
	if acked {
		if ch, ok := m.waiters[env.OperationID]; ok {
			ch <- nil
			delete(m.waiters, env.OperationID)
		}
		delete(m.pendingEnvs, env.OperationID)
	}
	m.mu.Unlock()
	return acked, err
}

// Submit records a pending local operation id under the manager lock.
func (m *Manager) Submit(operationID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Replica.Submit(operationID)
}

// MaterializeMissing fetches every un-cached path.
func (m *Manager) MaterializeMissing(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.materializeMissingLocked(ctx)
}

func (m *Manager) materializeMissingLocked(ctx context.Context) error {
	for _, path := range m.Replica.State.Paths() {
		if _, err := m.readLocked(ctx, path); err != nil {
			return err
		}
	}
	return nil
}

// TrackedAsBlob reports whether the path is tracked as a blob entry: a
// blob-tracked file keeps issuing blob replacements even when its new
// content is small valid UTF-8 (the authoritative engine rejects text
// patches against non-text entries).
func (m *Manager) TrackedAsBlob(path string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	info, ok := m.Replica.State.EntryInfo(path)
	return ok && !info.IsDir && !info.IsText
}

// Read returns local content for one path, fetching lazily when needed.
// Text and blob entries share the path: both restore from snapshots with a
// manifest reference, so both fetch manifest + chunks from the server CAS
// on first read.
func (m *Manager) Read(ctx context.Context, path string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.readLocked(ctx, path)
}

// PrepareWrite snapshots everything a whole-file write derives from — the
// old content, the base hash the authoritative state tracks, and the
// applied sequence — under ONE lock. Sampling them separately would let a
// remote operation land in between: splice offsets from revision N would
// claim revision N+1's hash and sequence, and the server would apply the
// stale offsets without transformation.
func (m *Manager) PrepareWrite(ctx context.Context, path string) (content []byte, baseHash string, baseSeq uint64, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	content, err = m.readLocked(ctx, path)
	if err != nil {
		// Unknown path = new file: empty base is safe (the server's
		// create guard rejects if a remote create raced us).
		return nil, "", m.Replica.AppliedSeq, err
	}
	return content, m.Replica.State.CurrentBaseHash(path), m.Replica.AppliedSeq, nil
}

// ErrStaleBase reports a flush whose buffer baseline no longer matches
// the current content hash (another participant's edit superseded it).
var ErrStaleBase = errors.New("stale base: content changed since buffer was loaded")

// PrepareWriteGuarded snapshots the write base ATOMICALLY with an
// optional baseline check: without the guard inside the same critical
// section, an edit landing between the server's pre-check and this
// prepare would be diffed against the newer revision and accepted as
// current, silently overwriting it.
func (m *Manager) PrepareWriteGuarded(ctx context.Context, path, baseHash string) ([]byte, string, uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if baseHash != "" {
		if cur := m.Replica.State.CurrentBaseHash(path); cur != baseHash {
			return nil, "", 0, ErrStaleBase
		}
	}
	content, err := m.readLocked(ctx, path)
	if err != nil {
		return nil, "", m.Replica.AppliedSeq, err
	}
	return content, m.Replica.State.CurrentBaseHash(path), m.Replica.AppliedSeq, nil
}

func (m *Manager) readLocked(ctx context.Context, path string) ([]byte, error) {
	if content, ok := m.Replica.State.CurrentContent(path); ok {
		return content, nil
	}
	if content, err := m.Replica.MaterializePath(path, m.Cache); err == nil {
		return content, nil
	}
	if m.fetcher == nil {
		return nil, vmcas.ErrObjectNotFound
	}
	info, ok := m.Replica.State.EntryInfo(path)
	if !ok || info.IsDir {
		return nil, fmt.Errorf("path %s not in workspace", path)
	}
	if info.Manifest == "" {
		return nil, fmt.Errorf("path %s has no local content or manifest", path)
	}
	if _, err := m.Cache.Get(info.Manifest); err != nil {
		if err := m.fetcher.FetchChunk(ctx, info.Manifest, m.Cache); err != nil {
			return nil, err
		}
	}
	data, err := m.Cache.Get(info.Manifest)
	if err != nil {
		return nil, err
	}
	manifest, err := vmcas.DecodeManifest(data)
	if err != nil {
		return nil, err
	}
	for _, chunkHash := range manifest.Chunks {
		if _, err := m.Cache.Get(chunkHash); err == nil {
			continue
		}
		if err := m.fetcher.FetchChunk(ctx, chunkHash, m.Cache); err != nil {
			return nil, err
		}
	}
	return m.Replica.MaterializePath(path, m.Cache)
}

// ApplyToWorkspace mirrors the replica's final state onto a host directory:
// files absent from the room are deleted from the target so the result
// strictly equals Room Final State (the locked v1 semantics: no three-way
// merge, no host-side change protection).
func (m *Manager) ApplyToWorkspace(ctx context.Context, targetDir string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	rootInfo, err := prepareApplyRoot(targetDir)
	if err != nil {
		return err
	}
	checkRoot := func() error { return verifyApplyRoot(targetDir, rootInfo) }
	if err := m.materializeMissingLocked(ctx); err != nil {
		return err
	}
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return err
	}
	roomPaths := map[string]bool{}
	// Restrictive directory modes (0555, 0500…) apply AFTER every
	// descendant exists: sorted paths visit the directory first, and an
	// immediate chmod would strip the owner's write permission before
	// the children's CreateTemp lands beneath it.
	var deferredDirs []struct {
		dst     string
		mode    uint32
		modeSet bool
	}
	for _, path := range m.Replica.State.Paths() {
		if err := checkRoot(); err != nil {
			return err
		}
		roomPaths[path] = true
		info, ok := m.Replica.State.EntryInfo(path)
		if !ok {
			continue
		}
		dst := filepath.Join(targetDir, filepath.FromSlash(path))
		// Room history can flip a path's type (file→dir or dir→file).
		// The host still holds the old type, and MkdirAll fails on an
		// existing file while a rename cannot land on a directory —
		// clear only the conflicting entry (RemoveAll for a superseded
		// subtree) before creating the authoritative one.
		if fi, err := os.Lstat(dst); err == nil {
			if info.IsDir && !fi.IsDir() {
				if err := os.Remove(dst); err != nil {
					return fmt.Errorf("clear file replaced by dir %s: %w", dst, err)
				}
			} else if !info.IsDir && fi.IsDir() {
				if err := os.RemoveAll(dst); err != nil {
					return fmt.Errorf("clear dir replaced by file %s: %w", dst, err)
				}
			} else if !info.IsDir && !fi.IsDir() && fi.Mode()&fs.ModeSymlink != 0 {
				// A host symlink at a room-file path is a conflicting
				// entry too: the mirror must not write through it.
				if err := os.Remove(dst); err != nil {
					return fmt.Errorf("clear symlink replaced by file %s: %w", dst, err)
				}
			}
		}
		if info.IsDir {
			if err := ensureUnder(targetDir, dst); err != nil {
				return err
			}
			if fi, err := os.Lstat(dst); err == nil && fi.IsDir() && fi.Mode().Perm()&0o200 == 0 {
				// Temporarily WIDEN an existing restrictive
				// directory (0555/0500…): MkdirAll is a no-op on
				// existing dirs, so creating children (or an
				// apply-and-leave RETRY after an earlier attempt
				// applied the restrictive mode before a
				// leave-fence rejection) would fail EACCES for a
				// non-root process. The deferred pass restores
				// the authoritative mode after every descendant
				// exists.
				if err := os.Chmod(dst, fi.Mode().Perm()|0o700); err != nil {
					return fmt.Errorf("widen %s: %w", dst, err)
				}
				if !info.ModeSet {
					// No authoritative mode for this dir: restore
					// the pre-widen host mode, not a fabricated one.
					deferredDirs = append(deferredDirs, struct {
						dst     string
						mode    uint32
						modeSet bool
					}{dst, uint32(fi.Mode().Perm()), true})
				}
			}
			if err := os.MkdirAll(dst, 0o755); err != nil {
				return err
			}
			// ModeSet, not Mode != 0: an explicitly synchronized 0000
			// must still chmod down from MkdirAll's 0755; only an
			// absent mode skips the deferred chmod.
			if info.ModeSet {
				deferredDirs = append(deferredDirs, struct {
					dst     string
					mode    uint32
					modeSet bool
				}{dst, info.Mode, true})
			}
			continue
		}
		content, err := m.readLocked(ctx, path)
		if err != nil {
			return fmt.Errorf("read %s: %w", path, err)
		}
		if err := ensureUnder(targetDir, filepath.Dir(dst)); err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		if err := atomicWrite(dst, content); err != nil {
			return err
		}
		// Executability and other synchronized permission bits survive
		// the mirror; CreateTemp's 0600 must not be the final mode.
		// ModeSet (not the numeric value): an explicit 0000 must chmod
		// DOWN from 0600, not skip.
		if info.ModeSet {
			if err := applyMode(dst, info.Mode); err != nil {
				return fmt.Errorf("chmod %s: %w", dst, err)
			}
		}
	}
	// Mirror: remove host files the room no longer has. This runs
	// BEFORE the deferred directory chmods below: a restrictive 0555
	// parent would strip the write permission needed to delete its
	// room-absent children, leaving Apply-to-Workspace permanently
	// unable to complete (and Apply-and-Leave stuck).
	if err := pruneRemoved(targetDir, roomPaths); err != nil {
		return err
	}
	if err := checkRoot(); err != nil {
		return err
	}
	// Bottom-up so children never chmod-block their parent's remaining
	// work — deepest paths first means a restrictive parent runs after
	// everything beneath it is already in place.
	sort.Slice(deferredDirs, func(i, j int) bool {
		return deferredDirs[i].dst > deferredDirs[j].dst
	})
	for _, d := range deferredDirs {
		if err := checkRoot(); err != nil {
			return err
		}
		if d.modeSet {
			if err := applyMode(d.dst, d.mode); err != nil {
				return fmt.Errorf("chmod %s: %w", d.dst, err)
			}
		}
	}
	return nil
}

// ensureUnder walks every path component from root to dir and refuses
// when one is a symlink or non-directory: participant-controlled room
// paths must never follow a pre-existing host link out of the selected
// workspace during Apply-to-Workspace.
func ensureUnder(root, dir string) error {
	rel, err := filepath.Rel(root, dir)
	if err != nil {
		return err
	}
	if rel == "." {
		return nil
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("path %s escapes workspace root", dir)
	}
	cur := root
	for _, part := range strings.Split(filepath.ToSlash(rel), "/") {
		cur = filepath.Join(cur, part)
		fi, err := os.Lstat(cur)
		if errors.Is(err, fs.ErrNotExist) {
			// Nothing exists below the first missing component, so
			// creating the chain cannot traverse a link.
			return os.MkdirAll(cur, 0o755)
		}
		if err != nil {
			return err
		}
		if fi.Mode()&fs.ModeSymlink != 0 {
			return fmt.Errorf("symlink ancestor %s would escape the workspace", cur)
		}
		if !fi.IsDir() {
			return fmt.Errorf("ancestor %s of the workspace path is not a directory", cur)
		}
	}
	return nil
}

// applyMode chmods a mirrored path when the room recorded a mode.
// Failures PROPAGATE: a silently swallowed chmod would let
// Apply-to-Workspace report success (and a following owner leave
// dissolve the room) while the mirror carries permissions the room
// never granted — with the authoritative state gone.
// applyMode applies an EXPLICITLY synchronized mode. Both call sites
// guard with ModeSet, so 0000 must chmod down from the temp file's
// 0600 (or the directory's 0755) — the old mode==0 early return kept
// reporting success while dropping the authoritative restriction, and
// a later dissolution made the loss permanent.
func applyMode(dst string, mode uint32) error {
	return os.Chmod(dst, fs.FileMode(mode&0o7777))
}

func atomicWrite(dst string, content []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(dst), ".apply-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(content); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	// replaceFile keeps the swap atomic on BOTH platforms: Windows
	// os.Rename refuses to replace an existing destination, and the old
	// remove-then-rename fallback deleted the ONLY installed copy
	// before attempting the second rename — a crash or second failure
	// left the workspace with NEITHER version. The Windows adapter
	// uses MoveFileEx(REPLACE_EXISTING); POSIX rename(2) already
	// replaces atomically.
	return replaceFile(tmp.Name(), dst)
}

var removePath = os.Remove

func pruneRemoved(root string, roomPaths map[string]bool) error {
	var dirs []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, rerr := filepath.Rel(root, p)
		if rerr != nil {
			return rerr
		}
		rel = filepath.ToSlash(rel)
		if rel == "." {
			return nil
		}
		if d.IsDir() {
			dirs = append(dirs, p)
			return nil
		}
		if !roomPaths[rel] {
			// Windows: a mirrored read-only mode (no 0200 bit) sets
			// FILE_ATTRIBUTE_READONLY and the delete fails ACCESS_DENIED,
			// failing the whole apply. Clear it first (POSIX no-op).
			clearReadOnly(p)
			return removePath(p)
		}
		return nil
	})
	if err != nil {
		return err
	}
	// Remove empty directories bottom-up, deepest first.
	sort.Sort(sort.Reverse(sort.StringSlice(dirs)))
	for _, d := range dirs {
		rel, _ := filepath.Rel(root, d)
		if !roomPaths[filepath.ToSlash(rel)] {
			if err := removePath(d); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return fmt.Errorf("remove stale directory %s: %w", d, err)
			}
		}
	}
	return nil
}
