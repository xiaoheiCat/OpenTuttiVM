// Package replica coordinates the local room replica: bootstrap from a
// server snapshot, CAS-backed lazy reads, full-replica policy for room
// owners and transfer candidates, and the mirror-style Apply to Workspace.
package replica

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
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
}

// New wires a replica manager.
func New(deviceID string, cache vmcas.Store, policy Policy, fetcher ChunkFetcher) *Manager {
	m := &Manager{
		Replica: vmsync.NewReplica(deviceID),
		Cache:   cache,
		Policy:  policy,
		fetcher: fetcher,
		waiters: map[string]chan error{},
	}
	// Sequenced applies materialize CAS-referenced content through this
	// hook: restored text before its first patch, and (per Full policy)
	// accepted blob replacements immediately. The hook runs from inside
	// locked manager paths, so it must not re-acquire mu.
	m.Replica.State.Materializer = m.materializeLocked
	return m
}

// ackWaitTimeout bounds how long a Room FS mutation blocks on the
// authoritative acknowledgement before failing the caller.
const ackWaitTimeout = 10 * time.Second

// SubmitAndWait submits one operation and blocks until the server accepts
// (broadcast ack) or rejects it. The submit function performs the actual
// transport write; separating it keeps the manager transport-agnostic.
func (m *Manager) SubmitAndWait(ctx context.Context, env vmprotocol.Envelope, submit func() error) error {
	m.mu.Lock()
	ch := make(chan error, 1)
	m.waiters[env.OperationID] = ch
	m.Replica.Submit(env.OperationID)
	m.mu.Unlock()
	drop := func() {
		m.mu.Lock()
		delete(m.waiters, env.OperationID)
		m.mu.Unlock()
	}
	if err := submit(); err != nil {
		drop()
		return err
	}
	timer := time.NewTimer(ackWaitTimeout)
	defer timer.Stop()
	select {
	case err := <-ch:
		return err
	case <-timer.C:
		drop()
		return fmt.Errorf("operation %s not acknowledged within %s", env.OperationID, ackWaitTimeout)
	case <-ctx.Done():
		drop()
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
	} else if m.fetcher == nil || m.Policy != Full {
		return err
	}
	// Content referenced but not cached: full replicas fetch through the
	// server; lazy replicas surface the miss to the read path.
	if _, err := m.readLocked(context.Background(), path); err != nil {
		return err
	}
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

// Read returns local content for one path, fetching lazily when needed.
// Text and blob entries share the path: both restore from snapshots with a
// manifest reference, so both fetch manifest + chunks from the server CAS
// on first read.
func (m *Manager) Read(ctx context.Context, path string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.readLocked(ctx, path)
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
	if err := m.materializeMissingLocked(ctx); err != nil {
		return err
	}
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return err
	}
	roomPaths := map[string]bool{}
	for _, path := range m.Replica.State.Paths() {
		roomPaths[path] = true
		info, ok := m.Replica.State.EntryInfo(path)
		if !ok {
			continue
		}
		dst := filepath.Join(targetDir, filepath.FromSlash(path))
		if info.IsDir {
			if err := os.MkdirAll(dst, 0o755); err != nil {
				return err
			}
			applyMode(dst, info.Mode)
			continue
		}
		content, err := m.readLocked(ctx, path)
		if err != nil {
			return fmt.Errorf("read %s: %w", path, err)
		}
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		if err := atomicWrite(dst, content); err != nil {
			return err
		}
		// Executability and other synchronized permission bits survive
		// the mirror; CreateTemp's 0600 must not be the final mode.
		applyMode(dst, info.Mode)
	}
	// Mirror: remove host files the room no longer has.
	return pruneRemoved(targetDir, roomPaths)
}

// applyMode chmods a mirrored path when the room recorded a mode.
func applyMode(dst string, mode uint32) {
	if mode == 0 {
		return
	}
	_ = os.Chmod(dst, fs.FileMode(mode&0o7777))
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
	return os.Rename(tmp.Name(), dst)
}

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
			return os.Remove(p)
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
			_ = os.Remove(d) // best effort; non-empty dirs stay
		}
	}
	return nil
}
