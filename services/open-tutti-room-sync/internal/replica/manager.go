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
}

// New wires a replica manager.
func New(deviceID string, cache vmcas.Store, policy Policy, fetcher ChunkFetcher) *Manager {
	return &Manager{
		Replica: vmsync.NewReplica(deviceID),
		Cache:   cache,
		Policy:  policy,
		fetcher: fetcher,
	}
}

// Bootstrap loads a snapshot plus its replay window and materializes
// content per policy.
func (m *Manager) Bootstrap(ctx context.Context, snap vmprotocol.WorkspaceSnapshot, ops []vmprotocol.Envelope) error {
	if err := m.Replica.Bootstrap(snap, ops); err != nil {
		return err
	}
	if m.Policy == Full {
		return m.MaterializeMissing(ctx)
	}
	return nil
}

// MaterializeMissing fetches every un-cached blob.
func (m *Manager) MaterializeMissing(ctx context.Context) error {
	for _, path := range m.Replica.State.Paths() {
		if _, err := m.Read(ctx, path); err != nil {
			return err
		}
	}
	return nil
}

// Read returns local content for one path, fetching lazily when needed.
func (m *Manager) Read(ctx context.Context, path string) ([]byte, error) {
	if content, ok := m.Replica.State.CurrentContent(path); ok {
		return content, nil
	}
	if content, err := m.Replica.MaterializePath(path, m.Cache); err == nil {
		return content, nil
	}
	if m.fetcher == nil {
		return nil, vmcas.ErrObjectNotFound
	}
	// Manifest or chunk missing from the cache: fetch from the server.
	manifestHash, ok := vmsync.BlobManifestOf(m.Replica.State, path)
	if !ok {
		return nil, fmt.Errorf("path %s has no local content or manifest", path)
	}
	if err := m.fetcher.FetchChunk(ctx, manifestHash, m.Cache); err != nil {
		return nil, err
	}
	data, err := m.Cache.Get(manifestHash)
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
	if err := m.MaterializeMissing(ctx); err != nil {
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
			continue
		}
		content, err := m.Read(ctx, path)
		if err != nil {
			return fmt.Errorf("read %s: %w", path, err)
		}
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		if err := atomicWrite(dst, content); err != nil {
			return err
		}
	}
	// Mirror: remove host files the room no longer has.
	return pruneRemoved(targetDir, roomPaths)
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
