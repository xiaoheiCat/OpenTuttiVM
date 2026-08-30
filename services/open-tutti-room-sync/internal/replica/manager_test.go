package replica

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	vmcas "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-cas"
	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
	vmsync "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-sync"
)

func TestSubmitAndWaitFailsClosedAtPendingBudget(t *testing.T) {
	store := vmcas.NewMemoryStore()
	mgr := NewWithOptions("dev", store, Lazy, nil, Options{MaxPendingOperations: 1, MaxPendingBytes: 1 << 20})
	first := vmprotocol.Envelope{OperationID: "first", Operation: vmprotocol.FileOperation{ID: "first", Path: "a", Kind: vmprotocol.OpCreate}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := mgr.SubmitAndWait(ctx, first, func() error { return nil }); err == nil {
		t.Fatal("first operation unexpectedly acknowledged")
	}
	second := first
	second.OperationID, second.Operation.ID = "second", "second"
	if err := mgr.SubmitAndWait(context.Background(), second, func() error { return nil }); !errors.Is(err, ErrPendingLimit) {
		t.Fatalf("error = %v", err)
	}
	if got := len(mgr.PendingEnvelopes()); got != 1 {
		t.Fatalf("pending = %d", got)
	}
}

func TestSubmitAndWaitFailsClosedAtPendingByteBudget(t *testing.T) {
	store := vmcas.NewMemoryStore()
	env := vmprotocol.Envelope{OperationID: "large", Operation: vmprotocol.FileOperation{ID: "large", Path: "a", Kind: vmprotocol.OpCreate}}
	mgr := NewWithOptions("dev", store, Lazy, nil, Options{MaxPendingOperations: 10, MaxPendingBytes: 1})
	if err := mgr.SubmitAndWait(context.Background(), env, func() error { return nil }); !errors.Is(err, ErrPendingLimit) {
		t.Fatalf("error = %v", err)
	}
	if got := len(mgr.PendingEnvelopes()); got != 0 {
		t.Fatalf("pending = %d", got)
	}
}

// buildSnapshot builds a snapshot of a tiny workspace through the real
// sequencer so tree entries and CAS objects are authentic.
func buildSnapshot(t *testing.T, store vmcas.Store) vmprotocol.WorkspaceSnapshot {
	t.Helper()
	w := vmsync.NewWorkspaceState()
	ctx := context.Background()
	ops := []vmprotocol.Envelope{
		{Operation: vmprotocol.FileOperation{ID: "1", Path: "src", Kind: vmprotocol.OpMkdir, IsDir: true}},
		{Operation: vmprotocol.FileOperation{ID: "2", Path: "src/app.ts", Kind: vmprotocol.OpCreate}},
		{Operation: vmprotocol.FileOperation{ID: "3", Path: "README.md", Kind: vmprotocol.OpCreate}},
		{Operation: vmprotocol.FileOperation{ID: "4", Path: "legacy.txt", Kind: vmprotocol.OpCreate}},
	}
	for _, op := range ops {
		if _, err := w.Accept(op); err != nil {
			t.Fatal(err)
		}
	}
	_ = ctx
	patch := &vmprotocol.TextPatch{BaseHash: vmsync.ContentHash(nil), Splices: []vmprotocol.Splice{
		{Offset: 0, Insert: "export const ready = true\n"},
	}}
	if _, err := w.Accept(vmprotocol.Envelope{Operation: vmprotocol.FileOperation{
		ID: "5", Path: "src/app.ts", Kind: vmprotocol.OpTextPatch, Patch: patch,
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Accept(vmprotocol.Envelope{Operation: vmprotocol.FileOperation{
		ID: "6", Path: "legacy.txt", Kind: vmprotocol.OpRemove,
	}}); err != nil {
		t.Fatal(err)
	}
	snap, err := w.Snapshot("room", vmprotocol.SnapshotBootstrap, store)
	if err != nil {
		t.Fatal(err)
	}
	return snap
}

func TestApplyToWorkspaceMirrorsRoomFinalState(t *testing.T) {
	serverStore := vmcas.NewMemoryStore()
	snap := buildSnapshot(t, serverStore)

	mgr := New("dev_owner", serverStore, Full, nil)
	if err := mgr.Bootstrap(context.Background(), snap, nil); err != nil {
		t.Fatal(err)
	}

	target := t.TempDir()
	// Host workspace has a stale file the room deleted plus a foreign file.
	stale := filepath.Join(target, "legacy.txt")
	os.WriteFile(stale, []byte("old"), 0o644)
	foreign := filepath.Join(target, "untracked.txt")
	os.WriteFile(foreign, []byte("keep-me-not"), 0o644)

	if err := mgr.ApplyToWorkspace(context.Background(), target); err != nil {
		t.Fatal(err)
	}

	// Mirror semantics: the result equals Room Final State strictly.
	appTS, err := os.ReadFile(filepath.Join(target, "src/app.ts"))
	if err != nil || string(appTS) != "export const ready = true\n" {
		t.Fatalf("src/app.ts = %q err=%v", appTS, err)
	}
	if _, err := os.Stat(filepath.Join(target, "README.md")); err != nil {
		t.Fatal("README.md missing after apply")
	}
	for _, gone := range []string{"legacy.txt", "untracked.txt"} {
		if _, err := os.Stat(filepath.Join(target, gone)); !os.IsNotExist(err) {
			t.Fatalf("%s must be removed by the mirror apply", gone)
		}
	}
}

func TestApplyToWorkspacePropagatesStaleDirectoryRemovalFailure(t *testing.T) {
	serverStore := vmcas.NewMemoryStore()
	snap := buildSnapshot(t, serverStore)
	mgr := New("dev_owner", serverStore, Full, nil)
	if err := mgr.Bootstrap(context.Background(), snap, nil); err != nil {
		t.Fatal(err)
	}
	target := t.TempDir()
	staleDir := filepath.Join(target, "stale")
	if err := os.Mkdir(staleDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(staleDir, "child"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	oldRemove := removePath
	removePath = func(path string) error {
		if path == staleDir || path == filepath.Join(target, "stale", "child") {
			return os.ErrPermission
		}
		return oldRemove(path)
	}
	t.Cleanup(func() { removePath = oldRemove })
	// Injected filesystem failure must escape instead of being best effort.
	if err := mgr.ApplyToWorkspace(context.Background(), target); err == nil {
		t.Fatal("apply unexpectedly succeeded with a non-empty stale directory")
	}
}

func TestApplyToWorkspaceRejectsRootSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation requires elevated Windows privilege")
	}
	serverStore := vmcas.NewMemoryStore()
	snap := buildSnapshot(t, serverStore)
	mgr := New("dev_owner", serverStore, Full, nil)
	if err := mgr.Bootstrap(context.Background(), snap, nil); err != nil {
		t.Fatal(err)
	}
	base := t.TempDir()
	root := filepath.Join(base, "root")
	if err := os.Symlink(t.TempDir(), root); err != nil {
		t.Fatal(err)
	}
	if err := mgr.ApplyToWorkspace(context.Background(), root); err == nil {
		t.Fatal("symlink root was accepted")
	}
}

func TestApplyToWorkspaceRejectsSymlinkDescendant(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation requires elevated Windows privilege")
	}
	serverStore := vmcas.NewMemoryStore()
	snap := buildSnapshot(t, serverStore)
	mgr := New("dev_owner", serverStore, Full, nil)
	if err := mgr.Bootstrap(context.Background(), snap, nil); err != nil {
		t.Fatal(err)
	}
	target := t.TempDir()
	if err := os.Symlink(t.TempDir(), filepath.Join(target, "src")); err != nil {
		t.Fatal(err)
	}
	if err := mgr.ApplyToWorkspace(context.Background(), target); err == nil {
		t.Fatal("symlink descendant was accepted")
	}
}

func TestLazyPolicyDefersContentUntilRead(t *testing.T) {
	serverStore := vmcas.NewMemoryStore()
	snap := buildSnapshot(t, serverStore)

	// A lazy replica with an empty cache cannot read blobs yet.
	lazyCache := vmcas.NewMemoryStore()
	mgr := New("dev_participant", lazyCache, Lazy, nil)
	if err := mgr.Bootstrap(context.Background(), snap, nil); err != nil {
		t.Fatal(err)
	}
	// Snapshot bootstrap with an empty cache: tree loads, content missing.
	if mgr.Replica.IsFull() {
		t.Fatal("lazy replica materialized eagerly")
	}
}

func TestFullPolicyFetchesThroughFetcher(t *testing.T) {
	serverStore := vmcas.NewMemoryStore()
	snap := buildSnapshot(t, serverStore)

	fetcher := &copyFetcher{source: serverStore, target: vmcas.NewMemoryStore()}
	mgr := New("dev_owner", fetcher.target, Full, fetcher)
	if err := mgr.Bootstrap(context.Background(), snap, nil); err != nil {
		t.Fatal(err)
	}
	if !mgr.Replica.IsFull() {
		t.Fatal("full replica incomplete after bootstrap+fetch")
	}
	content, err := mgr.Read(context.Background(), "src/app.ts")
	if err != nil || string(content) != "export const ready = true\n" {
		t.Fatalf("read after fetch = %q err=%v", content, err)
	}
}

// copyFetcher simulates the network hop: chunks live server-side and are
// fetched into the local cache on demand.
type copyFetcher struct {
	source vmcas.Store
	target vmcas.Store
}

func (f *copyFetcher) FetchChunk(ctx context.Context, hash string, cache vmcas.Store) error {
	data, err := f.source.Get(hash)
	if err != nil {
		return err
	}
	return cache.Put(hash, data)
}
