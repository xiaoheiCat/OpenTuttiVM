package roomfsbridge

import (
	"context"
	"sync"
	"testing"

	vmcas "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-cas"
	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
	vmsync "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-sync"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-room-sync/internal/replica"
)

type fakeSubmitter struct {
	mu   sync.Mutex
	envs []vmprotocol.Envelope
	mgr  *replica.Manager
}

// Submit records the envelope and simulates the authoritative round trip:
// the broadcast ack applies the same envelope with the next server
// sequence, releasing the bridge's waiter.
func (f *fakeSubmitter) Submit(env vmprotocol.Envelope) error {
	f.mu.Lock()
	f.envs = append(f.envs, env)
	f.mu.Unlock()
	if f.mgr != nil {
		next := env
		next.ServerSeq = f.mgr.Replica.AppliedSeq + 1
		_, err := f.mgr.ApplyServerOp(next)
		return err
	}
	return nil
}

type fakeUploader struct{ stored vmcas.Store }

func (f *fakeUploader) EnsureChunks(ctx context.Context, m vmcas.Manifest, chunks [][]byte) error {
	for i, hash := range m.Chunks {
		if err := f.stored.Put(hash, chunks[i]); err != nil {
			return err
		}
	}
	return f.stored.Put(m.Hash, m.Body())
}

func newBridge(t *testing.T) (*Handler, *fakeSubmitter, *replica.Manager) {
	t.Helper()
	w := vmsync.NewWorkspaceState()
	for _, op := range []vmprotocol.Envelope{
		{Operation: vmprotocol.FileOperation{ID: "1", Path: "src", Kind: vmprotocol.OpMkdir, IsDir: true}},
		{Operation: vmprotocol.FileOperation{ID: "2", Path: "src/app.ts", Kind: vmprotocol.OpCreate}},
	} {
		if _, err := w.Accept(op); err != nil {
			t.Fatal(err)
		}
	}
	patch := &vmprotocol.TextPatch{BaseHash: vmsync.ContentHash(nil), Splices: []vmprotocol.Splice{
		{Offset: 0, Insert: "v1"},
	}}
	if _, err := w.Accept(vmprotocol.Envelope{Operation: vmprotocol.FileOperation{
		ID: "3", Path: "src/app.ts", Kind: vmprotocol.OpTextPatch, Patch: patch,
	}}); err != nil {
		t.Fatal(err)
	}
	store := vmcas.NewMemoryStore()
	snap, err := w.Snapshot("room", vmprotocol.SnapshotBootstrap, store)
	if err != nil {
		t.Fatal(err)
	}
	mgr := replica.New("dev_a", store, replica.Full, nil)
	if err := mgr.Bootstrap(context.Background(), snap, nil); err != nil {
		t.Fatal(err)
	}
	sub := &fakeSubmitter{mgr: mgr}
	h := New(mgr, sub, &fakeUploader{stored: store}, "dev_a", "sess-x", nil)
	return h, sub, mgr
}

func TestBridgeReadAndList(t *testing.T) {
	h, _, _ := newBridge(t)

	st, err := h.Stat("src/app.ts")
	if err != nil || !st.Exists || st.Size != 2 {
		t.Fatalf("stat: %+v err=%v", st, err)
	}
	dir, err := h.Stat("src")
	if err != nil || !dir.Dir {
		t.Fatalf("stat dir: %+v err=%v", dir, err)
	}
	missing, _ := h.Stat("gone.txt")
	if missing.Exists {
		t.Fatal("missing path must not exist")
	}

	content, err := h.Read("src/app.ts")
	if err != nil || string(content) != "v1" {
		t.Fatalf("read: %q err=%v", content, err)
	}

	entries, err := h.List("")
	if err != nil || len(entries) != 1 || entries[0].Name != "src" || !entries[0].Dir {
		t.Fatalf("list: %+v err=%v", entries, err)
	}
}

func TestBridgeWriteSubmitsTextPatch(t *testing.T) {
	h, sub, _ := newBridge(t)

	if err := h.Write("src/app.ts", []byte("v2-edited")); err != nil {
		t.Fatal(err)
	}
	if len(sub.envs) != 1 {
		t.Fatalf("submitted %d ops", len(sub.envs))
	}
	env := sub.envs[0]
	if env.Operation.Kind != vmprotocol.OpTextPatch {
		t.Fatalf("kind %v", env.Operation.Kind)
	}
	if env.AuthorDeviceID != "dev_a" || env.AgentSessionID != "sess-x" {
		t.Fatalf("envelope authorship %+v", env)
	}
	// The patch replays v1 → v2-edited.
	applied, err := vmsync.ApplyPatch([]byte("v1"), *env.Operation.Patch)
	if err != nil || string(applied) != "v2-edited" {
		t.Fatalf("replay: %q err=%v", applied, err)
	}
}

func TestBridgeCreateRemoveRename(t *testing.T) {
	h, sub, _ := newBridge(t)

	if err := h.Create("notes.md", 0o644); err != nil {
		t.Fatal(err)
	}
	if err := h.Mkdir("docs", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := h.Rename("notes.md", "docs/notes.md"); err != nil {
		t.Fatal(err)
	}
	if err := h.Remove("docs/notes.md"); err != nil {
		t.Fatal(err)
	}
	if len(sub.envs) != 4 {
		t.Fatalf("submitted %d ops", len(sub.envs))
	}
	if sub.envs[0].Operation.Kind != vmprotocol.OpCreate ||
		sub.envs[1].Operation.Kind != vmprotocol.OpMkdir || !sub.envs[1].Operation.IsDir ||
		sub.envs[2].Operation.Kind != vmprotocol.OpRename ||
		sub.envs[3].Operation.Kind != vmprotocol.OpRemove {
		for i, e := range sub.envs {
			t.Logf("op %d: %v", i, e.Operation.Kind)
		}
		t.Fatal("unexpected operation kinds")
	}
}

func TestBridgeInvalidationHook(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	h, _, _ := newBridgeWithInval(t, func(p string) { mu.Lock(); paths = append(paths, p); mu.Unlock() })
	if err := h.Create("hooked.txt", 0o644); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(paths) != 1 || paths[0] != "hooked.txt" {
		t.Fatalf("invalidations %+v", paths)
	}
}

func newBridgeWithInval(t *testing.T, inval func(string)) (*Handler, *fakeSubmitter, *replica.Manager) {
	t.Helper()
	w := vmsync.NewWorkspaceState()
	store := vmcas.NewMemoryStore()
	snap, err := w.Snapshot("room", vmprotocol.SnapshotBootstrap, store)
	if err != nil {
		t.Fatal(err)
	}
	mgr := replica.New("dev_a", store, replica.Full, nil)
	if err := mgr.Bootstrap(context.Background(), snap, nil); err != nil {
		t.Fatal(err)
	}
	sub := &fakeSubmitter{mgr: mgr}
	return New(mgr, sub, nil, "dev_a", "sess-x", inval), sub, mgr
}
