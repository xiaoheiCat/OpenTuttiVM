package vmsync

import (
	"bytes"
	"strings"
	"testing"

	vmcas "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-cas"
	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
)

func mustPatch(t *testing.T, old, next []byte) *vmprotocol.TextPatch {
	t.Helper()
	p, ok := DiffText(old, next)
	if !ok {
		t.Fatalf("content not diffable as text")
	}
	return p
}

func submit(t *testing.T, w *WorkspaceState, base uint64, path string, patch *vmprotocol.TextPatch, agent string) (vmprotocol.Envelope, error) {
	t.Helper()
	env := vmprotocol.Envelope{
		RoomID:         "room",
		OperationID:    "op-" + path + "-" + agent + "-" + patch.BaseHash[:14] + itoa(base),
		AuthorDeviceID: "dev-" + agent,
		AgentSessionID: agent,
		BaseSeq:        base,
		Operation: vmprotocol.FileOperation{
			ID: "op", Path: path, Kind: vmprotocol.OpTextPatch, Patch: patch,
		},
	}
	return w.Accept(env)
}

func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	return string(b[i:])
}

const baseDoc = "line1: const port = 3000\nline2: const host = localhost\nline3: export default app\n"

func TestConcurrentEditsDifferentRegionsMerge(t *testing.T) {
	// The Tutti VM scenario: Alice edits the port line, Bob edits line3,
	// both from the same base. Both edits must survive in one sequence.
	w := NewWorkspaceState()
	if _, err := w.Accept(vmprotocol.Envelope{
		AuthorDeviceID: "dev-alice", AgentSessionID: "alice-claude", BaseSeq: 0,
		Operation: vmprotocol.FileOperation{ID: "c1", Path: "src/app.ts", Kind: vmprotocol.OpCreate},
	}); err != nil {
		t.Fatal(err)
	}
	aliceWant := strings.Replace(baseDoc, "3000", "8080", 1)
	bobWant := strings.Replace(baseDoc, "export default app", "export default server", 1)

	// Seed the base document first so both authors share one base revision.
	if _, err := submit(t, w, 1, "src/app.ts", mustPatch(t, []byte{}, []byte(baseDoc)), "alice-claude"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Alice is up to date and applies directly.
	if _, err := submit(t, w, 2, "src/app.ts", mustPatch(t, []byte(baseDoc), []byte(aliceWant)), "alice-claude"); err != nil {
		t.Fatalf("alice submit: %v", err)
	}
	// Bob submits against the same stale base; the server transforms.
	if _, err := submit(t, w, 2, "src/app.ts", mustPatch(t, []byte(baseDoc), []byte(bobWant)), "bob-codex"); err != nil {
		t.Fatalf("bob submit: %v", err)
	}

	merged := string(w.files["src/app.ts"].Content)
	want := strings.Replace(aliceWant, "export default app", "export default server", 1)
	if merged != want {
		t.Fatalf("merged:\n%q\nwant:\n%q", merged, want)
	}
}

func TestInsertAtSamePointOrdersHistoryFirst(t *testing.T) {
	w := NewWorkspaceState()
	base := []byte("abcdef")
	if _, err := w.Accept(vmprotocol.Envelope{
		AuthorDeviceID: "d1", AgentSessionID: "a1", BaseSeq: 0,
		Operation: vmprotocol.FileOperation{ID: "c", Path: "f", Kind: vmprotocol.OpCreate},
	}); err != nil {
		t.Fatal(err)
	}
	// Alice inserts "X" at offset 3; Bob (stale base) inserts "Y" at the
	// same offset. History-first order: applied op wins the left position.
	if _, err := submit(t, w, 1, "f", mustPatch(t, []byte{}, base), "a1"); err != nil {
		t.Fatal(err)
	}
	if _, err := submit(t, w, 2, "f", &vmprotocol.TextPatch{BaseHash: ContentHash(base), Splices: []vmprotocol.Splice{{Offset: 3, Insert: "X"}}}, "a1"); err != nil {
		t.Fatal(err)
	}
	if _, err := submit(t, w, 2, "f", &vmprotocol.TextPatch{BaseHash: ContentHash(base), Splices: []vmprotocol.Splice{{Offset: 3, Insert: "Y"}}}, "a2"); err != nil {
		t.Fatal(err)
	}
	if got := string(w.files["f"].Content); got != "abcXYdef" {
		t.Fatalf("same-point inserts = %q want %q (history-first)", got, "abcXYdef")
	}
}

func TestOverlappingEditsOpenBarrierAndFence(t *testing.T) {
	w := NewWorkspaceState()
	if _, err := w.Accept(vmprotocol.Envelope{
		AuthorDeviceID: "d1", AgentSessionID: "alice-claude", BaseSeq: 0,
		Operation: vmprotocol.FileOperation{ID: "c", Path: "auth.ts", Kind: vmprotocol.OpCreate},
	}); err != nil {
		t.Fatal(err)
	}
	aliceWant := strings.Replace(baseDoc, "const port = 3000", "const port = process.env.PORT", 1)
	// Seed the base document so both authors share one base revision.
	if _, err := submit(t, w, 1, "auth.ts", mustPatch(t, []byte{}, []byte(baseDoc)), "alice-claude"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := submit(t, w, 2, "auth.ts", mustPatch(t, []byte(baseDoc), []byte(aliceWant)), "alice-claude"); err != nil {
		t.Fatal(err)
	}
	// Bob rewrites the same "3000" region from the stale base → semantic
	// conflict. The barrier must open, Bob fenced, Alice (the last author
	// to complete a patch on the path) becomes resolver.
	bobWant := strings.Replace(baseDoc, "3000", "3001", 1)
	_, err := submit(t, w, 2, "auth.ts", mustPatch(t, []byte(baseDoc), []byte(bobWant)), "bob-codex")
	rej, ok := err.(*RejectionError)
	if !ok || rej.Reason != RejectSemanticConflict {
		t.Fatalf("expected semantic conflict, got %v", err)
	}
	if rej.ResolverAgent != "alice-claude" {
		t.Fatalf("resolver = %q want alice-claude (last completer)", rej.ResolverAgent)
	}
	if !w.IsBarriered("auth.ts") {
		t.Fatal("barrier not open")
	}

	// A third agent is fenced with conflict_barrier; the resolver passes.
	_, err = submit(t, w, w.Seq(), "auth.ts", mustPatch(t, []byte(aliceWant), []byte(aliceWant+"// done\n")), "carol-opencode")
	if err == nil || err.(*RejectionError).Reason != RejectConflictBarrier {
		t.Fatalf("expected barrier fencing, got %v", err)
	}
	fixed := strings.Replace(aliceWant, "3000", "process.env.PORT", 1)
	if _, err := submit(t, w, w.Seq(), "auth.ts", mustPatch(t, []byte(aliceWant), []byte(fixed)), "alice-claude"); err != nil {
		t.Fatalf("resolver fenced: %v", err)
	}

	notified, ok := w.ResolveBarrier("auth.ts")
	if !ok || !contains(notified, "bob-codex") {
		t.Fatalf("resolve barrier notified = %v ok=%v", notified, ok)
	}
	if w.IsBarriered("auth.ts") {
		t.Fatal("barrier still locked after resolve")
	}
	// Non-resolver edits work again after unlock.
	if _, err := submit(t, w, w.Seq(), "auth.ts", mustPatch(t, []byte(fixed), []byte(fixed+"// bob\n")), "bob-codex"); err != nil {
		t.Fatalf("post-unlock edit fenced: %v", err)
	}
}

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

func TestBlobReplaceOptimisticCheck(t *testing.T) {
	w := NewWorkspaceState()
	if _, err := w.Accept(vmprotocol.Envelope{
		AuthorDeviceID: "d1", BaseSeq: 0,
		Operation: vmprotocol.FileOperation{ID: "c", Path: "logo.png", Kind: vmprotocol.OpCreate},
	}); err != nil {
		t.Fatal(err)
	}
	// Simulate manifest upload state: current hash of the empty text file.
	current := w.currentHash("logo.png")
	if _, err := w.Accept(vmprotocol.Envelope{
		AuthorDeviceID: "d1", BaseSeq: 1,
		Operation: vmprotocol.FileOperation{ID: "b1", Path: "logo.png", Kind: vmprotocol.OpBlobReplace,
			Blob: &vmprotocol.BlobReplace{BaseHash: current, Manifest: "sha256:" + strings.Repeat("aa", 32)}},
	}); err != nil {
		t.Fatalf("fresh blob replace: %v", err)
	}
	// Stale base hash must be rejected with the current hash for recovery.
	_, err := w.Accept(vmprotocol.Envelope{
		AuthorDeviceID: "d2", BaseSeq: 1,
		Operation: vmprotocol.FileOperation{ID: "b2", Path: "logo.png", Kind: vmprotocol.OpBlobReplace,
			Blob: &vmprotocol.BlobReplace{BaseHash: current, Manifest: "sha256:" + strings.Repeat("bb", 32)}},
	})
	rej, ok := err.(*RejectionError)
	if !ok || rej.Reason != RejectBaseMismatch {
		t.Fatalf("expected base mismatch, got %v", err)
	}
	if rej.CurrentHash != "sha256:"+strings.Repeat("aa", 32) {
		t.Fatalf("current hash not reported: %+v", rej)
	}
}

func TestStaleBaseSequenceRejected(t *testing.T) {
	w := NewWorkspaceState()
	if _, err := w.Accept(vmprotocol.Envelope{
		AuthorDeviceID: "d1", AgentSessionID: "a1", BaseSeq: 0,
		Operation: vmprotocol.FileOperation{ID: "c", Path: "f", Kind: vmprotocol.OpCreate},
	}); err != nil {
		t.Fatal(err)
	}
	v1 := []byte("one two three")
	if _, err := submit(t, w, 1, "f", mustPatch(t, []byte{}, v1), "a1"); err != nil {
		t.Fatal(err)
	}
	v2 := []byte("one two three four")
	if _, err := submit(t, w, 2, "f", mustPatch(t, v1, v2), "a1"); err != nil {
		t.Fatal(err)
	}
	// A patch authored against seq 1 content but submitted with the correct
	// old hash for seq 1 still applies only if its base hash matches seq 1's
	// recorded hash; a wrong hash is a base mismatch.
	stale := &vmprotocol.TextPatch{BaseHash: "sha256:" + strings.Repeat("ff", 32), Splices: []vmprotocol.Splice{{Offset: 0, Insert: "x"}}}
	_, err := submit(t, w, 1, "f", stale, "a2")
	rej, ok := err.(*RejectionError)
	if !ok || rej.Reason != RejectBaseMismatch {
		t.Fatalf("expected base mismatch for wrong hash, got %v", err)
	}
}

func TestDiffTextFallsBackForBinary(t *testing.T) {
	bin := []byte{0x00, 0x01, 0xff, 0xfe}
	if _, ok := DiffText(bin, bin); ok {
		t.Fatal("binary content must not be text-diffable")
	}
	p, ok := DiffText([]byte("你好，世界"), []byte("你好，Tutti"))
	if !ok {
		t.Fatal("UTF-8 content must be text-diffable")
	}
	out, err := ApplyPatch([]byte("你好，世界"), *p)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != "你好，Tutti" {
		t.Fatalf("unicode apply = %q", out)
	}
}

func TestSnapshotRoundTripThroughCAS(t *testing.T) {
	w := NewWorkspaceState()
	store := vmcas.NewMemoryStore()
	if _, err := w.Accept(vmprotocol.Envelope{AuthorDeviceID: "d", BaseSeq: 0,
		Operation: vmprotocol.FileOperation{ID: "1", Path: "src", Kind: vmprotocol.OpMkdir, IsDir: true}}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Accept(vmprotocol.Envelope{AuthorDeviceID: "d", BaseSeq: 1,
		Operation: vmprotocol.FileOperation{ID: "2", Path: "src/app.ts", Kind: vmprotocol.OpCreate}}); err != nil {
		t.Fatal(err)
	}
	content := []byte(strings.Repeat("export const x = 1\n", 1000))
	if _, err := submit(t, w, 2, "src/app.ts", mustPatch(t, []byte{}, content), "a1"); err != nil {
		t.Fatal(err)
	}

	snap, err := w.Snapshot("room", vmprotocol.SnapshotPeriodic, store)
	if err != nil {
		t.Fatal(err)
	}
	if snap.ServerSeq != w.Seq() {
		t.Fatalf("snapshot seq %d != %d", snap.ServerSeq, w.Seq())
	}

	// Bootstrap a replica from the snapshot and materialize the text file.
	rep := NewReplica("dev-new")
	if err := rep.Bootstrap(snap, nil); err != nil {
		t.Fatal(err)
	}
	got, err := rep.MaterializePath("src/app.ts", store)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, content) {
		t.Fatal("snapshot round trip content mismatch")
	}
	if !rep.IsFull() {
		t.Fatal("single-file snapshot must be full after materialize")
	}
}

func TestReplicaAckCorrelationAndGap(t *testing.T) {
	rep := NewReplica("dev-me")
	seqEnv := func(seq uint64, author, opID string) vmprotocol.Envelope {
		return vmprotocol.Envelope{
			ServerSeq: seq, AuthorDeviceID: author, OperationID: opID,
			Operation: vmprotocol.FileOperation{ID: opID, Path: "f", Kind: vmprotocol.OpCreate},
		}
	}
	rep.Submit("op-1")
	acked, err := rep.ApplyServerOp(seqEnv(1, "dev-me", "op-1"))
	if err != nil || !acked {
		t.Fatalf("own op ack: acked=%v err=%v", acked, err)
	}
	if len(rep.PendingAcks()) != 0 {
		t.Fatalf("pending after ack: %v", rep.PendingAcks())
	}
	// Duplicate delivery is a no-op.
	acked, err = rep.ApplyServerOp(seqEnv(1, "dev-me", "op-1"))
	if err != nil || acked {
		t.Fatalf("duplicate: acked=%v err=%v", acked, err)
	}
	// Gap rejects with ErrSeqGap.
	if _, err := rep.ApplyServerOp(seqEnv(3, "dev-other", "op-3")); err == nil {
		t.Fatal("gap accepted")
	}
}

func TestEnvironmentPathDetection(t *testing.T) {
	if !IsEnvironmentPath(".opentuttivm/Dockerfile") || !IsEnvironmentPath(".devcontainer/devcontainer.json") {
		t.Fatal("environment paths not detected")
	}
	if IsEnvironmentPath("Dockerfile") {
		t.Fatal("wrong path detected")
	}
}

func TestRestoreKeepsTextClassForLaterPatches(t *testing.T) {
	store := vmcas.NewMemoryStore()
	w := NewWorkspaceState()
	mustAccept(t, w, vmprotocol.FileOperation{ID: "1", Path: "a.ts", Kind: vmprotocol.OpCreate})
	if _, err := submit(t, w, 0, "a.ts", mustPatch(t, []byte{}, []byte("hello room")), "a1"); err != nil {
		t.Fatal(err)
	}
	snap, err := w.Snapshot("room", vmprotocol.SnapshotPeriodic, store)
	if err != nil {
		t.Fatal(err)
	}

	// Bootstrap restores the entry as OT text, not a blob.
	restored := NewWorkspaceState()
	if err := restored.RestoreSnapshot(snap); err != nil {
		t.Fatal(err)
	}
	if err := restored.MaterializeTexts(store); err != nil {
		t.Fatal(err)
	}
	info, ok := restored.EntryInfo("a.ts")
	if !ok || info.IsDir || !info.IsText {
		t.Fatalf("restored class = %+v (must stay text)", info)
	}
	// A later text patch applies on top of the restored revision.
	next := mustPatch(t, []byte("hello room"), []byte("hello room, patched"))
	if _, err := submit(t, restored, snap.ServerSeq, "a.ts", next, "a2"); err != nil {
		t.Fatalf("text patch after bootstrap rejected: %v", err)
	}
	if got := string(restored.files["a.ts"].Content); got != "hello room, patched" {
		t.Fatalf("content = %q", got)
	}
}

func TestWorkspacePathTraversalRejected(t *testing.T) {
	w := NewWorkspaceState()
	for _, path := range []string{"../escape", "a/../../b", "/abs", "", ".", "..", "a//b"} {
		env := vmprotocol.Envelope{AuthorDeviceID: "d", Operation: vmprotocol.FileOperation{ID: "x", Path: path, Kind: vmprotocol.OpCreate}}
		if _, err := w.Accept(env); err == nil {
			t.Fatalf("path %q accepted", path)
		}
	}
	// Rename targets are validated too.
	env := vmprotocol.Envelope{AuthorDeviceID: "d", Operation: vmprotocol.FileOperation{
		ID: "x", Path: "ok.txt", Kind: vmprotocol.OpRename,
		Rename: &vmprotocol.Rename{OldPath: "ok.txt", NewPath: "../out"},
	}}
	if _, err := w.Accept(env); err == nil {
		t.Fatal("escape rename accepted")
	}
	// Snapshot entries with traversal paths cannot be restored.
	bad := vmprotocol.WorkspaceSnapshot{ServerSeq: 1, Entries: []vmprotocol.TreeEntry{
		{Path: "../../etc/passwd", Kind: vmprotocol.TreeEntryFile, Manifest: "sha256:x"},
	}}
	if err := w.RestoreSnapshot(bad); err == nil {
		t.Fatal("traversal snapshot entry restored")
	}
}

func TestAcceptDeduplicatesBeforeQuotaAndRejectsChangedDuplicate(t *testing.T) {
	w := NewWorkspaceState()
	w.MaxEntries = 1
	env := vmprotocol.Envelope{RoomID: "room", AuthorDeviceID: "device", OperationID: "op-1", BaseSeq: 0,
		Operation: vmprotocol.FileOperation{ID: "op-1", Path: "file.txt", Kind: vmprotocol.OpCreate}}
	accepted, err := w.Accept(env)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := w.Accept(env)
	if err != nil || retry != accepted {
		t.Fatalf("retry = %+v, err=%v; want original %+v", retry, err, accepted)
	}
	changed := env
	changed.Operation.Path = "other.txt"
	if _, err := w.Accept(changed); err == nil {
		t.Fatal("changed duplicate identity was accepted")
	}
	if w.Seq() != accepted.ServerSeq {
		t.Fatalf("changed duplicate advanced sequence to %d", w.Seq())
	}
}

func TestBarrierBindsResolverToDevice(t *testing.T) {
	w := NewWorkspaceState()
	mustAccept(t, w, vmprotocol.FileOperation{ID: "1", Path: "cfg.ts", Kind: vmprotocol.OpCreate})
	base := []byte("port = 3000")
	if _, err := submit(t, w, 0, "cfg.ts", mustPatch(t, []byte{}, base), "alice"); err != nil {
		t.Fatal(err)
	}
	seedSeq := w.Seq()
	// Alice and Bob both rewrite the same region from the shared base.
	aliceWant := []byte("port = ENV")
	if _, err := submit(t, w, seedSeq, "cfg.ts", mustPatch(t, base, aliceWant), "alice"); err != nil {
		t.Fatal(err)
	}
	// Bob collides from the stale base → barrier opens, alice resolves.
	if _, err := submit(t, w, seedSeq, "cfg.ts", mustPatch(t, base, []byte("port = 3001")), "bob"); err == nil {
		t.Fatal("expected semantic conflict")
	}
	// Mallory claims alice's session id from another device: fenced.
	impersonated := vmprotocol.Envelope{
		AuthorDeviceID: "dev-mallory", AgentSessionID: "alice", BaseSeq: w.Seq(),
		Operation: vmprotocol.FileOperation{ID: "m", Path: "cfg.ts", Kind: vmprotocol.OpTextPatch,
			Patch: mustPatch(t, aliceWant, []byte("mallory"))},
	}
	if _, err := w.Accept(impersonated); err == nil {
		t.Fatal("impersonated resolver passed the barrier")
	}
	// The real resolver's device passes.
	if _, err := submit(t, w, w.Seq(), "cfg.ts", mustPatch(t, aliceWant, []byte("fixed")), "alice"); err != nil {
		t.Fatalf("real resolver fenced: %v", err)
	}
}

func mustAccept(t *testing.T, w *WorkspaceState, op vmprotocol.FileOperation) vmprotocol.Envelope {
	t.Helper()
	env, err := w.Accept(vmprotocol.Envelope{AuthorDeviceID: "dev-a", Operation: op})
	if err != nil {
		t.Fatal(err)
	}
	return env
}
