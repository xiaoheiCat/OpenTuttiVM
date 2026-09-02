package vmsync

import (
	"testing"

	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
)

func TestRestoreSnapshotReplacesOperationDerivedState(t *testing.T) {
	w := NewWorkspaceState()
	if _, err := w.Accept(vmprotocol.Envelope{ServerSeq: 1, AuthorDeviceID: "dev", OperationID: "mkdir", Operation: vmprotocol.FileOperation{ID: "mkdir", Path: "old", Kind: vmprotocol.OpMkdir, IsDir: true}}); err != nil {
		t.Fatal(err)
	}
	w.barriers["old"] = &barrier{Locked: true}
	w.dedupStubs["dev\x00old"] = 1
	snap := vmprotocol.WorkspaceSnapshot{RoomID: "room", ServerSeq: 0}
	if err := w.RestoreSnapshot(snap); err != nil {
		t.Fatal(err)
	}
	if len(w.files) != 0 || len(w.history) != 0 || len(w.hashLog) != 0 || len(w.barriers) != 0 || len(w.ops) != 0 || len(w.accepted) != 0 || len(w.dedupStubs) != 0 || len(w.structSeqs) != 0 {
		t.Fatalf("restore retained derived state: files=%d history=%d hashes=%d barriers=%d ops=%d accepted=%d stubs=%d structs=%d", len(w.files), len(w.history), len(w.hashLog), len(w.barriers), len(w.ops), len(w.accepted), len(w.dedupStubs), len(w.structSeqs))
	}
}

func TestWorkspaceBudgetsReleasePathsButRetainCheckpointIdentities(t *testing.T) {
	w := NewWorkspaceState()
	w.MaxLivePathBytes = int64(len("one") + len("two"))
	w.MaxIdentityBytes = int64(len("dev\x00one") + len("dev\x00two") + len("dev\x00remove") + len("dev\x00tri"))
	create := func(id, path string) vmprotocol.Envelope {
		return vmprotocol.Envelope{AuthorDeviceID: "dev", OperationID: id, Operation: vmprotocol.FileOperation{ID: id, Path: path, Kind: vmprotocol.OpCreate}}
	}
	if _, err := w.Accept(create("one", "one")); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Accept(create("two", "two")); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Accept(create("three", "three")); err == nil {
		t.Fatal("cumulative path budget accepted")
	}
	if _, err := w.Accept(vmprotocol.Envelope{AuthorDeviceID: "dev", OperationID: "remove", BaseSeq: 2, Operation: vmprotocol.FileOperation{ID: "remove", Path: "one", Kind: vmprotocol.OpRemove}}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Accept(create("tri", "tri")); err != nil {
		t.Fatalf("path budget was not released: %v", err)
	}
	w.Checkpoint(w.Seq())
	if _, err := w.Accept(create("too-long-identity", "x")); err == nil {
		t.Fatal("retained identity budget ignored")
	}
	if _, err := w.Accept(create("tri", "tri")); err != nil {
		t.Fatalf("accepted retry rejected after checkpoint: %v", err)
	}
}

func TestWorkspaceRejectsLongOperationIdentitiesAndRestoreBudget(t *testing.T) {
	w := NewWorkspaceState()
	w.MaxOperationIDBytes = 4
	w.MaxAgentSessionBytes = 4
	if _, err := w.Accept(vmprotocol.Envelope{AuthorDeviceID: "dev", OperationID: "12345", Operation: vmprotocol.FileOperation{ID: "1", Path: "x", Kind: vmprotocol.OpCreate}}); err == nil {
		t.Fatal("long operation id accepted")
	}
	if _, err := w.Accept(vmprotocol.Envelope{AuthorDeviceID: "dev", OperationID: "ok", AgentSessionID: "12345", Operation: vmprotocol.FileOperation{ID: "ok", Path: "x", Kind: vmprotocol.OpCreate}}); err == nil {
		t.Fatal("long agent session id accepted")
	}
	w.MaxLivePathBytes = 3
	if err := w.RestoreSnapshot(vmprotocol.WorkspaceSnapshot{Entries: []vmprotocol.TreeEntry{{Path: "abcd", Kind: vmprotocol.TreeEntryDir}}}); err == nil {
		t.Fatal("over-budget snapshot restored")
	}
}

func TestWorkspaceContentBudgetCoversTextAndBlobLogicalSize(t *testing.T) {
	w := NewWorkspaceState()
	w.MaxContentBytes = 3
	create := func(id, path string) vmprotocol.Envelope {
		return vmprotocol.Envelope{AuthorDeviceID: "dev", OperationID: id, Operation: vmprotocol.FileOperation{ID: id, Path: path, Kind: vmprotocol.OpCreate}}
	}
	if _, err := w.Accept(create("a", "a")); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Accept(vmprotocol.Envelope{AuthorDeviceID: "dev", OperationID: "p", BaseSeq: 1, Operation: vmprotocol.FileOperation{ID: "p", Path: "a", Kind: vmprotocol.OpTextPatch, Patch: &vmprotocol.TextPatch{BaseHash: ContentHash(nil), Splices: []vmprotocol.Splice{{Offset: 0, Insert: "ab"}}}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Accept(create("b", "b")); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Accept(vmprotocol.Envelope{AuthorDeviceID: "dev", OperationID: "blob", BaseSeq: 3, Operation: vmprotocol.FileOperation{ID: "blob", Path: "b", Kind: vmprotocol.OpBlobReplace, Blob: &vmprotocol.BlobReplace{BaseHash: ContentHash(nil), Manifest: "sha256:x", Size: 2}}}); err == nil {
		t.Fatal("blob exceeding cumulative content budget accepted")
	}
}
