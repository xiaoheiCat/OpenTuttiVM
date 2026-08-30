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
