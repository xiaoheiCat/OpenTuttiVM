package vmsync

import (
	"testing"

	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
)

// TestTransformMultipleConcurrentDeletionsKeepFrame pins the coordinate
// frame: every splice of one concurrent patch compares against the
// ORIGINAL offset, so a shift is accumulated once, not compounded.
func TestTransformMultipleConcurrentDeletionsKeepFrame(t *testing.T) {
	// Reviewer's repro: concurrent deletions [0,5) and [6,8) of
	// "0123456789ABCDE"; a stale insert "X" at offset 10 must transform
	// to 3, not 5.
	patch := vmprotocol.TextPatch{
		Splices: []vmprotocol.Splice{{Offset: 10, Insert: "X"}},
	}
	concurrent := []appliedPatch{{
		Patch: vmprotocol.TextPatch{Splices: []vmprotocol.Splice{
			{Offset: 0, DeleteLen: 5},
			{Offset: 6, DeleteLen: 2},
		}},
	}}
	res := TransformPatch(&patch, concurrent)
	if res.Conflict {
		t.Fatal("non-overlapping edits must not conflict")
	}
	if got := patch.Splices[0].Offset; got != 3 {
		t.Fatalf("transformed offset = %d, want 3", got)
	}
}
