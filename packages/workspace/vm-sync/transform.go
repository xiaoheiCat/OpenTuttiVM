package vmsync

import (
	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
)

// TransformResult reports what happened while rebasing a patch onto
// concurrent history.
type TransformResult struct {
	// Conflict is true when the patch semantically collides with concurrent
	// edits and must enter the conflict barrier instead of merging.
	Conflict bool
	// ConflictWith lists the agent sessions whose concurrent operations
	// collided, so the barrier can notify them.
	ConflictWith []string
}

// TransformPatch rebases patch against already-sequenced concurrent patches
// (in sequence order). All splices — the incoming patch's and each
// concurrent patch's — are expressed in the document frame that the
// sequencer maintained at the moment the concurrent patch was applied, so a
// single left-to-right pass with the standard length-delta shift is correct.
//
// Merge rules of the hybrid collaboration model:
//
//   - concurrent edit entirely before ours: our offsets shift right by its
//     length delta (two pure inserts at the same point order history-first)
//   - concurrent edit entirely after ours: unchanged
//   - otherwise the edit regions genuinely collide — an insert strictly
//     inside a region someone else rewrote or deleted is not something v1
//     reconciles silently — and the path enters the conflict barrier
func TransformPatch(patch *vmprotocol.TextPatch, concurrent []appliedPatch) TransformResult {
	res := TransformResult{}
	for _, c := range concurrent {
		for si := range patch.Splices {
			s := &patch.Splices[si]
			// All of c's splices live in c's original frame, and so does
			// s at this point: accumulate the shift from every splice
			// and apply it once instead of mutating s.Offset mid-pass
			// (a shifted s would compare later splices in a mixed frame).
			shift := 0
			for ci := range c.Splices {
				o := c.Splices[ci]
				delta := o.InsertLen - o.DeleteLen
				oEnd := o.Offset + o.DeleteLen
				sEnd := s.Offset + s.DeleteLen

				switch {
				case oEnd <= s.Offset:
					shift += delta
				case o.Offset >= sEnd:
					// Concurrent edit at or after our end boundary.
				default:
					res.Conflict = true
					if c.Agent != "" {
						res.ConflictWith = appendUnique(res.ConflictWith, c.Agent)
					}
				}
			}
			s.Offset += shift
		}
	}
	return res
}

// appliedPatch is one sequenced text patch the server already applied, with
// the author's agent session for conflict attribution. Patch holds the
// splices in the coordinate frame the sequencer used when applying, which is
// what transform needs.
type appliedPatch struct {
	Seq     uint64
	Agent   string
	Device  string
	Splices []compactSplice
}

// compactSplice is the transform-ready summary of one splice: the
// transform needs ONLY offset/extent/insert-length, so retained
// history never pins the insert PAYLOAD — full text stays solely in
// w.ops until the next Checkpoint compacts it. Without this, a path
// edited with ~8 MiB inserts retained every payload forever (the
// 128-entry window bounded COUNT, not bytes) and one member could
// OOM the process-global server.
type compactSplice struct {
	Offset    int
	DeleteLen int
	InsertLen int
}

func appendUnique(list []string, v string) []string {
	for _, x := range list {
		if x == v {
			return list
		}
	}
	return append(list, v)
}
