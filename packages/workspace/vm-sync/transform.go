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
			for ci := range c.Patch.Splices {
				o := c.Patch.Splices[ci]
				delta := len(o.Insert) - o.DeleteLen
				oEnd := o.Offset + o.DeleteLen
				sEnd := s.Offset + s.DeleteLen

				switch {
				case oEnd <= s.Offset:
					s.Offset += delta
				case o.Offset >= sEnd:
					// Concurrent edit at or after our end boundary.
				default:
					res.Conflict = true
					if c.Agent != "" {
						res.ConflictWith = appendUnique(res.ConflictWith, c.Agent)
					}
				}
			}
		}
	}
	return res
}

// appliedPatch is one sequenced text patch the server already applied, with
// the author's agent session for conflict attribution. Patch holds the
// splices in the coordinate frame the sequencer used when applying, which is
// what transform needs.
type appliedPatch struct {
	Seq    uint64
	Agent  string
	Device string
	Patch  vmprotocol.TextPatch
}

func appendUnique(list []string, v string) []string {
	for _, x := range list {
		if x == v {
			return list
		}
	}
	return append(list, v)
}
