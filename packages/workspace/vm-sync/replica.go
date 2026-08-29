package vmsync

import (
	"fmt"
	"sort"

	vmcas "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-cas"
	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
)

// ApplySequenced applies an already-sequenced server envelope structurally,
// without transformation, base validation, or barrier fencing — the server
// already canonicalized it. Replicas use this to mirror authoritative state.
func (w *WorkspaceState) ApplySequenced(env vmprotocol.Envelope) error {
	op := env.Operation
	switch op.Kind {
	case vmprotocol.OpCreate:
		if _, exists := w.files[op.Path]; !exists {
			w.applyCreate(op)
		}
	case vmprotocol.OpRemove:
		w.applyRemove(&env)
	case vmprotocol.OpMkdir:
		w.applyMkdir(op)
	case vmprotocol.OpRmdir:
		w.applyRmdir(op)
	case vmprotocol.OpRename:
		w.applyRename(&env)
	case vmprotocol.OpMetadataChange:
		w.applyMetadata(op)
	case vmprotocol.OpTextPatch:
		f := w.files[op.Path]
		if f == nil || f.IsDir {
			return nil // path removed concurrently; snapshot resync heals
		}
		if f.Kind == kindBlob {
			// Text edit on a file that became a blob server-side: fetch the
			// current state via CAS on next read.
			return nil
		}
		if op.Patch == nil {
			return nil
		}
		// A restored-from-snapshot entry still holding only a manifest
		// must materialize before the patch applies: applying to nil
		// would silently drop the base (offset 0) or mis-handle offsets.
		if f.Content == nil && f.Manifest != "" {
			if err := w.materializeViaHook(op.Path); err != nil {
				return fmt.Errorf("materialize %s for patch: %w", op.Path, err)
			}
		}
		next, err := ApplyPatch(f.Content, *op.Patch)
		if err != nil {
			// Authoritative offsets no longer line up with local state:
			// surface the divergence so the caller gap-resyncs instead of
			// advancing AppliedSeq over a permanently skewed replica.
			return fmt.Errorf("apply patch to %s: %w", op.Path, err)
		}
		f.Content = next
	case vmprotocol.OpBlobReplace:
		f := w.files[op.Path]
		if f == nil || f.IsDir {
			return nil
		}
		if op.Blob == nil {
			return nil
		}
		f.Kind = kindBlob
		f.Manifest = op.Blob.Manifest
		f.Content = nil
		// The server verified the declaration against the chunk graph;
		// replicas mirror the authoritative size.
		f.Size = op.Blob.Size
		f.Materialized = false
		// Full-policy owners eagerly fetch accepted replacements so the
		// promised server-failure-survival copy exists immediately;
		// lazy replicas defer (EagerBlobs) and materialize on first
		// read — the manifest reference itself is already authoritative.
		if w.Materializer != nil && w.EagerBlobs {
			if err := w.Materializer(op.Path); err != nil {
				return fmt.Errorf("materialize replaced blob %s: %w", op.Path, err)
			}
		}
	default:
		return fmt.Errorf("unknown operation kind %q", op.Kind)
	}
	return nil
}

// Replica is the client-side projection of one room workspace used by
// room-sync. It applies already-sequenced server operations, correlates
// acknowledgements of its own submissions, and materializes blob content
// from CAS on demand.
//
// Replica policy follows the room role: the owner device keeps a full
// replica strong enough to survive a server failure (Apply to Workspace);
// participants keep a working cache.
type Replica struct {
	State *WorkspaceState
	// DeviceID identifies this replica's author for ack correlation.
	DeviceID string
	// AppliedSeq is the highest server sequence applied locally.
	AppliedSeq uint64
	// pending tracks locally submitted operation IDs awaiting their server
	// acknowledgement.
	pending map[string]bool
}

// NewReplica returns a replica over an empty workspace.
func NewReplica(deviceID string) *Replica {
	return &Replica{State: NewWorkspaceState(), DeviceID: deviceID, pending: map[string]bool{}}
}

// Bootstrap loads a snapshot plus the operations after it. The snapshot's
// entries carry the tree and manifest hashes, so lazy replicas bootstrap
// without any CAS reads; content materializes on demand.
func (r *Replica) Bootstrap(snap vmprotocol.WorkspaceSnapshot, ops []vmprotocol.Envelope) error {
	if err := r.State.RestoreSnapshot(snap); err != nil {
		return err
	}
	r.AppliedSeq = snap.ServerSeq
	// Pending ids stay pending across the bootstrap: an operation the
	// server committed before the disconnect is folded into the snapshot
	// itself (no envelope in the replay window), and clearing it here
	// would strand the writer. The reconciliation happens when the
	// deduplicated retry gets the original envelope back: ApplyServerOp
	// below acknowledges pending operations even at-or-below AppliedSeq.
	for _, env := range ops {
		if _, err := r.ApplyServerOp(env); err != nil {
			return err
		}
	}
	return nil
}

// Submit records a locally authored operation as pending acknowledgement.
func (r *Replica) Submit(operationID string) { r.pending[operationID] = true }

// PendingAcks lists operation IDs still awaiting acknowledgement.
func (r *Replica) PendingAcks() []string {
	out := make([]string, 0, len(r.pending))
	for id := range r.pending {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// ErrSeqGap reports a missing operation; the caller must resync from a
// snapshot or replay window before applying further operations.
var ErrSeqGap = errSeqGap{}

type errSeqGap struct{}

func (errSeqGap) Error() string { return "replica sequence gap: resync required" }

// ApplyServerOp applies one sequenced envelope. It returns true when the
// envelope acknowledged one of this replica's pending operations. Duplicates
// are skipped idempotently; gaps reject with ErrSeqGap so the caller resyncs.
func (r *Replica) ApplyServerOp(env vmprotocol.Envelope) (acked bool, err error) {
	if env.ServerSeq == 0 || env.ServerSeq <= r.AppliedSeq {
		// A deduplicated retry gets the ORIGINAL accepted envelope back
		// from the server, whose sequence is at-or-below what a
		// post-disconnect bootstrap already restored (the operation was
		// folded into the snapshot). The state need not reapply, but the
		// writer is still blocked on this acknowledgement: recognize it
		// instead of silently discarding, which would strand the waiter
		// until timeout over a write that actually succeeded.
		if env.AuthorDeviceID == r.DeviceID && r.pending[env.OperationID] {
			delete(r.pending, env.OperationID)
			return true, nil
		}
		return false, nil
	}
	if env.ServerSeq != r.AppliedSeq+1 {
		return false, ErrSeqGap
	}
	if env.AuthorDeviceID == r.DeviceID && r.pending[env.OperationID] {
		delete(r.pending, env.OperationID)
		acked = true
	}
	if err := r.State.ApplySequenced(env); err != nil {
		return acked, err
	}
	r.AppliedSeq = env.ServerSeq
	return acked, nil
}

// MaterializePath fetches blob content for one path from CAS. Text files are
// always local. Returns the content and whether it is cached locally.
func (r *Replica) MaterializePath(path string, store vmcas.Store) ([]byte, error) {
	f := r.State.files[path]
	if f == nil {
		return nil, vmcas.ErrObjectNotFound
	}
	if f.IsDir {
		return nil, nil
	}
	if f.Kind == kindText {
		if f.Content == nil && f.Manifest != "" {
			// Restored-from-snapshot text: materialize from CAS so
			// text OT keeps working after bootstrap.
			if err := r.State.materializeText(path, f, store); err != nil {
				return nil, err
			}
		}
		return f.Content, nil
	}
	if f.Materialized && f.Content != nil {
		return f.Content, nil
	}
	data, err := store.Get(f.Manifest)
	if err != nil {
		return nil, err
	}
	m, err := vmcas.DecodeManifest(data)
	if err != nil {
		return nil, err
	}
	content, err := vmcas.Materialize(store, m)
	if err != nil {
		return nil, err
	}
	// A zero-byte blob materializes to nil, which the replica treats
	// as "not materialized" — normalize to an empty slice so a valid
	// zero-chunk manifest (truncated-to-zero file) counts as full and
	// does not refetch forever.
	if content == nil {
		content = []byte{}
	}
	f.Content = content
	f.Materialized = true
	return content, nil
}

// MaterializeAll eagerly fetches every blob; the full-replica policy used by
// room owners and ownership-transfer candidates.
func (r *Replica) MaterializeAll(store vmcas.Store) error {
	for _, p := range r.State.Paths() {
		if _, err := r.MaterializePath(p, store); err != nil {
			return fmt.Errorf("materialize %s: %w", p, err)
		}
	}
	return nil
}

// IsFull reports whether every path is materialized locally.
func (r *Replica) IsFull() bool {
	for _, f := range r.State.files {
		if f.IsDir {
			continue
		}
		// Restored-from-snapshot entries (text or blob) with only a
		// manifest reference are not materialized yet.
		if f.Content == nil && f.Manifest != "" {
			return false
		}
	}
	return true
}
