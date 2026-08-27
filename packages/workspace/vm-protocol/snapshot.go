package vmprotocol

// TreeEntryKind distinguishes file entries from directory entries in a
// workspace snapshot tree.
type TreeEntryKind string

const (
	// TreeEntryFile is a regular file tracked by the room workspace.
	TreeEntryFile TreeEntryKind = "file"
	// TreeEntryDir is a directory.
	TreeEntryDir TreeEntryKind = "dir"
)

// TreeEntry describes one path in a workspace snapshot. Text and binary files
// alike are materialized through CAS manifests at snapshot time; the
// collaboration layer keeps text files in the operation stream, the snapshot
// layer persists full content.
type TreeEntry struct {
	Path string        `json:"path"`
	Kind TreeEntryKind `json:"kind"`
	// Manifest is the CAS manifest hash for files; empty for directories.
	Manifest string `json:"manifest,omitempty"`
	// Size is the file size in bytes at snapshot time.
	Size int64 `json:"size,omitempty"`
	// Mode is the POSIX permission bits.
	Mode uint32 `json:"mode"`
}

// WorkspaceSnapshot is a checkpoint of the authoritative workspace state at
// ServerSeq. Snapshots are the persistence, bootstrap, export, and
// owner-transfer layer; the in-memory operation log ahead of the newest
// snapshot is intentionally volatile per the no-recovery server failure
// model.
type WorkspaceSnapshot struct {
	RoomID string `json:"room_id"`
	// ServerSeq is the sequence the snapshot covers; replaying operations
	// with ServerSeq > this value on top of the snapshot reconstructs the
	// current state.
	ServerSeq uint64 `json:"server_seq"`
	// RootTreeHash is the content hash over the sorted entry list.
	RootTreeHash string `json:"root_tree_hash"`
	// Entries lists every path in the workspace.
	Entries []TreeEntry `json:"entries"`
	// Reason records what triggered the checkpoint.
	Reason SnapshotReason `json:"reason"`
}

// SnapshotReason records why a checkpoint was taken.
type SnapshotReason string

const (
	// SnapshotPeriodic covers timer- and size-threshold checkpoints.
	SnapshotPeriodic SnapshotReason = "periodic"
	// SnapshotBootstrap covers checkpoints taken to give a newly joined
	// device a stable bootstrap point.
	SnapshotBootstrap SnapshotReason = "bootstrap"
	// SnapshotOwnerTransfer covers checkpoints required before committing an
	// ownership transfer so the incoming owner can complete a full replica.
	SnapshotOwnerTransfer SnapshotReason = "owner_transfer"
)
