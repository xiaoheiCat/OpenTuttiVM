package vmprotocol

import "strings"

// TreeEntryKind distinguishes file entries from directory entries in a
// workspace snapshot tree.
type TreeEntryKind string

const (
	// TreeEntryFile is a binary/large file tracked as a CAS blob.
	TreeEntryFile TreeEntryKind = "file"
	// TreeEntryText is a UTF-8 file tracked by text OT; restoring it as
	// text (not blob) keeps later text patches applicable.
	TreeEntryText TreeEntryKind = "text"
	// TreeEntryDir is a directory.
	TreeEntryDir TreeEntryKind = "dir"
)

// ValidWorkspacePath reports whether p is a safe workspace-relative path:
// slash-separated, no empty/./.. segments, no absolute or Windows-style
// prefixes, no control bytes. Every operation, rename target, and
// snapshot entry must pass this check before entering workspace state —
// it is the traversal guard for mirror-style apply-to-workspace.
func ValidWorkspacePath(p string) bool {
	if p == "" || len(p) > 4096 {
		return false
	}
	if p[0] == '/' || p[0] == '\\' {
		return false
	}
	if strings.ContainsAny(p, "\x00\\") {
		return false
	}
	start := 0
	for i := 0; i <= len(p); i++ {
		if i == len(p) || p[i] == '/' {
			seg := p[start:i]
			if seg == "" || seg == "." || seg == ".." || !validWindowsSegment(seg) {
				return false
			}
			start = i + 1
		}
	}
	return true
}

// validWindowsSegment reports whether one path segment can materialize on
// NTFS: no reserved characters (a colon can appear anywhere in a POSIX
// name — "dir/a:b.txt" — not just drive prefixes), no control characters
// (Windows rejects U+0001–U+001F outright; NUL dies earlier), no trailing
// dot/space, and no reserved device names (CON, COM1, …) even with an
// extension. Paths accepted here must stay creatable by every platform's
// replica; AGENTS.md makes Windows part of the default compatibility
// contract.
func validWindowsSegment(seg string) bool {
	if strings.ContainsAny(seg, "<>:\"|?*") {
		return false
	}
	for i := 0; i < len(seg); i++ {
		if seg[i] < 0x20 || seg[i] == 0x7f {
			return false
		}
	}
	if seg == "" || seg[len(seg)-1] == '.' || seg[len(seg)-1] == ' ' {
		return false
	}
	base := seg
	if idx := strings.IndexByte(base, '.'); idx > 0 {
		base = base[:idx]
	}
	switch strings.ToUpper(base) {
	case "CON", "PRN", "AUX", "NUL",
		"COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
		"LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9":
		return false
	}
	return true
}

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
