// Package vmprotocol defines the OpenTuttiVM workspace collaboration
// protocol shared by open-tutti-server, open-tutti-room-sync, and
// open-tutti-fs.
//
// The protocol models a room workspace as a server-authoritative sequence of
// canonical file operations. Text files are coordinated through UTF-8
// byte-range patches, binary and large files through content-addressed blob
// replacement, and directory/metadata changes through sequenced filesystem
// operations. Snapshots over the operation log are the persistence and
// bootstrap layer, not the collaboration layer.
package vmprotocol

import "encoding/json"

// OperationKind enumerates the canonical file operation types of the hybrid
// collaboration model.
type OperationKind string

const (
	// OpTextPatch edits a UTF-8 text file in place via byte-range splices.
	OpTextPatch OperationKind = "text_patch"
	// OpBlobReplace swaps the entire content of a binary or large file for a
	// new CAS manifest, guarded by an optimistic version check.
	OpBlobReplace OperationKind = "blob_replace"
	// OpCreate creates a new file.
	OpCreate OperationKind = "create"
	// OpRemove deletes a file.
	OpRemove OperationKind = "remove"
	// OpRename moves or renames a file.
	OpRename OperationKind = "rename"
	// OpMkdir creates a directory.
	OpMkdir OperationKind = "mkdir"
	// OpRmdir removes a directory.
	OpRmdir OperationKind = "rmdir"
	// OpMetadataChange changes file mode or similar metadata.
	OpMetadataChange OperationKind = "metadata_change"
)

// Splice is one UTF-8 byte-range edit of a text file: delete DeleteLen bytes
// at Offset, then insert Insert at the same offset. Splices within one patch
// must be ordered by Offset and must not overlap.
type Splice struct {
	Offset    int    `json:"offset"`
	DeleteLen int    `json:"delete_len"`
	Insert    string `json:"insert"`
}

// TextPatch carries byte-range edits plus a guard: BaseHash is the SHA-256 of
// the file content the patch was computed against. When a transform moves the
// patch across concurrent edits, the server recomputes the expected hash; a
// mismatch is a semantic conflict for the conflict barrier, not silent data
// loss.
type TextPatch struct {
	BaseHash string   `json:"base_hash"`
	Splices  []Splice `json:"splices"`
}

// BlobReplace replaces the whole content of a file with a CAS manifest. The
// optimistic version check compares BaseHash against the server-side current
// content hash; on mismatch the server rejects the operation with the current
// state so the client can re-upload against the latest revision.
type BlobReplace struct {
	BaseHash string `json:"base_hash"`
	Manifest string `json:"manifest"`
	// Size is the manifest's declared byte size; the server verifies it
	// against the chunk graph before acceptance so authoritative state
	// can carry it without re-reading CAS.
	Size int64 `json:"size,omitempty"`
}

// Rename moves a file from OldPath to NewPath.
type Rename struct {
	OldPath string `json:"old_path"`
	NewPath string `json:"new_path"`
}

// MetadataChange updates POSIX metadata, currently file mode.
type MetadataChange struct {
	Mode uint32 `json:"mode"`
}

// FileOperation is one canonical workspace operation. Exactly one kind-specific
// payload field is set, selected by Kind.
type FileOperation struct {
	// ID uniquely identifies the operation on the originating device; combined
	// with AuthorDeviceID it is globally unique and makes application
	// idempotent.
	ID string `json:"id"`
	// Path is the slash-separated workspace-relative path. For rename it is
	// the payload old path destination handled by the Rename payload.
	Path string `json:"path"`
	// Kind selects the payload below.
	Kind OperationKind `json:"kind"`
	// IsDir marks directory-targeting create/remove operations.
	IsDir bool `json:"is_dir,omitempty"`

	Patch  *TextPatch      `json:"patch,omitempty"`
	Blob   *BlobReplace    `json:"blob,omitempty"`
	Rename *Rename         `json:"rename,omitempty"`
	Mode   *MetadataChange `json:"mode,omitempty"`
}

// Envelope wraps an operation with sequencing metadata. BaseSeq is the
// server sequence the author had observed when creating the operation;
// ServerSeq is assigned by the server's authoritative sequencer on accept and
// is zero on submission.
type Envelope struct {
	RoomID         string        `json:"room_id"`
	OperationID    string        `json:"operation_id"`
	Operation      FileOperation `json:"operation"`
	AuthorDeviceID string        `json:"author_device_id"`
	// AgentSessionID identifies the agent or terminal session that produced
	// the operation; empty for direct user actions.
	AgentSessionID string `json:"agent_session_id,omitempty"`
	BaseSeq        uint64 `json:"base_seq"`
	ServerSeq      uint64 `json:"server_seq"`
	TimestampMS    int64  `json:"timestamp_ms"`
}

// DecodeEnvelope parses an envelope from JSON wire bytes.
func DecodeEnvelope(data []byte) (Envelope, error) {
	var env Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return Envelope{}, err
	}
	return env, nil
}

// Encode serializes the envelope to JSON wire bytes.
func (e Envelope) Encode() ([]byte, error) {
	return json.Marshal(e)
}
