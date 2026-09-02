package vmsync

import (
	"bytes"
	"fmt"
	"unicode/utf8"

	vmcas "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-cas"
	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
)

// ConvertChange turns one whole-file write into a Room File Operation:
// UTF-8 text edits become server-authoritative text patches (byte-range
// OT); binaries and oversized files become CAS blob replaces guarded by an
// optimistic version check. oldBaseHash is the hash the authoritative
// state currently expects for the path — the raw content hash while the
// file is text-tracked, the manifest hash once blob-tracked
// (WorkspaceState.CurrentBaseHash). trackedAsBlob reports that tracked
// kind: a blob-tracked entry stays blob-tracked even when its new
// content is small valid UTF-8, because the authoritative engine rejects
// text patches against non-text entries (the file would become
// unwritable through ordinary edits). ChunkSink uploads blob chunks
// (and the manifest object) before the operation is submitted.
func ConvertChange(id, path, oldBaseHash string, trackedAsBlob bool, oldContent, newContent []byte, chunkSink func(manifest vmcas.Manifest, chunks [][]byte) error) (vmprotocol.FileOperation, error) {
	if bytes.Equal(oldContent, newContent) {
		return vmprotocol.FileOperation{}, fmt.Errorf("no content change on %s", path)
	}
	if trackedAsBlob || !isTextClass(oldContent, newContent) {
		return blobReplace(id, path, oldBaseHash, newContent, chunkSink)
	}
	base := oldBaseHash
	if base == "" {
		base = ContentHash(oldContent)
	}
	patch, changed := DiffText(oldContent, newContent)
	if !changed {
		return vmprotocol.FileOperation{}, fmt.Errorf("no content change on %s", path)
	}
	return vmprotocol.FileOperation{
		ID: id, Path: path, Kind: vmprotocol.OpTextPatch,
		Patch: &vmprotocol.TextPatch{BaseHash: base, Splices: patch.Splices},
	}, nil
}

func blobReplace(id, path, oldBaseHash string, newContent []byte, chunkSink func(manifest vmcas.Manifest, chunks [][]byte) error) (vmprotocol.FileOperation, error) {
	manifest, chunks, err := vmcas.BuildManifest(bytes.NewReader(newContent))
	if err != nil {
		return vmprotocol.FileOperation{}, err
	}
	if chunkSink == nil {
		return vmprotocol.FileOperation{}, fmt.Errorf("blob change on %s requires a chunk sink", path)
	}
	if err := chunkSink(manifest, chunks); err != nil {
		return vmprotocol.FileOperation{}, fmt.Errorf("upload chunks for %s: %w", path, err)
	}
	return vmprotocol.FileOperation{
		ID: id, Path: path, Kind: vmprotocol.OpBlobReplace,
		Blob: &vmprotocol.BlobReplace{BaseHash: oldBaseHash, Manifest: manifest.Hash, Size: manifest.Size},
	}, nil
}

// isTextClass reports whether old→new should sync as a text patch: both
// sides valid UTF-8 and within the OT text ceiling.
func isTextClass(oldContent, newContent []byte) bool {
	if len(newContent) > MaxTextFile || len(oldContent) > MaxTextFile {
		return false
	}
	return utf8.Valid(oldContent) && utf8.Valid(newContent)
}
