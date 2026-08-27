// Package vmcas implements the content-addressed object layer of the
// OpenTuttiVM workspace: 4 MiB fixed chunks addressed by SHA-256, file
// manifests listing chunk hashes, and local-filesystem object storage.
//
// The server keeps objects under its data directory; room-sync keeps a CAS
// cache of the same layout. Chunk upload is idempotent and
// content-addressed: clients ask which chunks are missing and PUT only those.
package vmcas

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// ChunkSize is the fixed chunk size in bytes (4 MiB). The final chunk of a
// file may be shorter.
const ChunkSize = 4 * 1024 * 1024

// Manifest describes one file's content as an ordered list of chunk hashes.
// The manifest's own identity is Hash = SHA-256 of the canonical JSON body
// with Hash omitted.
type Manifest struct {
	// Size is the total file size in bytes.
	Size int64 `json:"size"`
	// Chunks lists chunk content hashes in order; the last chunk may be
	// shorter than ChunkSize.
	Chunks []string `json:"chunks"`
	// Hash is the manifest's content hash.
	Hash string `json:"hash,omitempty"`
}

// ChunkHash computes the canonical chunk object hash for content.
func ChunkHash(content []byte) string {
	sum := sha256.Sum256(content)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// BuildManifest chunks r into ChunkSize pieces and returns the manifest plus
// the chunk contents in order, so callers can upload missing chunks without
// re-reading.
func BuildManifest(r io.Reader) (Manifest, [][]byte, error) {
	var chunks [][]byte
	var hashes []string
	var size int64
	buf := make([]byte, ChunkSize)
	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			chunks = append(chunks, chunk)
			hashes = append(hashes, ChunkHash(chunk))
			size += int64(n)
		}
		if err == nil {
			continue
		}
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		return Manifest{}, nil, fmt.Errorf("read chunk: %w", err)
	}
	if chunks == nil {
		// Empty file: a manifest with zero chunks is valid; size 0.
		chunks = [][]byte{}
		hashes = []string{}
	}
	m := Manifest{Size: size, Chunks: hashes}
	m.Hash = m.ComputeHash()
	return m, chunks, nil
}

// Body returns the canonical JSON body the manifest hash covers — the
// manifest object stored in CAS under Hash is exactly these bytes (without
// the hash field), so a stored manifest self-verifies.
func (m Manifest) Body() []byte {
	body := struct {
		Size   int64    `json:"size"`
		Chunks []string `json:"chunks"`
	}{Size: m.Size, Chunks: m.Chunks}
	data, err := json.Marshal(body)
	if err != nil {
		// json.Marshal of int64+[]string cannot fail.
		panic(err)
	}
	return data
}

// ComputeHash returns the content hash of the manifest body.
func (m Manifest) ComputeHash() string {
	sum := sha256.Sum256(m.Body())
	return "sha256:" + hex.EncodeToString(sum[:])
}

// DecodeManifest parses manifest JSON. Bodies without a hash field are
// accepted and self-hashed: inside CAS the object key already guarantees
// integrity, and manifests are stored under their body hash. A declared
// hash is always verified.
func DecodeManifest(data []byte) (Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return Manifest{}, fmt.Errorf("decode manifest: %w", err)
	}
	if m.Hash == "" {
		m.Hash = m.ComputeHash()
		return m, nil
	}
	if got := m.ComputeHash(); got != m.Hash {
		return Manifest{}, fmt.Errorf("manifest hash mismatch: declared %s computed %s", m.Hash, got)
	}
	return m, nil
}

// Encode serializes the manifest including its hash.
func (m Manifest) Encode() ([]byte, error) {
	if m.Hash == "" {
		m.Hash = m.ComputeHash()
	}
	return json.Marshal(m)
}
