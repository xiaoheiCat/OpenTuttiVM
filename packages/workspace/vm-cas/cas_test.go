package vmcas

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"strings"
	"testing"
)

func TestBuildManifestChunksAt4MiB(t *testing.T) {
	// 9 MiB minus 1 byte → two full chunks and one 4 MiB - 1 remainder.
	content := make([]byte, 2*ChunkSize+ChunkSize-1)
	if _, err := rand.Read(content); err != nil {
		t.Fatal(err)
	}
	m, chunks, err := BuildManifest(bytes.NewReader(content))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if m.Size != int64(len(content)) {
		t.Fatalf("size = %d want %d", m.Size, len(content))
	}
	if len(m.Chunks) != 3 || len(chunks) != 3 {
		t.Fatalf("chunks = %d want 3", len(m.Chunks))
	}
	if len(chunks[2]) != ChunkSize-1 {
		t.Fatalf("last chunk = %d want %d", len(chunks[2]), ChunkSize-1)
	}
	// Local modification of one middle byte must change exactly that chunk's
	// hash, so re-upload only touches one chunk.
	content[ChunkSize+100] ^= 0xff
	m2, _, err := BuildManifest(bytes.NewReader(content))
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if m.Chunks[0] != m2.Chunks[0] {
		t.Fatal("first chunk hash changed for unrelated edit")
	}
	if m.Chunks[1] == m2.Chunks[1] {
		t.Fatal("edited chunk hash did not change")
	}
}

func TestManifestSelfHashVerification(t *testing.T) {
	m, _, err := BuildManifest(strings.NewReader("hello open tutti"))
	if err != nil {
		t.Fatal(err)
	}
	data, err := m.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeManifest(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Hash != m.ComputeHash() {
		t.Fatal("hash drift")
	}
	// Tampered body must fail verification.
	tampered := strings.Replace(string(data), `"size":16`, `"size":15`, 1)
	if _, err := DecodeManifest([]byte(tampered)); err == nil {
		t.Fatal("tampered manifest accepted")
	}
}

func TestLocalStoreRoundTripAndIdempotency(t *testing.T) {
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("chunk-content-0123456789")
	sum := sha256.Sum256(content)
	hash := "sha256:" + hex.EncodeToString(sum[:])

	if err := store.Put(hash, content); err != nil {
		t.Fatalf("put: %v", err)
	}
	// Same-content PUT is an idempotent no-op.
	if err := store.Put(hash, content); err != nil {
		t.Fatalf("idempotent put: %v", err)
	}
	got, err := store.Get(hash)
	if err != nil || !bytes.Equal(got, content) {
		t.Fatalf("get: %v %q", err, got)
	}
	// Hash-keyed write with different content must be rejected.
	if err := store.Put(hash, []byte("other")); err == nil {
		t.Fatal("mismatched content accepted")
	}
	// Layout uses the sha256/<2>/<62> directory split.
	p, _ := store.path(hash)
	if !strings.Contains(filepath.ToSlash(p), "/sha256/") {
		t.Fatalf("unexpected layout %q", p)
	}
	missing, _ := store.Missing([]string{hash, "sha256:" + strings.Repeat("0", 64)})
	if len(missing) != 1 || missing[0] != "sha256:"+strings.Repeat("0", 64) {
		t.Fatalf("missing = %v", missing)
	}
}

func TestMaterializeAssemblesFile(t *testing.T) {
	store := NewMemoryStore()
	content := bytes.Repeat([]byte("abcdefgh"), (ChunkSize/8)+3) // spans two chunks
	m, chunks, err := BuildManifest(bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	for i, c := range chunks {
		if err := store.Put(m.Chunks[i], c); err != nil {
			t.Fatal(err)
		}
	}
	out, err := Materialize(store, m)
	if err != nil {
		t.Fatalf("materialize: %v", err)
	}
	if !bytes.Equal(out, content) {
		t.Fatal("materialized content differs")
	}
}
