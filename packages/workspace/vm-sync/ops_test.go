package vmsync

import (
	"bytes"
	"strings"
	"testing"

	vmcas "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-cas"
	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
)

func TestConvertChangeTextBecomesTextPatch(t *testing.T) {
	old := []byte("func main() {\n\tfmt.Println(\"hello\")\n}\n")
	next := []byte("func main() {\n\tfmt.Println(\"hello, room\")\n\tfmt.Println(\"bye\")\n}\n")

	op, err := ConvertChange("op-1", "src/main.go", "", false, old, next, nil)
	if err != nil {
		t.Fatal(err)
	}
	if op.Kind != vmprotocol.OpTextPatch || op.Patch == nil {
		t.Fatalf("kind=%v", op.Kind)
	}
	if op.Patch.BaseHash != ContentHash(old) {
		t.Fatal("base hash must pin the pre-write revision")
	}
	applied, err := ApplyPatch(old, *op.Patch)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(applied, next) {
		t.Fatalf("patch replay mismatch:\n%q\n%q", applied, next)
	}
}

func TestConvertChangeBinaryBecomesBlobReplace(t *testing.T) {
	old := []byte{0x00, 0x01, 0x02, 0xff}
	next := append(append([]byte{}, old...), 0xfe, 0xfd)

	oldManifest, _, _ := vmcas.BuildManifest(bytes.NewReader(old))
	var uploaded [][]byte
	var uploadedManifest vmcas.Manifest
	// A blob-tracked file's base is its current manifest hash.
	op, err := ConvertChange("op-2", "assets/logo.png", oldManifest.Hash, false, old, next, func(m vmcas.Manifest, chunks [][]byte) error {
		uploadedManifest, uploaded = m, chunks
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if op.Kind != vmprotocol.OpBlobReplace || op.Blob == nil {
		t.Fatalf("kind=%v", op.Kind)
	}
	if op.Blob.Manifest != uploadedManifest.Hash {
		t.Fatalf("manifest hash %s != uploaded %s", op.Blob.Manifest, uploadedManifest.Hash)
	}
	if op.Blob.BaseHash != oldManifest.Hash {
		t.Fatal("base hash must pin the pre-write blob version")
	}
	store := vmcas.NewMemoryStore()
	for i, hash := range uploadedManifest.Chunks {
		if err := store.Put(hash, uploaded[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Put(uploadedManifest.Hash, uploadedManifest.Body()); err != nil {
		t.Fatal(err)
	}
	materialized, err := vmcas.Materialize(store, uploadedManifest)
	if err != nil || !bytes.Equal(materialized, next) {
		t.Fatalf("materialize mismatch err=%v", err)
	}
}

func TestConvertChangeOversizedTextFallsBackToBlob(t *testing.T) {
	old := bytes.Repeat([]byte("a"), MaxTextFile+1)
	next := append(bytes.Repeat([]byte("a"), MaxTextFile+1), 'b')

	// The file was text-tracked: the authoritative state pins the raw
	// content hash, so the blob replacement must carry THAT base or the
	// server rejects the text→blob transition with base_mismatch.
	base := ContentHash(old)
	op, err := ConvertChange("op-3", "big.txt", base, false, old, next, func(m vmcas.Manifest, chunks [][]byte) error {
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if op.Kind != vmprotocol.OpBlobReplace {
		t.Fatalf("oversized text must blob-replace, got %v", op.Kind)
	}
	if op.Blob.BaseHash != base {
		t.Fatalf("text→blob transition must keep the tracked text hash, got %s", op.Blob.BaseHash)
	}
}

func TestConvertChangeIdenticalWriteRefused(t *testing.T) {
	same := []byte(strings.Repeat("same\n", 10))
	if _, err := ConvertChange("op-4", "noop.txt", "", false, same, same, nil); err == nil {
		t.Fatal("identical content must not produce an operation")
	}
}

func TestConvertChangeBlobWithoutSinkRefused(t *testing.T) {
	if _, err := ConvertChange("op-5", "img.png", "", false, nil, []byte{0x00, 0xff}, nil); err == nil {
		t.Fatal("blob conversion without a chunk sink must fail before upload")
	}
}
