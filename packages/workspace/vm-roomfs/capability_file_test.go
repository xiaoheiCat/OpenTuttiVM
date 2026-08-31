package roomfs

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCapabilityFileRejectsSymlinkAndBroadPermissions(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "roomfs.cap")
	if err := os.WriteFile(target, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := WriteCapabilityFile(target, "new"); err == nil {
		t.Fatal("broad existing file was replaced")
	}
	if got, err := os.ReadFile(target); err != nil || string(got) != "old" {
		t.Fatalf("target changed: %q %v", got, err)
	}
	link := filepath.Join(dir, "link.cap")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if err := WriteCapabilityFile(link, "attack"); err == nil {
		t.Fatal("symlink target accepted")
	}
}

func TestCapabilityFileAtomicWriteAndSafeRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "roomfs.cap")
	if err := WriteCapabilityFile(path, "secret"); err != nil {
		t.Fatal(err)
	}
	got, err := ReadCapabilityFile(path)
	if err != nil || got != "secret" {
		t.Fatalf("read = %q, %v", got, err)
	}
}
