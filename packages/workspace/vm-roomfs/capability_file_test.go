package roomfs

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestCapabilityFileCreatesMissingTarget(t *testing.T) {
	path := filepath.Join(t.TempDir(), "roomfs.cap")
	if err := WriteCapabilityFile(path, "secret"); err != nil {
		t.Fatal(err)
	}
	got, err := ReadCapabilityFile(path)
	if err != nil || got != "secret" {
		t.Fatalf("read = %q, %v", got, err)
	}
	if runtime.GOOS != "windows" {
		info, err := os.Lstat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("permissions = %o, want 600", info.Mode().Perm())
		}
	}
}

func TestCapabilityFileReplacesExistingSafeFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "roomfs.cap")
	if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := WriteCapabilityFile(path, "new"); err != nil {
		t.Fatal(err)
	}
	got, err := ReadCapabilityFile(path)
	if err != nil || got != "new" {
		t.Fatalf("read = %q, %v", got, err)
	}
}

func TestCapabilityFileRejectsUnsafeTargets(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "roomfs.cap")
	if err := os.WriteFile(target, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" {
		if err := WriteCapabilityFile(target, "new"); err == nil {
			t.Fatal("broad existing file was replaced")
		}
		if got, err := os.ReadFile(target); err != nil || string(got) != "old" {
			t.Fatalf("target changed: %q %v", got, err)
		}
	}
	link := filepath.Join(dir, "link.cap")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if err := WriteCapabilityFile(link, "attack"); err == nil {
		t.Fatal("symlink target accepted")
	}
	directory := filepath.Join(dir, "directory.cap")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := WriteCapabilityFile(directory, "attack"); err == nil {
		t.Fatal("directory target accepted")
	}
}

func TestCapabilityFileSafeRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "roomfs.cap")
	if err := os.WriteFile(path, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := ReadCapabilityFile(path)
	if err != nil || got != "secret" {
		t.Fatalf("read = %q, %v", got, err)
	}
}

func TestCapabilityFileReadRejectsSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target.cap")
	link := filepath.Join(dir, "link.cap")
	if err := os.WriteFile(target, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if _, err := ReadCapabilityFile(link); err == nil {
		t.Fatal("symlink was accepted")
	}
}
