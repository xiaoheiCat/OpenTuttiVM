package vmcas

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var hashPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// ValidHash reports whether h is a well-formed content hash.
func ValidHash(h string) bool { return hashPattern.MatchString(h) }

// Store is the content-addressed object store. Implementations must make
// writes idempotent: storing an existing hash again succeeds as a no-op.
type Store interface {
	// Has reports whether a chunk object exists.
	Has(hash string) (bool, error)
	// Put stores chunk content under its hash after verifying it.
	Put(hash string, content []byte) error
	// Get returns chunk content.
	Get(hash string) ([]byte, error)
	// Missing filters hashes down to the ones not present.
	Missing(hashes []string) ([]string, error)
}

// MemoryStore is an in-memory Store for tests and caches.
type MemoryStore struct {
	objects map[string][]byte
}

// NewMemoryStore returns an empty memory store.
func NewMemoryStore() *MemoryStore { return &MemoryStore{objects: map[string][]byte{}} }

func (s *MemoryStore) Has(hash string) (bool, error) {
	_, ok := s.objects[hash]
	return ok, nil
}

func (s *MemoryStore) Put(hash string, content []byte) error {
	sum := sha256.Sum256(content)
	got := "sha256:" + hex.EncodeToString(sum[:])
	if got != hash {
		return fmt.Errorf("content hash mismatch: key %s content %s", hash, got)
	}
	s.objects[hash] = append([]byte(nil), content...)
	return nil
}

func (s *MemoryStore) Get(hash string) ([]byte, error) {
	c, ok := s.objects[hash]
	if !ok {
		return nil, ErrObjectNotFound
	}
	return append([]byte(nil), c...), nil
}

func (s *MemoryStore) Missing(hashes []string) ([]string, error) {
	var out []string
	for _, h := range hashes {
		if ok, _ := s.Has(h); !ok {
			out = append(out, h)
		}
	}
	return out, nil
}

// ErrObjectNotFound is returned for reads of missing objects.
var ErrObjectNotFound = errors.New("cas object not found")

// LocalStore persists chunks on the local filesystem, laid out as
// <root>/sha256/<first2>/<rest> so no directory grows unboundedly. It is the
// v1 single-machine object backend for both the server data directory and the
// room-sync cache.
type LocalStore struct {
	root string
}

// NewLocalStore opens (and creates) a store rooted at dir.
func NewLocalStore(dir string) (*LocalStore, error) {
	if dir == "" {
		return nil, errors.New("cas store root required")
	}
	if err := os.MkdirAll(filepath.Join(dir, "sha256"), 0o755); err != nil {
		return nil, fmt.Errorf("create cas root: %w", err)
	}
	return &LocalStore{root: dir}, nil
}

func (s *LocalStore) path(hash string) (string, error) {
	if !ValidHash(hash) {
		return "", fmt.Errorf("invalid cas hash %q", hash)
	}
	hex := strings.TrimPrefix(hash, "sha256:")
	return filepath.Join(s.root, "sha256", hex[:2], hex[2:]), nil
}

func (s *LocalStore) Has(hash string) (bool, error) {
	p, err := s.path(hash)
	if err != nil {
		return false, err
	}
	_, err = os.Stat(p)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}

func (s *LocalStore) Put(hash string, content []byte) error {
	p, err := s.path(hash)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(content)
	got := "sha256:" + hex.EncodeToString(sum[:])
	if got != hash {
		return fmt.Errorf("content hash mismatch: key %s content %s", hash, got)
	}
	if ok, _ := s.Has(hash); ok {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return fmt.Errorf("create chunk dir: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(p), ".chunk-*")
	if err != nil {
		return fmt.Errorf("temp chunk: %w", err)
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(content); err != nil {
		tmp.Close()
		return fmt.Errorf("write chunk: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), p); err != nil {
		return fmt.Errorf("commit chunk: %w", err)
	}
	return nil
}

func (s *LocalStore) Get(hash string) ([]byte, error) {
	p, err := s.path(hash)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(p)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrObjectNotFound
	}
	return data, err
}

func (s *LocalStore) Missing(hashes []string) ([]string, error) {
	var out []string
	for _, h := range hashes {
		if ok, _ := s.Has(h); !ok {
			out = append(out, h)
		}
	}
	return out, nil
}

// Materialize reassembles file content from chunks in the store.
func Materialize(store Store, m Manifest) ([]byte, error) {
	var buf bytes.Buffer
	if err := MaterializeTo(store, m, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// MaterializeTo streams reassembled content to w.
func MaterializeTo(store Store, m Manifest, w io.Writer) error {
	var written int64
	for i, h := range m.Chunks {
		c, err := store.Get(h)
		if err != nil {
			return fmt.Errorf("chunk %d (%s): %w", i, h, err)
		}
		if _, err := w.Write(c); err != nil {
			return err
		}
		written += int64(len(c))
	}
	if written != m.Size {
		return fmt.Errorf("manifest size %d but materialized %d", m.Size, written)
	}
	return nil
}
