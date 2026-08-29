package vmsync

// EntryInfo is a read-only view of one path's live state, for replicas and
// the mirror-apply engine.
type EntryInfo struct {
	IsDir bool
	Mode  uint32
	// ModeSet reports whether Mode was explicitly recorded (create or
	// chmod): a set 0000 must not be rewritten to a readable default.
	ModeSet bool
	IsText  bool
	// Content is set for text files.
	Content []byte
	// Manifest is the CAS manifest hash for blobs.
	Manifest string
	// Materialized reports whether blob content is locally available.
	Materialized bool
	// Size is the authoritative content length (text or blob).
	Size int64
}

// EntryInfo returns the live state of one path.
func (w *WorkspaceState) EntryInfo(path string) (EntryInfo, bool) {
	f := w.files[path]
	if f == nil {
		return EntryInfo{}, false
	}
	return EntryInfo{
		IsDir:        f.IsDir,
		Mode:         f.Mode,
		ModeSet:      f.ModeSet,
		IsText:       !f.IsDir && f.Kind == kindText,
		Content:      f.Content,
		Manifest:     f.Manifest,
		Materialized: f.Materialized,
		Size:         f.Size,
	}, true
}

// BlobManifestOf returns the CAS manifest hash of a blob path.
func BlobManifestOf(w *WorkspaceState, path string) (string, bool) {
	info, ok := w.EntryInfo(path)
	if !ok || info.IsDir || info.IsText {
		return "", false
	}
	return info.Manifest, true
}
