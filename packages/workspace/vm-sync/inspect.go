package vmsync

// EntryInfo is a read-only view of one path's live state, for replicas and
// the mirror-apply engine.
type EntryInfo struct {
	IsDir  bool
	Mode   uint32
	IsText bool
	// Content is set for text files.
	Content []byte
	// Manifest is the CAS manifest hash for blobs.
	Manifest string
	// Materialized reports whether blob content is locally available.
	Materialized bool
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
		IsText:       !f.IsDir && f.Kind == kindText,
		Content:      f.Content,
		Manifest:     f.Manifest,
		Materialized: f.Materialized,
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
