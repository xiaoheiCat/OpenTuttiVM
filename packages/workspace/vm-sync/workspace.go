package vmsync

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"

	vmcas "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-cas"
	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
)

// RejectionReason categorizes why the sequencer fenced an operation.
type RejectionReason string

const (
	// RejectConflictBarrier: the path is locked by the conflict barrier and
	// the author is not the assigned resolver.
	RejectConflictBarrier RejectionReason = "conflict_barrier"
	// RejectSemanticConflict: concurrent overlapping edits; the path enters
	// the barrier and this operation is refused.
	RejectSemanticConflict RejectionReason = "semantic_conflict"
	// RejectBaseMismatch: optimistic version check failed; the author must
	// re-derive against the returned current hash.
	RejectBaseMismatch RejectionReason = "base_mismatch"
	// RejectInvalid: the operation does not apply to the current tree.
	RejectInvalid RejectionReason = "invalid"
)

// RejectionError carries the reason plus the state the author needs to
// recover (current content hash for base-mismatch re-uploads, resolver and
// notified agents for barrier events).
type RejectionError struct {
	Reason         RejectionReason
	CurrentHash    string
	ResolverAgent  string
	NotifiedAgents []string
}

func (e *RejectionError) Error() string {
	return fmt.Sprintf("operation rejected: %s", e.Reason)
}

// fileKind distinguishes OT-tracked text files from CAS-backed blobs.
type fileKind string

const (
	kindText fileKind = "text"
	kindBlob fileKind = "blob"
)

type fileState struct {
	IsDir bool
	Mode  uint32
	Kind  fileKind
	// Content is authoritative for text files.
	Content []byte
	// Manifest is the current CAS manifest hash for blob files.
	Manifest string
	// Size is the blob size when known.
	Size int64
	// Materialized reports whether blob content is locally available.
	Materialized bool
}

// seqHash remembers the content hash of one path as of one server sequence,
// validating the base hash of patches authored against older revisions.
type seqHash struct {
	seq  uint64
	hash string
}

// barrier is the per-path conflict lock.
type barrier struct {
	ResolverDevice string
	Locked         bool
	ResolverAgent  string
	Notified       []string
	Revision       uint64
}

// EnvironmentPaths are the environment definition files whose changes must
// broadcast room.environment.changed; devices surface "Environment changed —
// Rebuild" and rebuild locally on explicit user action only.
var EnvironmentPaths = map[string]bool{
	".opentuttivm/Dockerfile":         true,
	".devcontainer/devcontainer.json": true,
}

// IsEnvironmentPath reports whether a path is an environment definition.
func IsEnvironmentPath(path string) bool { return EnvironmentPaths[path] }

// WorkspaceState is the server-authoritative in-memory workspace of one
// room. It owns the sequence counter, applies operations with
// transformation, fences conflict barriers, and materializes snapshots into
// CAS. Per the no-recovery failure model it is memory-only; snapshots are
// the persistence layer.
type WorkspaceState struct {
	seq      uint64
	files    map[string]*fileState
	history  map[string][]appliedPatch
	hashLog  map[string][]seqHash
	barriers map[string]*barrier
	ops      []vmprotocol.Envelope
	// Materializer, when set, fetches CAS-referenced content for a path
	// into memory (text restore, eager full-replica blob fetch). The
	// replica owner wires it; the server leaves it nil.
	Materializer func(path string) error
}

// NewWorkspaceState returns an empty workspace at sequence 0.
func NewWorkspaceState() *WorkspaceState {
	return &WorkspaceState{
		files:    map[string]*fileState{},
		history:  map[string][]appliedPatch{},
		hashLog:  map[string][]seqHash{},
		barriers: map[string]*barrier{},
	}
}

// Seq returns the current server sequence.
func (w *WorkspaceState) Seq() uint64 { return w.seq }

// BarrierResolverMatches reports whether the connection identity is the
// assigned resolver for an open barrier. Used to authorize explicit
// conflict_resolved submissions.
func (w *WorkspaceState) BarrierResolverMatches(path, deviceID, agentSessionID string) bool {
	b := w.barriers[path]
	if b == nil || !b.Locked {
		return false
	}
	return agentSessionID == b.ResolverAgent &&
		(b.ResolverDevice == "" || deviceID == b.ResolverDevice)
}

// fenced reports whether the operation is blocked by an open barrier on any
// path it touches. The assigned resolver passes.
func (w *WorkspaceState) fenced(env *vmprotocol.Envelope) bool {
	op := env.Operation
	paths := []string{op.Path}
	if op.Kind == vmprotocol.OpRename && op.Rename != nil {
		paths = append(paths, op.Rename.OldPath, op.Rename.NewPath)
	}
	for _, p := range paths {
		if p == "" {
			continue
		}
		b := w.barriers[p]
		if b == nil || !b.Locked {
			continue
		}
		// The resolver is identified by BOTH its agent session and its
		// device: session ids are client-claimed, device identity is
		// stamped server-side, so only the real resolver device passes.
		if env.AgentSessionID != b.ResolverAgent || (b.ResolverDevice != "" && env.AuthorDeviceID != b.ResolverDevice) {
			return true
		}
	}
	return false
}

// Accept validates, transforms, and applies one submitted operation,
// assigning it the next server sequence. On success it returns the envelope
// to broadcast. On conflict it may open a barrier and returns a
// *RejectionError.
func (w *WorkspaceState) Accept(env vmprotocol.Envelope) (vmprotocol.Envelope, error) {
	if !vmprotocol.ValidWorkspacePath(env.Operation.Path) {
		return env, &RejectionError{Reason: RejectInvalid}
	}
	if op := env.Operation; op.Kind == vmprotocol.OpRename && op.Rename != nil {
		if !vmprotocol.ValidWorkspacePath(op.Rename.OldPath) || !vmprotocol.ValidWorkspacePath(op.Rename.NewPath) {
			return env, &RejectionError{Reason: RejectInvalid}
		}
	}
	// Kind-specific payloads must be present before dispatch: a nil deref
	// here would crash the authoritative sequencer instead of rejecting
	// one malformed operation.
	switch {
	case env.Operation.Kind == vmprotocol.OpTextPatch && env.Operation.Patch == nil:
		return env, &RejectionError{Reason: RejectInvalid}
	case env.Operation.Kind == vmprotocol.OpBlobReplace && env.Operation.Blob == nil:
		return env, &RejectionError{Reason: RejectInvalid}
	case env.Operation.Kind == vmprotocol.OpRename && env.Operation.Rename == nil:
		return env, &RejectionError{Reason: RejectInvalid}
	}
	if w.fenced(&env) {
		b := w.barriers[env.Operation.Path]
		return env, &RejectionError{
			Reason:         RejectConflictBarrier,
			ResolverAgent:  b.ResolverAgent,
			NotifiedAgents: b.Notified,
		}
	}

	next := env
	var err error
	switch env.Operation.Kind {
	case vmprotocol.OpCreate:
		err = w.applyCreate(env.Operation)
	case vmprotocol.OpRemove:
		err = w.applyRemove(env.Operation)
	case vmprotocol.OpMkdir:
		err = w.applyMkdir(env.Operation)
	case vmprotocol.OpRmdir:
		err = w.applyRmdir(env.Operation)
	case vmprotocol.OpRename:
		err = w.applyRename(env.Operation)
	case vmprotocol.OpMetadataChange:
		err = w.applyMetadata(env.Operation)
	case vmprotocol.OpTextPatch:
		err = w.applyTextPatch(&next)
	case vmprotocol.OpBlobReplace:
		err = w.applyBlobReplace(env.Operation)
	default:
		err = &RejectionError{Reason: RejectInvalid}
	}
	if err != nil {
		return env, asRejection(err)
	}

	w.seq++
	next.ServerSeq = w.seq
	w.record(&next)
	return next, nil
}

func asRejection(err error) error {
	var r *RejectionError
	if errors.As(err, &r) {
		return r
	}
	return &RejectionError{Reason: RejectInvalid, CurrentHash: err.Error()}
}

func (w *WorkspaceState) record(env *vmprotocol.Envelope) {
	w.ops = append(w.ops, *env)
	op := env.Operation
	w.recordHistory(env)
	switch {
	case op.Kind == vmprotocol.OpRename && op.Rename != nil:
		w.pushHash(op.Rename.OldPath, env.ServerSeq, "")
		w.pushHash(op.Rename.NewPath, env.ServerSeq, w.currentHash(op.Rename.NewPath))
	case op.Kind == vmprotocol.OpRemove || op.Kind == vmprotocol.OpRmdir:
		w.pushHash(op.Path, env.ServerSeq, "")
	default:
		w.pushHash(op.Path, env.ServerSeq, w.currentHash(op.Path))
	}
}

func (w *WorkspaceState) currentHash(path string) string {
	f := w.files[path]
	if f == nil || f.IsDir {
		return ""
	}
	if f.Kind == kindBlob {
		return f.Manifest
	}
	return ContentHash(f.Content)
}

func (w *WorkspaceState) pushHash(path string, seq uint64, hash string) {
	w.hashLog[path] = append(w.hashLog[path], seqHash{seq: seq, hash: hash})
	const perPathLog = 256
	if n := len(w.hashLog[path]); n > perPathLog {
		w.hashLog[path] = w.hashLog[path][n-perPathLog:]
	}
}

// concurrentPatches returns sequenced text patches on path after baseSeq,
// expressed in the frames transform needs.
func (w *WorkspaceState) concurrentPatches(path string, baseSeq uint64) []appliedPatch {
	var out []appliedPatch
	for _, p := range w.history[path] {
		if p.Seq > baseSeq {
			out = append(out, p)
		}
	}
	return out
}

func (w *WorkspaceState) applyTextPatch(env *vmprotocol.Envelope) error {
	op := env.Operation
	f := w.files[op.Path]
	if f == nil || f.IsDir || f.Kind != kindText {
		return &RejectionError{Reason: RejectInvalid}
	}
	patch := *op.Patch

	// Guard: the author's base hash must match the path content as of their
	// base sequence; this detects stale or duplicated submissions.
	if patch.BaseHash != "" {
		if base := w.hashAt(op.Path, env.BaseSeq); base != "" && base != patch.BaseHash {
			return &RejectionError{Reason: RejectBaseMismatch, CurrentHash: w.currentHash(op.Path)}
		}
	}
	// A base older than the retained transform window cannot be safely
	// transformed: the dropped patches would skew every offset.
	if hist := w.history[op.Path]; len(hist) == transformHistoryWindow && env.BaseSeq < hist[0].Seq {
		return &RejectionError{Reason: RejectBaseMismatch, CurrentHash: w.currentHash(op.Path)}
	}

	concurrent := w.concurrentPatches(op.Path, env.BaseSeq)
	if res := TransformPatch(&patch, concurrent); res.Conflict {
		return w.openBarrier(op.Path, env, res.ConflictWith)
	}
	if err := ValidateSplices(patch.Splices, len(f.Content)); err != nil {
		return &RejectionError{Reason: RejectInvalid}
	}
	next, err := ApplyPatch(f.Content, patch)
	if err != nil {
		return &RejectionError{Reason: RejectInvalid}
	}
	if len(next) > MaxTextFile || !utf8.Valid(next) {
		return &RejectionError{Reason: RejectInvalid}
	}
	f.Content = next
	env.Operation.Patch = &patch
	return nil
}

func (w *WorkspaceState) applyBlobReplace(op vmprotocol.FileOperation) error {
	f := w.files[op.Path]
	if f == nil || f.IsDir {
		return &RejectionError{Reason: RejectInvalid}
	}
	if op.Blob.BaseHash != w.currentHash(op.Path) {
		return &RejectionError{Reason: RejectBaseMismatch, CurrentHash: w.currentHash(op.Path)}
	}
	f.Kind = kindBlob
	f.Manifest = op.Blob.Manifest
	f.Content = nil
	f.Size = 0
	f.Materialized = false
	return nil
}

func (w *WorkspaceState) applyCreate(op vmprotocol.FileOperation) error {
	if _, exists := w.files[op.Path]; exists {
		return &RejectionError{Reason: RejectInvalid}
	}
	var mode uint32 = 0o644
	if op.Mode != nil {
		mode = op.Mode.Mode
	}
	w.files[op.Path] = &fileState{Mode: mode, Kind: kindText, Content: []byte{}}
	return nil
}

func (w *WorkspaceState) applyRemove(op vmprotocol.FileOperation) error {
	f, exists := w.files[op.Path]
	if !exists {
		return &RejectionError{Reason: RejectInvalid}
	}
	if f.IsDir != op.IsDir {
		return &RejectionError{Reason: RejectInvalid}
	}
	delete(w.files, op.Path)
	return nil
}

func (w *WorkspaceState) applyMkdir(op vmprotocol.FileOperation) error {
	if _, exists := w.files[op.Path]; exists {
		return &RejectionError{Reason: RejectInvalid}
	}
	w.files[op.Path] = &fileState{IsDir: true, Mode: 0o755}
	return nil
}

func (w *WorkspaceState) applyRmdir(op vmprotocol.FileOperation) error {
	f, exists := w.files[op.Path]
	if !exists || !f.IsDir {
		return &RejectionError{Reason: RejectInvalid}
	}
	for p := range w.files {
		if strings.HasPrefix(p, op.Path+"/") {
			return &RejectionError{Reason: RejectInvalid}
		}
	}
	delete(w.files, op.Path)
	return nil
}

func (w *WorkspaceState) applyRename(op vmprotocol.FileOperation) error {
	r := op.Rename
	if r == nil {
		return &RejectionError{Reason: RejectInvalid}
	}
	f, exists := w.files[r.OldPath]
	if !exists {
		return &RejectionError{Reason: RejectInvalid}
	}
	if _, exists := w.files[r.NewPath]; exists {
		return &RejectionError{Reason: RejectInvalid}
	}
	// Renaming a directory into its own subtree ("a" → "a/b") would let
	// the freshly inserted destination match the source prefix again and
	// double-move every entry; reject before any mutation.
	if r.NewPath == r.OldPath || strings.HasPrefix(r.NewPath, r.OldPath+"/") {
		return &RejectionError{Reason: RejectInvalid}
	}
	w.files[r.NewPath] = f
	delete(w.files, r.OldPath)
	if f.IsDir {
		// A nonempty directory rename moves every descendant with it;
		// leaving them under the old prefix corrupts snapshots and
		// Apply-to-Workspace output.
		prefix := r.OldPath + "/"
		descendants := make([]string, 0, 8)
		for p := range w.files {
			if strings.HasPrefix(p, prefix) {
				descendants = append(descendants, p)
			}
		}
		sort.Strings(descendants) // deterministic replay across replicas
		for _, p := range descendants {
			moved := r.NewPath + p[len(r.OldPath):]
			w.files[moved] = w.files[p]
			delete(w.files, p)
			w.rekeyHistory(p, moved)
		}
		if len(descendants) > 0 {
			w.rekeyHistory(r.OldPath, r.NewPath)
		}
	}
	return nil
}

// rekeyHistory moves per-path transform/hash history after a rename so
// later patches against the new path keep their OT context.
func (w *WorkspaceState) rekeyHistory(oldPath, newPath string) {
	if h, ok := w.history[oldPath]; ok {
		w.history[newPath] = h
		delete(w.history, oldPath)
	}
	if hl, ok := w.hashLog[oldPath]; ok {
		w.hashLog[newPath] = hl
		delete(w.hashLog, oldPath)
	}
}

func (w *WorkspaceState) applyMetadata(op vmprotocol.FileOperation) error {
	f, exists := w.files[op.Path]
	if !exists || op.Mode == nil {
		return &RejectionError{Reason: RejectInvalid}
	}
	f.Mode = op.Mode.Mode
	return nil
}

// openBarrier locks a path after a semantic conflict. The agent that most
// recently completed a patch on the path becomes the resolver; notified
// agents are everyone with recent history on the path plus the colliding
// authors, so all affected agents hear conflict_detected and later
// conflict_resolved.
func (w *WorkspaceState) openBarrier(path string, env *vmprotocol.Envelope, conflictedWith []string) error {
	hist := w.history[path]
	resolver := ""
	resolverDevice := env.AuthorDeviceID
	if n := len(hist); n > 0 {
		resolver = hist[n-1].Agent
		if hist[n-1].Device != "" {
			resolverDevice = hist[n-1].Device
		}
	}
	notified := append([]string{}, conflictedWith...)
	// The rejected submitter is an affected party: their edit collided.
	if env.AgentSessionID != "" {
		notified = appendUnique(notified, env.AgentSessionID)
	}
	for _, p := range hist {
		if p.Agent != "" {
			notified = appendUnique(notified, p.Agent)
		}
	}
	w.barriers[path] = &barrier{
		Locked:         true,
		ResolverAgent:  resolver,
		ResolverDevice: resolverDevice,
		Notified:       notified,
		Revision:       w.seq,
	}
	return &RejectionError{
		Reason:         RejectSemanticConflict,
		ResolverAgent:  resolver,
		NotifiedAgents: notified,
	}
}

// ResolveBarrier lifts the barrier after the resolver committed a fixed
// revision. Returns the notified agents so the caller broadcasts
// conflict_resolved with the resolved revision.
func (w *WorkspaceState) ResolveBarrier(path string) (notified []string, ok bool) {
	b := w.barriers[path]
	if b == nil || !b.Locked {
		return nil, false
	}
	notified = b.Notified
	delete(w.barriers, path)
	return notified, true
}

// BarrierInfo exposes barrier state for status and events.
func (w *WorkspaceState) BarrierInfo(path string) (resolver string, locked bool, notified []string) {
	b := w.barriers[path]
	if b == nil {
		return "", false, nil
	}
	return b.ResolverAgent, b.Locked, b.Notified
}

// hashAt returns the recorded content hash of path as of seq (the latest
// entry with seq' <= seq), or "" when unknown.
func (w *WorkspaceState) hashAt(path string, seq uint64) string {
	result := ""
	for _, e := range w.hashLog[path] {
		if e.seq <= seq {
			result = e.hash
		} else {
			break
		}
	}
	return result
}

func (w *WorkspaceState) recordHistory(env *vmprotocol.Envelope) {
	op := env.Operation
	if op.Kind != vmprotocol.OpTextPatch || op.Patch == nil {
		return
	}
	w.history[op.Path] = append(w.history[op.Path], appliedPatch{
		Seq:    env.ServerSeq,
		Agent:  env.AgentSessionID,
		Device: env.AuthorDeviceID,
		Patch:  *op.Patch,
	})
	if n := len(w.history[op.Path]); n > transformHistoryWindow {
		w.history[op.Path] = w.history[op.Path][n-transformHistoryWindow:]
	}
}

// transformHistoryWindow bounds the retained per-path patch history. A
// submission whose base predates the retained window cannot be safely
// transformed (earlier edits would be missing from its offsets), so it is
// rejected instead of mis-applied.
const transformHistoryWindow = 128

// materializeViaHook pulls CAS-referenced content for one path through the
// configured Materializer hook; without a hook the caller must resync.
func (w *WorkspaceState) materializeViaHook(path string) error {
	if w.Materializer == nil {
		return fmt.Errorf("no materializer configured for %s", path)
	}
	return w.Materializer(path)
}

// Snapshot materializes the current state into CAS and returns the snapshot.
// Text file contents are chunked like any other object so bootstrap, export,
// and owner transfer all read from one object layer.
func (w *WorkspaceState) Snapshot(roomID string, reason vmprotocol.SnapshotReason, store vmcas.Store) (vmprotocol.WorkspaceSnapshot, error) {
	snap := vmprotocol.WorkspaceSnapshot{
		RoomID:    roomID,
		ServerSeq: w.seq,
		Reason:    reason,
	}
	paths := w.Paths()

	var treeSrc strings.Builder
	for _, p := range paths {
		f := w.files[p]
		entry := vmprotocol.TreeEntry{Path: p, Mode: f.Mode}
		if f.IsDir {
			entry.Kind = vmprotocol.TreeEntryDir
			treeSrc.WriteString(fmt.Sprintf("%s\t%s\t\t\t%#o\n", p, entry.Kind, entry.Mode))
			snap.Entries = append(snap.Entries, entry)
			continue
		}
		if f.Kind == kindText {
			m, chunks, err := vmcas.BuildManifest(strings.NewReader(string(f.Content)))
			if err != nil {
				return snap, err
			}
			for i, c := range chunks {
				if err := store.Put(m.Chunks[i], c); err != nil {
					return snap, err
				}
			}
			// The manifest object is its hash-covered body, so stored bytes
			// self-verify like any other chunk.
			if err := store.Put(m.Hash, m.Body()); err != nil {
				return snap, err
			}
			entry.Manifest = m.Hash
			entry.Size = m.Size
			entry.Kind = vmprotocol.TreeEntryText
		} else {
			entry.Manifest = f.Manifest
			entry.Size = f.Size
		}
		if entry.Kind == "" {
			entry.Kind = vmprotocol.TreeEntryFile
		}
		treeSrc.WriteString(fmt.Sprintf("%s\t%s\t%s\t%d\t%#o\n", p, entry.Kind, entry.Manifest, entry.Size, entry.Mode))
		snap.Entries = append(snap.Entries, entry)
	}
	sum := sha256.Sum256([]byte(treeSrc.String()))
	snap.RootTreeHash = "sha256:" + hex.EncodeToString(sum[:])
	return snap, nil
}

// RestoreSnapshot loads a snapshot as the bootstrap state, rewinding the
// sequence to the snapshot's. Entries carry manifest hashes directly, so
// lazy replicas load the tree without CAS reads; callers materialize
// content from CAS according to their policy.
func (w *WorkspaceState) RestoreSnapshot(snap vmprotocol.WorkspaceSnapshot) error {
	w.seq = snap.ServerSeq
	w.files = map[string]*fileState{}
	for _, e := range snap.Entries {
		if !vmprotocol.ValidWorkspacePath(e.Path) {
			return fmt.Errorf("snapshot entry %q: invalid workspace path", e.Path)
		}
		if e.Kind == vmprotocol.TreeEntryDir {
			w.files[e.Path] = &fileState{IsDir: true, Mode: e.Mode}
			continue
		}
		if e.Kind == vmprotocol.TreeEntryText {
			// Keep OT tracking: later text patches stay applicable after
			// bootstrap. Content materializes from CAS on demand.
			w.files[e.Path] = &fileState{Mode: e.Mode, Kind: kindText, Manifest: e.Manifest, Size: e.Size}
			continue
		}
		w.files[e.Path] = &fileState{Mode: e.Mode, Kind: kindBlob, Manifest: e.Manifest, Size: e.Size}
	}
	return nil
}

// MaterializeTexts loads CAS-backed content for every restored text entry
// whose content is not yet in memory. The server engine calls this after
// RestoreSnapshot so replayed text patches apply against real content;
// lazy replicas call it per path on demand.
func (w *WorkspaceState) MaterializeTexts(store vmcas.Store) error {
	for _, p := range w.Paths() {
		f := w.files[p]
		if f == nil || f.IsDir || f.Kind != kindText || f.Content != nil || f.Manifest == "" {
			continue
		}
		if err := w.materializeText(p, f, store); err != nil {
			return err
		}
	}
	return nil
}

func (w *WorkspaceState) materializeText(path string, f *fileState, store vmcas.Store) error {
	data, err := store.Get(f.Manifest)
	if err != nil {
		return fmt.Errorf("manifest %s for %s: %w", f.Manifest, path, err)
	}
	m, err := vmcas.DecodeManifest(data)
	if err != nil {
		return fmt.Errorf("manifest %s for %s: %w", f.Manifest, path, err)
	}
	content, err := vmcas.Materialize(store, m)
	if err != nil {
		return fmt.Errorf("content for %s: %w", path, err)
	}
	if content == nil {
		// Empty files must hold an empty (non-nil) slice so materialized
		// checks distinguish them from unrestored manifest references.
		content = []byte{}
	}
	f.Content = content
	return nil
}

// CurrentContent returns live text content for one path; false for blobs or
// missing paths. Used by room-sync lazy reads.
func (w *WorkspaceState) CurrentContent(path string) ([]byte, bool) {
	f := w.files[path]
	if f == nil || f.IsDir || f.Kind != kindText || f.Content == nil {
		return nil, false
	}
	return f.Content, true
}

// Paths lists every tracked path in sorted order.
func (w *WorkspaceState) Paths() []string {
	out := make([]string, 0, len(w.files))
	for p := range w.files {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

// TextPaths lists the OT-tracked text file paths.
func (w *WorkspaceState) TextPaths() []string {
	var out []string
	for p, f := range w.files {
		if !f.IsDir && f.Kind == kindText {
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out
}

// IsBarriered reports whether a path is currently conflict-locked.
func (w *WorkspaceState) IsBarriered(path string) bool {
	b := w.barriers[path]
	return b != nil && b.Locked
}

// OpsSince returns accepted envelopes after seq, for clients resuming from a
// checkpoint.
func (w *WorkspaceState) OpsSince(seq uint64) []vmprotocol.Envelope {
	var out []vmprotocol.Envelope
	for _, env := range w.ops {
		if env.ServerSeq > seq {
			out = append(out, env)
		}
	}
	return out
}
