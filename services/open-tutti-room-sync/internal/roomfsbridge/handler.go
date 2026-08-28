// Package roomfsbridge adapts the Room FS Protocol onto the local replica:
// reads come from the replica (lazy CAS fetch), writes convert into Room
// File Operations and submit through the room socket, and mutations report
// success only after the authoritative acknowledgement.
package roomfsbridge

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	vmcas "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-cas"
	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
	vmsync "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-sync"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-fs/roomfs"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-room-sync/internal/replica"
)

// Submitter sends operations to the server (the WS session).
type Submitter interface {
	Submit(env vmprotocol.Envelope) error
}

// ChunkUploader uploads blob chunks before a blob-replace op goes out.
type ChunkUploader interface {
	EnsureChunks(ctx context.Context, manifest vmcas.Manifest, chunks [][]byte) error
}

// Handler implements roomfs.Handler over the replica manager.
type Handler struct {
	mu        sync.Mutex
	mgr       *replica.Manager
	submitter Submitter
	uploader  ChunkUploader
	deviceID  string
	sessionID string
	opCounter int
	inval     func(path string)
}

// New wires the bridge. inval (optional) is invoked with each changed path
// so the host broadcasts invalidations to connected mounts.
func New(mgr *replica.Manager, submitter Submitter, uploader ChunkUploader, deviceID, sessionID string, inval func(path string)) *Handler {
	return &Handler{
		mgr: mgr, submitter: submitter, uploader: uploader,
		deviceID: deviceID, sessionID: sessionID, inval: inval,
	}
}

// InvalidateRemote drops mount caches for a path changed by another
// participant; called after the operation applied locally.
func (h *Handler) InvalidateRemote(path string) {
	if h.inval != nil {
		h.inval(path)
	}
}

func (h *Handler) nextOpID() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.opCounter++
	return fmt.Sprintf("%s-%d", h.sessionID, h.opCounter)
}

// Stat implements roomfs.Handler.
func (h *Handler) Stat(path string) (*roomfs.Stat, error) {
	var stat roomfs.Stat
	h.mgr.WithState(func(state *vmsync.WorkspaceState) {
		info, ok := state.EntryInfo(path)
		if !ok {
			return
		}
		mode := info.Mode
		if mode == 0 {
			if info.IsDir {
				mode = 0o755
			} else {
				mode = 0o644
			}
		}
		stat = roomfs.Stat{Dir: info.IsDir, Mode: mode, Exists: true}
	})
	if !stat.Exists {
		return &roomfs.Stat{}, nil
	}
	if !stat.Dir {
		// Cheap for materialized text; lazy blobs read on demand.
		if content, err := h.mgr.Read(context.Background(), path); err == nil {
			stat.Size = int64(len(content))
		}
	}
	return &stat, nil
}

// Read implements roomfs.Handler.
func (h *Handler) Read(path string) ([]byte, error) {
	return h.mgr.Read(context.Background(), path)
}

// List implements roomfs.Handler.
func (h *Handler) List(path string) ([]roomfs.DirEntry, error) {
	prefix := ""
	if path != "" {
		prefix = path + "/"
	}
	var out []roomfs.DirEntry
	h.mgr.WithState(func(state *vmsync.WorkspaceState) {
		type childInfo struct{ dir bool }
		children := map[string]*childInfo{}
		mark := func(name string, dir bool) {
			c := children[name]
			if c == nil {
				c = &childInfo{}
				children[name] = c
			}
			if dir {
				c.dir = true
			}
		}
		for _, p := range state.Paths() {
			if !strings.HasPrefix(p, prefix) || len(p) == len(prefix) {
				continue
			}
			rest := p[len(prefix):]
			if idx := strings.IndexByte(rest, '/'); idx >= 0 {
				// A deeper path proves the child is a directory even
				// when the directory entry itself is absent.
				mark(rest[:idx], true)
				continue
			}
			if info, ok := state.EntryInfo(p); ok {
				// Direct entry: empty directories carry their type
				// only here — a zero-value would emit S_IFREG.
				mark(rest, info.IsDir)
			} else {
				mark(rest, false)
			}
		}
		out = make([]roomfs.DirEntry, 0, len(children))
		for name, c := range children {
			out = append(out, roomfs.DirEntry{Name: name, Dir: c.dir})
		}
	})
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Write implements roomfs.Handler: whole-file write → File Operation.
func (h *Handler) Write(path string, content []byte) error {
	// Old content, tracked base hash, and base sequence come from ONE
	// locked snapshot: a remote operation landing between separate reads
	// would splice stale offsets onto a fresh revision's hash and seq.
	old, base, baseSeq, _ := h.mgr.PrepareWrite(context.Background(), path)
	op, err := vmsync.ConvertChange(h.nextOpID(), path, base, old, content,
		func(manifest vmcas.Manifest, chunks [][]byte) error {
			if h.uploader == nil {
				return fmt.Errorf("no chunk uploader configured")
			}
			return h.uploader.EnsureChunks(context.Background(), manifest, chunks)
		})
	if err != nil {
		return err
	}
	return h.submitAtSeq(op, baseSeq)
}

// Create implements roomfs.Handler.
func (h *Handler) Create(path string, mode uint32) error {
	return h.submit(vmprotocol.FileOperation{
		ID: h.nextOpID(), Path: path, Kind: vmprotocol.OpCreate,
		Mode: &vmprotocol.MetadataChange{Mode: mode},
	})
}

// Mkdir implements roomfs.Handler.
func (h *Handler) Mkdir(path string, mode uint32) error {
	return h.submit(vmprotocol.FileOperation{
		ID: h.nextOpID(), Path: path, Kind: vmprotocol.OpMkdir, IsDir: true,
		Mode: &vmprotocol.MetadataChange{Mode: mode},
	})
}

// Remove implements roomfs.Handler. Directory intent comes from replica
// state so the authoritative remove matches the target's kind.
func (h *Handler) Remove(path string) error {
	op := vmprotocol.FileOperation{
		ID: h.nextOpID(), Path: path, Kind: vmprotocol.OpRemove,
	}
	h.mgr.WithState(func(state *vmsync.WorkspaceState) {
		if info, ok := state.EntryInfo(path); ok && info.IsDir {
			op.IsDir = true
		}
	})
	return h.submit(op)
}

// Rename implements roomfs.Handler.
func (h *Handler) Rename(from, to string) error {
	return h.submit(vmprotocol.FileOperation{
		ID: h.nextOpID(), Path: from, Kind: vmprotocol.OpRename,
		Rename: &vmprotocol.Rename{OldPath: from, NewPath: to},
	})
}

// submit sends one operation and reports success only after the server
// accepted it (broadcast acknowledgement); rejections surface as errors.
func (h *Handler) submit(op vmprotocol.FileOperation) error {
	return h.submitAtSeq(op, h.mgr.Replica.AppliedSeq)
}

// submitAtSeq is submit with an explicit base sequence (write path: the
// sequence captured atomically with the content the splice offsets were
// derived from).
func (h *Handler) submitAtSeq(op vmprotocol.FileOperation, baseSeq uint64) error {
	env := vmprotocol.Envelope{
		OperationID: op.ID, Operation: op,
		AuthorDeviceID: h.deviceID, AgentSessionID: h.sessionID,
		BaseSeq: baseSeq,
	}
	err := h.mgr.SubmitAndWait(context.Background(), env, func() error {
		return h.submitter.Submit(env)
	})
	if err != nil {
		return err
	}
	if h.inval != nil {
		h.inval(op.Path)
	}
	return nil
}
