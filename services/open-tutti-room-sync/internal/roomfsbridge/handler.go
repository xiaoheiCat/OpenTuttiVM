// Package roomfsbridge adapts the Room FS Protocol onto the local replica:
// reads come from the replica (lazy CAS fetch), writes convert into Room
// File Operations and submit through the room socket, and local changes
// broadcast invalidations to every connected mount.
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

func (h *Handler) nextOpID() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.opCounter++
	return fmt.Sprintf("%s-%d", h.sessionID, h.opCounter)
}

// Stat implements roomfs.Handler.
func (h *Handler) Stat(path string) (*roomfs.Stat, error) {
	info, ok := h.mgr.Replica.State.EntryInfo(path)
	if !ok {
		return &roomfs.Stat{}, nil
	}
	mode := info.Mode
	if mode == 0 {
		if info.IsDir {
			mode = 0o755
		} else {
			mode = 0o644
		}
	}
	var size int64
	if !info.IsDir {
		// Cheap for materialized text; lazy blobs read on demand.
		if content, err := h.mgr.Read(context.Background(), path); err == nil {
			size = int64(len(content))
		}
	}
	return &roomfs.Stat{Dir: info.IsDir, Size: size, Mode: mode, Exists: true}, nil
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
	type childInfo struct{ dir bool }
	children := map[string]*childInfo{}
	for _, p := range h.mgr.Replica.State.Paths() {
		if !strings.HasPrefix(p, prefix) || len(p) == len(prefix) {
			continue
		}
		rest := p[len(prefix):]
		if idx := strings.IndexByte(rest, '/'); idx >= 0 {
			name := rest[:idx]
			c := children[name]
			if c == nil {
				c = &childInfo{}
				children[name] = c
			}
			c.dir = true
			continue
		}
		if children[rest] == nil {
			children[rest] = &childInfo{}
		}
	}
	out := make([]roomfs.DirEntry, 0, len(children))
	for name, c := range children {
		out = append(out, roomfs.DirEntry{Name: name, Dir: c.dir})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Write implements roomfs.Handler: whole-file write → File Operation.
func (h *Handler) Write(path string, content []byte) error {
	old, err := h.mgr.Read(context.Background(), path)
	if err != nil {
		old = nil
	}
	op, err := vmsync.ConvertChange(h.nextOpID(), path, old, content,
		func(manifest vmcas.Manifest, chunks [][]byte) error {
			if h.uploader == nil {
				return fmt.Errorf("no chunk uploader configured")
			}
			return h.uploader.EnsureChunks(context.Background(), manifest, chunks)
		})
	if err != nil {
		return err
	}
	return h.submit(op)
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

// Remove implements roomfs.Handler.
func (h *Handler) Remove(path string) error {
	return h.submit(vmprotocol.FileOperation{
		ID: h.nextOpID(), Path: path, Kind: vmprotocol.OpRemove,
	})
}

// Rename implements roomfs.Handler.
func (h *Handler) Rename(from, to string) error {
	return h.submit(vmprotocol.FileOperation{
		ID: h.nextOpID(), Path: from, Kind: vmprotocol.OpRename,
		Rename: &vmprotocol.Rename{OldPath: from, NewPath: to},
	})
}

func (h *Handler) submit(op vmprotocol.FileOperation) error {
	env := vmprotocol.Envelope{
		OperationID: op.ID, Operation: op,
		AuthorDeviceID: h.deviceID, AgentSessionID: h.sessionID,
		BaseSeq: h.mgr.Replica.AppliedSeq,
	}
	if err := h.submitter.Submit(env); err != nil {
		return err
	}
	if h.inval != nil {
		h.inval(op.Path)
	}
	return nil
}
