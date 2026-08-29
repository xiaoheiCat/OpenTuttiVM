// Package sequencer owns the per-room server-authoritative workspace: it
// applies operations through vmsync, broadcasts accepted operations,
// broadcasts conflict-barrier events, and checkpoints snapshots into CAS.
package sequencer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	vmcas "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-cas"
	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
	vmsync "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-sync"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/config"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/store"
)

// Sender is the event fanout the sequencer needs: room broadcast plus
// targeted delivery to a rejected author.
type Sender interface {
	BroadcastRoom(roomID string, ev vmprotocol.Event)
	SendTo(roomID, deviceID string, ev vmprotocol.Event)
}

// Manager keeps one engine per active room.
type Manager struct {
	repo  store.Repository
	cfg   config.Config
	cas   vmcas.Store
	send  Sender
	log   *slog.Logger
	clock func() int64

	mu      sync.Mutex
	engines map[string]*engine
}

type engine struct {
	state *vmsync.WorkspaceState
	// terminal marks a room whose owner asserted Apply-and-Leave: the
	// sequence check and this flag share the manager mutex, so NO
	// Submit can sequence an edit between the check and the dissolve
	// that destroys the authoritative copy.
	terminal     bool
	opsSinceSnap int
}

// NewManager wires the sequencer.
func NewManager(repo store.Repository, cfg config.Config, cas vmcas.Store, send Sender, log *slog.Logger) *Manager {
	return &Manager{
		repo: repo, cfg: cfg, cas: cas, send: send, log: log,
		clock:   func() int64 { return nowMillis() },
		engines: map[string]*engine{},
	}
}

// Submit runs one operation through the authoritative sequencer and fans
// out the results.
func (m *Manager) Submit(env vmprotocol.Envelope) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Membership revalidation INSIDE the sequencing fence: cancelling a
	// kicked device's socket does not abort its in-flight handler, and
	// the HTTP layer's earlier check races the membership deletion. A
	// revoked device that already read its op frame must not sequence
	// an edit after the deletion committed.
	if _, err := m.repo.GetMembership(context.Background(), env.RoomID, env.AuthorDeviceID); err != nil {
		return fmt.Errorf("device %s is not a member of room %s", env.AuthorDeviceID, env.RoomID)
	}
	// Deduplication identity is REQUIRED, not advisory: an empty
	// OperationID bypasses the dedup block, so a retry after a lost
	// acknowledgement would sequence the same insertion twice instead
	// of returning the original ack — breaking the at-least-once
	// contract and duplicating content.
	if env.OperationID == "" || env.AuthorDeviceID == "" {
		return fmt.Errorf("operation id and author device id are required")
	}

	eng, err := m.engine(env.RoomID)
	if err != nil {
		return err
	}
	if eng.terminal {
		return fmt.Errorf("room %s is finalizing; edits rejected", env.RoomID)
	}
	env.TimestampMS = m.clock()
	// A blob replacement commits a manifest hash into authoritative state:
	// the referenced object graph (manifest + every chunk) must already
	// exist in CAS, or every replica and the final workspace apply would
	// fail permanently on the lookup.
	if env.Operation.Kind == vmprotocol.OpBlobReplace && env.Operation.Blob != nil {
		if err := m.validateBlobGraph(env.RoomID, env.Operation.Blob.Manifest); err != nil {
			m.reject(env, &vmsync.RejectionError{Reason: vmsync.RejectInvalid, CurrentHash: err.Error()})
			return err
		}
		// The declared size must match the verified manifest so state can
		// carry it without re-reading CAS (a mismatched declaration would
		// poison snapshots with size: 0 or worse).
		if data, err := m.cas.Get(env.Operation.Blob.Manifest); err == nil {
			if manifest, err := vmcas.DecodeManifest(data); err == nil && env.Operation.Blob.Size != manifest.Size {
				err := fmt.Errorf("blob size %d != manifest %d", env.Operation.Blob.Size, manifest.Size)
				m.reject(env, &vmsync.RejectionError{Reason: vmsync.RejectInvalid, CurrentHash: err.Error()})
				return err
			}
		}
	}
	accepted, err := eng.state.Accept(env)
	if err != nil {
		m.reject(env, err)
		return err
	}
	eng.opsSinceSnap++

	m.send.BroadcastRoom(env.RoomID, vmprotocol.Event{
		Topic:   vmprotocol.TopicOperation,
		RoomID:  env.RoomID,
		Payload: mustJSON(accepted),
	})
	// Environment files renamed OVER (an editor's atomic save renames a
	// temp file onto .opentuttivm/Dockerfile) must emit the rebuild
	// prompt too: Operation.Path is the temp SOURCE, so checking only
	// it missed the destination other devices keep using stale.
	envPath := vmsync.IsEnvironmentPath(env.Operation.Path)
	if !envPath && env.Operation.Rename != nil {
		envPath = vmsync.IsEnvironmentPath(env.Operation.Rename.NewPath)
	}
	if envPath {
		m.send.BroadcastRoom(env.RoomID, vmprotocol.Event{
			Topic:  vmprotocol.TopicEnvironmentChanged,
			RoomID: env.RoomID,
			Payload: mustJSON(vmprotocol.EnvironmentChangedPayload{
				Revision:     int64(accepted.ServerSeq),
				ChangedFiles: []string{env.Operation.Path},
				ChangedBy:    env.AuthorDeviceID,
			}),
		})
	}
	if eng.opsSinceSnap >= m.cfg.SnapshotIntervalOps {
		if _, err := m.snapshotLocked(env.RoomID, vmprotocol.SnapshotPeriodic); err != nil {
			m.log.Error("periodic snapshot failed", "room", env.RoomID, "err", err)
		}
	}
	return nil
}

func (m *Manager) reject(env vmprotocol.Envelope, err error) {
	var rej *vmsync.RejectionError
	if !errors.As(err, &rej) {
		rej = &vmsync.RejectionError{Reason: "invalid"}
	}
	if rej.Reason == vmsync.RejectSemanticConflict {
		// Barrier just opened: tell the whole room which path locked, who
		// resolves, and who is blocked.
		m.send.BroadcastRoom(env.RoomID, vmprotocol.Event{
			Topic:  vmprotocol.TopicConflictDetected,
			RoomID: env.RoomID,
			Payload: mustJSON(vmprotocol.ConflictPayload{
				Path:           env.Operation.Path,
				ResolverAgent:  rej.ResolverAgent,
				NotifiedAgents: rej.NotifiedAgents,
			}),
		})
	}
	m.send.SendTo(env.RoomID, env.AuthorDeviceID, vmprotocol.Event{
		Topic:  vmprotocol.TopicOperationRejected,
		RoomID: env.RoomID,
		Payload: mustJSON(vmprotocol.RejectionPayload{
			OperationID: env.OperationID,
			Path:        env.Operation.Path,
			Reason:      string(rej.Reason),
			CurrentHash: rej.CurrentHash,
		}),
	})
}

// validateBlobGraph verifies a replacement manifest decodes, is
// materializable (declared size matches the chunk bytes; every chunk
// except the last is exactly ChunkSize), and every object in the graph
// is referenced by the SUBMITTING ROOM before the operation becomes
// authoritative: the process-global CAS may hold the same hash for
// another room, and accepting it would grant this room download access
// through snapshot ref collection, bypassing the per-room authorization
// the CAS endpoints enforce.
func (m *Manager) validateBlobGraph(roomID, manifestHash string) error {
	if manifestHash == "" {
		return errors.New("blob manifest hash required")
	}
	if ok, err := m.repo.HasCASRef(context.Background(), roomID, manifestHash); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("manifest %s not referenced by this room", manifestHash)
	}
	data, err := m.cas.Get(manifestHash)
	if err != nil {
		return fmt.Errorf("manifest %s not in CAS: %w", manifestHash, err)
	}
	manifest, err := vmcas.DecodeManifest(data)
	if err != nil {
		return fmt.Errorf("manifest %s undecodable: %w", manifestHash, err)
	}
	if manifest.Hash != manifestHash {
		return fmt.Errorf("manifest %s self-hash mismatch", manifestHash)
	}
	// A zero-byte file legitimately has no chunks (BuildManifest emits
	// exactly that); any OTHER zero-chunk manifest would declare a
	// positive size and fail the total check below.
	if len(manifest.Chunks) == 0 && manifest.Size != 0 {
		return fmt.Errorf("manifest %s declares %d bytes but has no chunks", manifestHash, manifest.Size)
	}
	// Expansion caps BEFORE any I/O: a hostile manifest repeating one
	// referenced chunk tens of thousands of times would drive a CAS
	// read per occurrence while Submit holds the process-global mutex
	// (hundreds of GiB of I/O stalling every room), and an accepted
	// over-sized logical file would OOM full replicas at materialize.
	if manifest.Size < 0 || manifest.Size > vmcas.MaxBlobFileSize {
		return fmt.Errorf("manifest %s declares %d bytes, over the %d-byte cap", manifestHash, manifest.Size, vmcas.MaxBlobFileSize)
	}
	if len(manifest.Chunks) > vmcas.MaxManifestChunks {
		return fmt.Errorf("manifest %s references %d chunks, over the %d cap", manifestHash, len(manifest.Chunks), vmcas.MaxManifestChunks)
	}
	// Each UNIQUE chunk is validated once: a chunk hash fixes the
	// bytes (content-addressed), so repeated references reuse the
	// cached length — duplicate-heavy manifests cost list-length work,
	// not per-occurrence CAS reads.
	lengths := make(map[string]int, len(manifest.Chunks))
	chunkLen := func(chunk string) (int, error) {
		if n, ok := lengths[chunk]; ok {
			return n, nil
		}
		if ok, err := m.repo.HasCASRef(context.Background(), roomID, chunk); err != nil {
			return 0, err
		} else if !ok {
			return 0, fmt.Errorf("chunk %s of %s not referenced by this room", chunk, manifestHash)
		}
		body, err := m.cas.Get(chunk)
		if err != nil {
			return 0, fmt.Errorf("chunk %s of %s not in CAS: %w", chunk, manifestHash, err)
		}
		lengths[chunk] = len(body)
		return len(body), nil
	}
	var total int64
	for i, chunk := range manifest.Chunks {
		n, err := chunkLen(chunk)
		if err != nil {
			return err
		}
		// Fixed-chunk invariant: every chunk but the last is exactly
		// ChunkSize; a shorter interior chunk means Materialize would
		// yield bytes that never match the declared size.
		if i < len(manifest.Chunks)-1 && n != vmcas.ChunkSize {
			return fmt.Errorf("interior chunk %d of %s is %d bytes, want %d",
				i, manifestHash, n, vmcas.ChunkSize)
		}
		if n > vmcas.ChunkSize {
			return fmt.Errorf("chunk %d of %s is %d bytes, over %d", i, manifestHash, n, vmcas.ChunkSize)
		}
		total += int64(n)
	}
	if total != manifest.Size {
		return fmt.Errorf("manifest %s declares %d bytes but chunks total %d",
			manifestHash, manifest.Size, total)
	}
	return nil
}

// MembershipMutation runs fn while holding the SAME mutex that guards
// operation admission: revocation (kick/leave) and Submit previously
// raced on different locks, letting a socket's already-read frame
// sequence an edit after the membership deletion committed.
func (m *Manager) MembershipMutation(roomID, deviceID string, fn func() error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, err := m.repo.GetMembership(context.Background(), roomID, deviceID); err != nil {
		return fmt.Errorf("device %s is not a member of room %s", deviceID, roomID)
	}
	return fn()
}

// UnfreezeAt restores sequencing after a disband that FAILED before
// committing: the room stays active, and a terminal engine would have
// rejected every later edit.
func (m *Manager) UnfreezeAt(roomID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if eng, ok := m.engines[roomID]; ok {
		eng.terminal = false
	}
}

// ClearBarriersOf lifts barriers still assigned to an evicted member
// (kick/leave): the barrier would otherwise block the path for every
// remaining participant until room dissolution, with its resolver
// unable to reconnect. Broadcasts a barrier-cleared event per path.
func (m *Manager) ClearBarriersOf(roomID, deviceID string) {
	m.mu.Lock()
	eng, ok := m.engines[roomID]
	if !ok {
		m.mu.Unlock()
		return
	}
	cleared := eng.state.ClearBarriersOf(deviceID)
	m.mu.Unlock()
	for _, path := range cleared {
		m.send.BroadcastRoom(roomID, vmprotocol.Event{
			Topic:  vmprotocol.TopicConflictResolved,
			RoomID: roomID,
			Payload: mustJSON(vmprotocol.ConflictPayload{
				Path:          path,
				ResolverAgent: "",
			}),
		})
	}
}

// ResolveConflict lifts a barrier after the resolver committed the fix and
// notifies every blocked agent with the resolved revision. The connection
// identity must match the barrier's assigned resolver: a blocked
// participant cannot lift the server-enforced fence.
func (m *Manager) ResolveConflict(roomID, path, deviceID, agentSessionID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	eng, err := m.engine(roomID)
	if err != nil {
		return err
	}
	if !eng.state.BarrierResolverMatches(path, deviceID, agentSessionID) {
		return errors.New("only the assigned resolver may resolve this barrier")
	}
	// The resolver's own patches already sequenced; lift the barrier.
	notified, ok := eng.state.ResolveBarrier(path)
	if !ok {
		return errors.New("no open barrier for path")
	}
	m.send.BroadcastRoom(roomID, vmprotocol.Event{
		Topic:  vmprotocol.TopicConflictResolved,
		RoomID: roomID,
		Payload: mustJSON(vmprotocol.ConflictPayload{
			Path:             path,
			ResolvedRevision: eng.state.Seq(),
			NotifiedAgents:   notified,
		}),
	})
	return nil
}

// Bootstrap returns a stable checkpoint plus the operations after it for a
// joining or reconnecting device.
func (m *Manager) Bootstrap(ctx context.Context, roomID string) (vmprotocol.WorkspaceSnapshot, []vmprotocol.Envelope, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	snap, err := m.snapshotLocked(roomID, vmprotocol.SnapshotBootstrap)
	if err != nil {
		return snap, nil, err
	}
	eng := m.engines[roomID]
	return snap, eng.state.OpsSince(snap.ServerSeq), nil
}

// FreezeAt is the owner Apply-and-Leave fence: it validates the
// asserted apply sequence and freezes the engine against further
// submissions ATOMICALLY (under the manager mutex). A plain read-then-
// compare let a concurrent Submit sequence an edit after the check but
// before the dissolve destroyed the authoritative copy.
func (m *Manager) FreezeAt(roomID string, baseSeq uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	eng, ok := m.engines[roomID]
	if !ok {
		return nil // room already torn down; nothing left to fence
	}
	if eng.terminal {
		return nil // idempotent re-freeze of the same leave
	}
	if eng.state.Seq() != baseSeq {
		return fmt.Errorf("workspace changed since apply (have seq %d, leave asserted %d)", eng.state.Seq(), baseSeq)
	}
	eng.terminal = true
	return nil
}

// SnapshotForTransfer checkpoints before an ownership transfer commits so
// the incoming owner can complete a full replica.
func (m *Manager) SnapshotForTransfer(ctx context.Context, roomID string) (vmprotocol.WorkspaceSnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snapshotLocked(roomID, vmprotocol.SnapshotOwnerTransfer)
}

func (m *Manager) snapshotLocked(roomID string, reason vmprotocol.SnapshotReason) (vmprotocol.WorkspaceSnapshot, error) {
	eng, err := m.engine(roomID)
	if err != nil {
		return vmprotocol.WorkspaceSnapshot{}, err
	}
	// The ENTIRE publication — CAS object writes, snapshot record,
	// liveness validation, and reference insertion — shares the
	// publication fence: another room's collector can delete an
	// identical content hash it observes unreferenced, so writing the
	// objects before taking the fence could persist a snapshot whose
	// objects are deleted before its references exist.
	var snap vmprotocol.WorkspaceSnapshot
	if err := m.repo.CASPublication(func() error {
		var err error
		snap, err = eng.state.Snapshot(roomID, reason, m.cas)
		if err != nil {
			return err
		}
		// Dissolution fence, INSIDE the publication lock: the engine()
		// liveness read earlier can race a dissolve that deletes all
		// room references; publishing snapshot refs for an already
		// dissolved room would either point at deleted objects or pin
		// them forever.
		if room, err := m.repo.GetRoom(context.Background(), roomID); err != nil || room.DissolvedAt != nil {
			return fmt.Errorf("room dissolved before snapshot publication")
		}
		entries, err := json.Marshal(snap.Entries)
		if err != nil {
			return err
		}
		if err := m.repo.SaveSnapshot(context.Background(), store.SnapshotRecord{
			RoomID: roomID, ServerSeq: snap.ServerSeq, RootTreeHash: snap.RootTreeHash,
			EntriesJSON: entries, Reason: string(reason),
		}); err != nil {
			return err
		}
		return m.repo.AddCASRefs(context.Background(), roomID, m.snapshotRefs(snap))
	}); err != nil {
		return snap, err
	}
	eng.opsSinceSnap = 0
	// Payloads at or below the snapshot sequence are folded into the
	// persisted snapshot; keeping them in memory let a room retain
	// every ~8 MiB body forever (a few hundred edits exhaust the
	// process for EVERY room).
	eng.state.Checkpoint(snap.ServerSeq)
	m.send.BroadcastRoom(roomID, vmprotocol.Event{
		Topic:   vmprotocol.TopicSnapshotAnnounce,
		RoomID:  roomID,
		Payload: mustJSON(snap),
	})
	return snap, nil
}

// snapshotRefs collects every object hash a snapshot makes downloadable:
// each entry's manifest plus the manifest's complete chunk graph — text
// snapshots write chunk objects too, and CAS reads authorize per room
// reference, so missing chunk refs would 404 mid-bootstrap.
func (m *Manager) snapshotRefs(snap vmprotocol.WorkspaceSnapshot) []string {
	out := make([]string, 0, len(snap.Entries)*2)
	for _, e := range snap.Entries {
		if e.Manifest == "" {
			continue
		}
		out = append(out, e.Manifest)
		if data, err := m.cas.Get(e.Manifest); err == nil {
			if manifest, err := vmcas.DecodeManifest(data); err == nil {
				out = append(out, manifest.Chunks...)
			}
		}
	}
	return out
}

func (m *Manager) engine(roomID string) (*engine, error) {
	// Re-read the room on EVERY acquisition, cached engine included:
	// a socket submission acquiring the sequencer lock after
	// DissolveRoom commits but before the leave/grace path calls
	// CloseRoom would otherwise be accepted and broadcast after the
	// terminal room-ending event — a client could observe a successful
	// edit that teardown then discards (and a snapshot could recreate
	// CAS references).
	room, err := m.repo.GetRoom(context.Background(), roomID)
	if err != nil {
		return nil, fmt.Errorf("room %s: %w", roomID, err)
	}
	// A dissolved room is terminal: a stale socket must not recreate an
	// empty engine and sequence post-ending operations.
	if room.DissolvedAt != nil {
		return nil, fmt.Errorf("room %s is dissolved", roomID)
	}
	if eng, ok := m.engines[roomID]; ok {
		return eng, nil
	}
	eng := &engine{state: vmsync.NewWorkspaceState()}
	m.engines[roomID] = eng
	return eng, nil
}

// CloseRoom drops the in-memory engine when a room dissolves.
func (m *Manager) CloseRoom(roomID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.engines, roomID)
}

// State exposes the live engine state for tunnel/preview surfaces.
func (m *Manager) State(roomID string) (*vmsync.WorkspaceState, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	eng, ok := m.engines[roomID]
	if !ok {
		return nil, false
	}
	return eng.state, true
}

func mustJSON(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

func nowMillis() int64 { return time.Now().UnixMilli() }
