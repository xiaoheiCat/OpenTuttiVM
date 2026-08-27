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
	state        *vmsync.WorkspaceState
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

	eng, err := m.engine(env.RoomID)
	if err != nil {
		return err
	}
	env.TimestampMS = m.clock()
	// A blob replacement commits a manifest hash into authoritative state:
	// the referenced object graph (manifest + every chunk) must already
	// exist in CAS, or every replica and the final workspace apply would
	// fail permanently on the lookup.
	if env.Operation.Kind == vmprotocol.OpBlobReplace && env.Operation.Blob != nil {
		if err := m.validateBlobGraph(env.Operation.Blob.Manifest); err != nil {
			m.reject(env, &vmsync.RejectionError{Reason: vmsync.RejectInvalid, CurrentHash: err.Error()})
			return err
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
	if vmsync.IsEnvironmentPath(env.Operation.Path) {
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

// validateBlobGraph verifies a replacement manifest decodes and every
// referenced chunk exists in the room's CAS before the operation becomes
// authoritative.
func (m *Manager) validateBlobGraph(manifestHash string) error {
	if manifestHash == "" {
		return errors.New("blob manifest hash required")
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
	for _, chunk := range manifest.Chunks {
		if _, err := m.cas.Get(chunk); err != nil {
			return fmt.Errorf("chunk %s of %s not in CAS: %w", chunk, manifestHash, err)
		}
	}
	return nil
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
	snap, err := eng.state.Snapshot(roomID, reason, m.cas)
	if err != nil {
		return snap, err
	}
	eng.opsSinceSnap = 0
	entries, err := json.Marshal(snap.Entries)
	if err != nil {
		return snap, err
	}
	if err := m.repo.SaveSnapshot(context.Background(), store.SnapshotRecord{
		RoomID: roomID, ServerSeq: snap.ServerSeq, RootTreeHash: snap.RootTreeHash,
		EntriesJSON: entries, Reason: string(reason),
	}); err != nil {
		return snap, err
	}
	if err := m.repo.AddCASRefs(context.Background(), roomID, snapshotHashes(snap)); err != nil {
		return snap, err
	}
	m.send.BroadcastRoom(roomID, vmprotocol.Event{
		Topic:   vmprotocol.TopicSnapshotAnnounce,
		RoomID:  roomID,
		Payload: mustJSON(snap),
	})
	return snap, nil
}

func snapshotHashes(snap vmprotocol.WorkspaceSnapshot) []string {
	out := make([]string, 0, len(snap.Entries))
	for _, e := range snap.Entries {
		if e.Manifest != "" {
			out = append(out, e.Manifest)
		}
	}
	return out
}

func (m *Manager) engine(roomID string) (*engine, error) {
	eng, ok := m.engines[roomID]
	if !ok {
		if _, err := m.repo.GetRoom(context.Background(), roomID); err != nil {
			return nil, fmt.Errorf("room %s: %w", roomID, err)
		}
		eng = &engine{state: vmsync.NewWorkspaceState()}
		m.engines[roomID] = eng
	}
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
