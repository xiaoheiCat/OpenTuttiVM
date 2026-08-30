package sequencer

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	vmcas "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-cas"
	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/config"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/store"
	store_sqlite "github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/store/sqlite"
)

type recordingSender struct {
	events []vmprotocol.Event
}

func (s *recordingSender) BroadcastRoom(_ string, ev vmprotocol.Event) {
	s.events = append(s.events, ev)
}
func (s *recordingSender) SendTo(_ string, _ string, ev vmprotocol.Event) {
	s.events = append(s.events, ev)
}

func TestSubmitExpiredBlobPendingDoesNotAdvanceStateOrBroadcast(t *testing.T) {
	repo, err := store_sqlite.Open(t.TempDir() + "/sequencer.db")
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	ctx := context.Background()
	now := time.Now()
	if err := repo.CreateRoomWithOwner(ctx,
		store.Device{ID: "dev-a", DisplayName: "A", Hostname: "host", PublicKeyPEM: "key", FirstSeenAt: now},
		store.Room{ID: "room-a", ShareID: "share-a", PasswordHash: "password", OwnerDeviceID: "dev-a", CreatedAt: now},
		store.Membership{RoomID: "room-a", DeviceID: "dev-a", JoinedAt: now, LastSeenAt: now},
	); err != nil {
		t.Fatal(err)
	}

	cas := vmcas.NewMemoryStore()
	sender := &recordingSender{}
	mgr := NewManager(repo, config.Config{CASRoomQuotaBytes: 1 << 20, CASPendingQuotaBytes: 1 << 20}, cas, sender, slog.Default())
	if err := mgr.Submit(vmprotocol.Envelope{
		RoomID: "room-a", OperationID: "create", AuthorDeviceID: "dev-a",
		Operation: vmprotocol.FileOperation{ID: "create", Path: "file.bin", Kind: vmprotocol.OpCreate},
	}); err != nil {
		t.Fatal(err)
	}
	manifest, chunks, err := vmcas.BuildManifest(strings.NewReader("blob"))
	if err != nil {
		t.Fatal(err)
	}
	for i, chunk := range chunks {
		if err := cas.Put(manifest.Chunks[i], chunk); err != nil {
			t.Fatal(err)
		}
	}
	if err := cas.Put(manifest.Hash, manifest.Body()); err != nil {
		t.Fatal(err)
	}
	if err := repo.ReserveCASPending(ctx, store.CASPendingRef{
		RoomID: "room-a", DeviceID: "dev-a", Hash: manifest.Hash,
		Size: int64(len(manifest.Body())), ExpiresAt: now.Add(-time.Second),
	}, 1<<20); err != nil {
		t.Fatal(err)
	}
	if err := repo.ReserveCASPending(ctx, store.CASPendingRef{
		RoomID: "room-a", DeviceID: "dev-a", Hash: manifest.Chunks[0],
		Size: int64(len(chunks[0])), ExpiresAt: now.Add(-time.Second),
	}, 1<<20); err != nil {
		t.Fatal(err)
	}

	beforeEvents := len(sender.events)
	err = mgr.Submit(vmprotocol.Envelope{
		RoomID: "room-a", OperationID: "blob", AuthorDeviceID: "dev-a", BaseSeq: 1,
		Operation: vmprotocol.FileOperation{ID: "blob", Path: "file.bin", Kind: vmprotocol.OpBlobReplace,
			Blob: &vmprotocol.BlobReplace{Manifest: manifest.Hash, Size: manifest.Size}},
	})
	if err == nil {
		t.Fatal("expired pending blob was accepted")
	}
	seq, err := mgr.CurrentSequence("room-a")
	if err != nil {
		t.Fatal(err)
	}
	if seq != 1 {
		t.Fatalf("state sequence advanced after expired pending: %d", seq)
	}
	for _, ev := range sender.events[beforeEvents:] {
		if ev.Topic == vmprotocol.TopicOperation {
			t.Fatal("expired pending blob was broadcast as an operation")
		}
	}
}
