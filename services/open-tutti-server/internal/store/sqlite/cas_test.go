package sqlite

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/store"
)

func TestCASRefsSizedQuotaDeduplicatesPerRoomAndRollsBack(t *testing.T) {
	r, err := Open(t.TempDir() + "/cas.db")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ctx := context.Background()
	obj := store.CASObject{Hash: "sha256:" + "a" /* valid shape is not required by accounting */, Size: 7}
	if err := r.AddCASRefsSized(ctx, "room-a", []store.CASObject{obj, obj}, 7); err != nil {
		t.Fatal(err)
	}
	if err := r.AddCASRefsSized(ctx, "room-b", []store.CASObject{obj}, 7); err != nil {
		t.Fatal(err)
	}
	if err := r.AddCASRefsSized(ctx, "room-a", []store.CASObject{{Hash: "sha256:" + "b", Size: 1}}, 7); err == nil {
		t.Fatal("quota overflow accepted")
	}
	refs, err := r.RoomCASRefs(ctx, "room-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 {
		t.Fatalf("failed transaction changed refs: %v", refs)
	}
	if ok, err := r.HasCASRef(ctx, "room-b", obj.Hash); err != nil || !ok {
		t.Fatalf("cross-room reference missing: ok=%v err=%v", ok, err)
	}
	if errors.Is(err, store.ErrNotFound) {
		t.Fatal("unexpected not found")
	}
}

func TestCASPendingRefsPromoteAndSweep(t *testing.T) {
	r, err := Open(t.TempDir() + "/pending.db")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ctx := context.Background()
	ref := store.CASPendingRef{RoomID: "room-a", DeviceID: "dev-a", Hash: "sha256:" + strings.Repeat("a", 64), Size: 10, ExpiresAt: time.Now().Add(time.Hour)}
	if err := r.ReserveCASPending(ctx, ref, 20); err != nil {
		t.Fatal(err)
	}
	if ok, err := r.HasCASRef(ctx, ref.RoomID, ref.Hash); err != nil || ok {
		t.Fatalf("pending ref became live: %v %v", ok, err)
	}
	if err := r.PromoteCASPending(ctx, ref.RoomID, ref.DeviceID, []string{ref.Hash}, 20); err != nil {
		t.Fatal(err)
	}
	if ok, err := r.HasCASRef(ctx, ref.RoomID, ref.Hash); err != nil || !ok {
		t.Fatalf("promotion missing: %v %v", ok, err)
	}
	ref.Hash = "sha256:" + strings.Repeat("b", 64)
	ref.ExpiresAt = time.Unix(10, 0)
	if err := r.ReserveCASPending(ctx, ref, 20); err != nil {
		t.Fatal(err)
	}
	if err := r.SweepCASPending(ctx, time.Unix(10, 0)); err != nil {
		t.Fatal(err)
	}
}

func TestPromoteCASPendingMissingReservationRollsBack(t *testing.T) {
	r, err := Open(t.TempDir() + "/pending.db")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ctx := context.Background()
	hash := "sha256:" + strings.Repeat("d", 64)
	if err := r.PromoteCASPending(ctx, "room-a", "dev-a", []string{hash}, 20); err == nil {
		t.Fatal("promotion without a reservation succeeded")
	}
	if ok, err := r.HasCASRef(ctx, "room-a", hash); err != nil || ok {
		t.Fatalf("failed promotion changed live refs: ok=%v err=%v", ok, err)
	}
}

func TestPublishCASPendingRollsBackPromotionWhenPrepareFails(t *testing.T) {
	r, err := Open(t.TempDir() + "/pending.db")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ctx := context.Background()
	hash := "sha256:" + strings.Repeat("e", 64)
	if err := r.ReserveCASPending(ctx, store.CASPendingRef{
		RoomID: "room-a", DeviceID: "dev-a", Hash: hash, Size: 10, ExpiresAt: time.Now().Add(time.Hour),
	}, 20); err != nil {
		t.Fatal(err)
	}
	prepareErr := errors.New("prepare rejected")
	if err := r.PublishCASPending(ctx, "room-a", "dev-a", []string{hash}, 20, func() error { return prepareErr }); !errors.Is(err, prepareErr) {
		t.Fatalf("PublishCASPending error = %v, want %v", err, prepareErr)
	}
	if ok, err := r.HasCASRef(ctx, "room-a", hash); err != nil || ok {
		t.Fatalf("failed publication changed live refs: ok=%v err=%v", ok, err)
	}
	if err := r.CanPromoteCASPending(ctx, "room-a", "dev-a", []string{hash}, 20); err != nil {
		t.Fatalf("failed publication lost pending reservation: %v", err)
	}
}

func TestCollectCASDoesNotHoldDatabaseTransactionDuringDelete(t *testing.T) {
	r, err := Open(t.TempDir() + "/cas.db")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ctx := context.Background()
	hash := "sha256:" + "c"
	started := make(chan struct{})
	finish := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- r.CollectUnreferencedCAS(ctx, []string{hash}, func(string) error {
			close(started)
			<-finish
			return nil
		})
	}()
	<-started
	// The slow filesystem callback must not keep the SQLite transaction or
	// connection checked out. This query would block until the callback in the
	// old implementation returned.
	queryDone := make(chan error, 1)
	go func() {
		_, err := r.ListCASRefCounts(ctx)
		queryDone <- err
	}()
	select {
	case err := <-queryDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("database query blocked by CAS deletion")
	}
	close(finish)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestCollectCASDeletesMetadataOnlyAfterObjectDelete(t *testing.T) {
	r, err := Open(t.TempDir() + "/cas.db")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ctx := context.Background()
	hash := "sha256:" + strings.Repeat("f", 64)
	if err := r.AddCASRefsSized(ctx, "room-a", []store.CASObject{{Hash: hash, Size: 1}}, 10); err != nil {
		t.Fatal(err)
	}
	if err := r.DeleteRoomCASRefs(ctx, "room-a"); err != nil {
		t.Fatal(err)
	}
	if err := r.CollectUnreferencedCAS(ctx, []string{hash}, func(string) error { return nil }); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM cas_objects WHERE hash=?`, hash).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("unreferenced CAS metadata was retained")
	}
}

func TestDeleteCASPendingAndCollectKeepsOtherRoomLiveMetadata(t *testing.T) {
	r, err := Open(t.TempDir() + "/cas.db")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ctx := context.Background()
	hash := "sha256:" + strings.Repeat("1", 64)
	if err := r.AddCASRefsSized(ctx, "room-live", []store.CASObject{{Hash: hash, Size: 4}}, 10); err != nil {
		t.Fatal(err)
	}
	if err := r.ReserveCASPending(ctx, store.CASPendingRef{RoomID: "room-failed", DeviceID: "dev", Hash: hash, Size: 4, ExpiresAt: time.Now().Add(time.Hour)}, 10); err != nil {
		t.Fatal(err)
	}
	if err := r.DeleteCASPendingAndCollect(ctx, "room-failed", "dev", []string{hash}); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM cas_objects WHERE hash=?`, hash).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("live metadata count = %d", count)
	}
	if ok, err := r.HasCASRef(ctx, "room-live", hash); err != nil || !ok {
		t.Fatalf("live ref lost: %v %v", ok, err)
	}
}
