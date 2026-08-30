package sqlite

import (
	"context"
	"errors"
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
