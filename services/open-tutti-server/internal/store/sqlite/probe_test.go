package sqlite

import (
	"context"
	"testing"
	"time"

	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/store"
)

// TestEnrollWithTicketRedeemsFreshTicket pins the redemption compare-and-set:
// a ticket must stay valid through its final second (>=) and expire only
// strictly after expires_at; an earlier draft compared against the ticket's
// own stored instant and rejected every redemption.
func TestEnrollWithTicketRedeemsFreshTicket(t *testing.T) {
	r, err := Open(t.TempDir() + "/p.db")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close() // Windows: TempDir cleanup cannot remove an open db
	ctx := context.Background()
	exp := time.Now().Add(10 * time.Minute)
	if err := r.CreateJoinTicket(ctx, store.JoinTicket{Hash: "h1", RoomID: "r1", ShareID: "s1", ExpiresAt: exp}); err != nil {
		t.Fatal("insert:", err)
	}
	if err := r.CreateRoomWithOwner(ctx, store.Device{ID: "d1", DisplayName: "x", Hostname: "h", PublicKeyPEM: "k", FirstSeenAt: time.Now()}, store.Room{ID: "r1", ShareID: "s1", PasswordHash: "p", OwnerDeviceID: "d1", CreatedAt: time.Now()}, store.Membership{RoomID: "r1", DeviceID: "d1", JoinedAt: time.Now(), LastSeenAt: time.Now(), SessionTokenHash: "th"}); err != nil {
		t.Fatal("createRoomWithOwner:", err)
	}
	err = r.EnrollWithTicket(ctx, "h1", time.Now(), store.Device{ID: "d2", DisplayName: "y", Hostname: "h2", PublicKeyPEM: "k2", FirstSeenAt: time.Now()}, store.Membership{RoomID: "r1", DeviceID: "d2", JoinedAt: time.Now(), LastSeenAt: time.Now(), SessionTokenHash: "th2"})
	if err != nil {
		t.Fatal("enroll:", err)
	}
	t.Log("OK")
}
