package room

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/config"
	store_sqlite "github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/store/sqlite"
)

type manualClock struct {
	mu  sync.Mutex
	now time.Time
}

func newManualClock() *manualClock {
	return &manualClock{now: time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)}
}

func (c *manualClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }

func (c *manualClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func testConfig(invite string) config.Config {
	return config.Config{
		Secret: "test-secret", ServerInviteCode: invite,
		OwnerGracePeriod: 5 * time.Minute, JoinTicketTTL: 60 * time.Second,
		SnapshotIntervalOps: 512, PublicURL: "http://server.example",
	}
}

func newTestService(t *testing.T, invite string) (*Service, *manualClock) {
	t.Helper()
	repo, err := store_sqlite.Open(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { repo.Close() })
	clock := newManualClock()
	svc := NewService(repo, testConfig(invite), clock, nil)
	return svc, clock
}

func ownerDevice(id string) DeviceInput {
	return DeviceInput{ID: id, DisplayName: "Anna", Hostname: "Annas-MacBook-Pro", PublicKey: testKeyPEM()}
}

// testKeyPEM returns a real Ed25519 public-key PEM: device input
// validation rejects anything unparseable (fail closed).
func testKeyPEM() string {
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		panic(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pub}))
}

func memberDevice(id string) DeviceInput {
	return DeviceInput{ID: id, DisplayName: id, Hostname: id + "-host", PublicKey: testKeyPEM()}
}

func createRoom(t *testing.T, svc *Service, invite string) CreatedRoom {
	t.Helper()
	created, err := svc.CreateRoom(context.Background(), CreateRoomInput{InviteCode: invite, Device: ownerDevice("dev_owner")})
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	return created
}

func joinRoom(t *testing.T, svc *Service, created CreatedRoom, dev DeviceInput) string {
	t.Helper()
	ticket, _, err := svc.IssueJoinTicket(context.Background(), created.ShareID, created.Password)
	if err != nil {
		t.Fatalf("issue ticket: %v", err)
	}
	_, token, err := svc.JoinRedeem(context.Background(), created.RoomID, ticket, dev)
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	return token
}

func TestCreateRoomInviteCodeEnforced(t *testing.T) {
	svc, _ := newTestService(t, "secret-code")
	ctx := context.Background()

	if _, err := svc.CreateRoom(ctx, CreateRoomInput{Device: ownerDevice("d1")}); !errors.Is(err, ErrInviteRequired) {
		t.Fatalf("missing invite: %v", err)
	}
	if _, err := svc.CreateRoom(ctx, CreateRoomInput{InviteCode: "wrong", Device: ownerDevice("d1")}); !errors.Is(err, ErrInviteWrong) {
		t.Fatalf("wrong invite: %v", err)
	}
	created := createRoom(t, svc, "secret-code")
	if !strings.HasPrefix(created.ShareURL, "http://server.example/share/r_") {
		t.Fatalf("share url = %q", created.ShareURL)
	}
	if len(created.Password) != 6 || strings.Trim(created.Password, "0123456789") != "" {
		t.Fatalf("password %q is not 6 digits", created.Password)
	}
	if created.SessionToken == "" {
		t.Fatal("creator must receive a session token")
	}
}

func TestJoinTicketSingleUseAndPasswordBound(t *testing.T) {
	svc, _ := newTestService(t, "")
	created := createRoom(t, svc, "")
	ctx := context.Background()

	if _, _, err := svc.IssueJoinTicket(ctx, created.ShareID, "000000"); err == nil {
		t.Fatal("wrong password must fail")
	}
	ticket, expires, err := svc.IssueJoinTicket(ctx, created.ShareID, created.Password)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := svc.JoinRedeem(ctx, created.RoomID, ticket, memberDevice("dev_bob")); err != nil {
		t.Fatalf("first redeem: %v", err)
	}
	// Single use: a second redeem of the same ticket fails.
	if _, _, err := svc.JoinRedeem(ctx, created.RoomID, ticket, memberDevice("dev_carol")); err == nil {
		t.Fatal("ticket reuse must fail")
	}
	_ = expires
}

func TestJoinTicketExpiry(t *testing.T) {
	svc, clock := newTestService(t, "")
	created := createRoom(t, svc, "")
	ticket, _, err := svc.IssueJoinTicket(context.Background(), created.ShareID, created.Password)
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(61 * time.Second)
	if _, _, err := svc.JoinRedeem(context.Background(), created.RoomID, ticket, memberDevice("dev_late")); err == nil {
		t.Fatal("expired ticket must fail")
	}
}

func TestOwnerLeaveRules(t *testing.T) {
	svc, _ := newTestService(t, "")
	created := createRoom(t, svc, "")
	ctx := context.Background()

	// Owner cannot leave before applying the final workspace state.
	_, _, err := svc.Leave(ctx, LeaveInput{RoomID: created.RoomID, DeviceID: "dev_owner", WorkspaceApplied: false})
	if !errors.Is(err, ErrOwnerMustApply) {
		t.Fatalf("expected ErrOwnerMustApply, got %v", err)
	}
	// Applied but neither disband nor transfer.
	_, _, err = svc.Leave(ctx, LeaveInput{RoomID: created.RoomID, DeviceID: "dev_owner", WorkspaceApplied: true})
	if !errors.Is(err, ErrOwnerMustDisbandOrTransfer) {
		t.Fatalf("expected ErrOwnerMustDisbandOrTransfer, got %v", err)
	}
	// Disband after apply works and ends the room.
	if _, _, err := svc.Leave(ctx, LeaveInput{RoomID: created.RoomID, DeviceID: "dev_owner", WorkspaceApplied: true, Disband: true}); err != nil {
		t.Fatalf("disband: %v", err)
	}
	room, err := svc.repo.GetRoom(ctx, created.RoomID)
	if err != nil {
		t.Fatal(err)
	}
	if room.DissolvedAt == nil {
		t.Fatal("room not dissolved after owner disband")
	}
	// The share link is dead: tickets can no longer be issued.
	if _, _, err := svc.IssueJoinTicket(ctx, created.ShareID, created.Password); err == nil {
		t.Fatal("share link must be invalid after dissolution")
	}
}

func TestLastParticipantLeaveDissolvesRoom(t *testing.T) {
	svc, _ := newTestService(t, "")
	created := createRoom(t, svc, "")
	joinRoom(t, svc, created, memberDevice("dev_bob"))
	ctx := context.Background()

	// Participant leaves first — room continues for the owner.
	if _, _, err := svc.Leave(ctx, LeaveInput{RoomID: created.RoomID, DeviceID: "dev_bob"}); err != nil {
		t.Fatalf("participant leave: %v", err)
	}
	room, _ := svc.repo.GetRoom(ctx, created.RoomID)
	if room.DissolvedAt != nil {
		t.Fatal("room dissolved too early")
	}
	// Owner applies, disbands: last member out ends the meeting.
	if _, _, err := svc.Leave(ctx, LeaveInput{RoomID: created.RoomID, DeviceID: "dev_owner", WorkspaceApplied: true, Disband: true}); err != nil {
		t.Fatal(err)
	}
	room, _ = svc.repo.GetRoom(ctx, created.RoomID)
	if room.DissolvedAt == nil {
		t.Fatal("room must dissolve when everyone left")
	}
	members, _ := svc.repo.ListMemberships(ctx, created.RoomID)
	if len(members) != 0 {
		t.Fatalf("memberships must be cleared, got %d", len(members))
	}
}

func TestOwnerGracePeriodAutoTransfer(t *testing.T) {
	svc, clock := newTestService(t, "")
	created := createRoom(t, svc, "")
	joinRoom(t, svc, created, memberDevice("dev_bob"))
	joinRoom(t, svc, created, memberDevice("dev_carol"))
	ctx := context.Background()

	// All three connect; carol connects later so bob has the longest
	// continuous presence.
	if err := svc.MarkOnline(ctx, created.RoomID, "dev_owner"); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Minute)
	if err := svc.MarkOnline(ctx, created.RoomID, "dev_bob"); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Minute)
	if err := svc.MarkOnline(ctx, created.RoomID, "dev_carol"); err != nil {
		t.Fatal(err)
	}

	// Owner drops without a graceful leave.
	if _, err := svc.MarkOffline(ctx, created.RoomID, "dev_owner"); err != nil {
		t.Fatal(err)
	}

	// Inside the grace window the room keeps its owner.
	if _, err := svc.CheckGracePeriods(ctx, created.RoomID); err != nil {
		t.Fatal(err)
	}
	room, _ := svc.repo.GetRoom(ctx, created.RoomID)
	if room.OwnerDeviceID != "dev_owner" {
		t.Fatalf("owner changed inside grace window: %s", room.OwnerDeviceID)
	}

	// After the grace period a lazy-only room still promotes its
	// longest-connected online member: waiting is an unrecoverable
	// limbo because prepare/transfer is owner-gated and the absent
	// owner can never authorize one. The successor is recorded with
	// the full policy; its room-sync materializes on the IsOwner
	// event.
	clock.Advance(6 * time.Minute)
	if _, err := svc.CheckGracePeriods(ctx, created.RoomID); err != nil {
		t.Fatal(err)
	}
	room, _ = svc.repo.GetRoom(ctx, created.RoomID)
	if room.OwnerDeviceID != "dev_bob" {
		t.Fatalf("lazy-only room left leaderless or promoted %s, want dev_bob", room.OwnerDeviceID)
	}
	bob, err := svc.repo.GetMembership(ctx, created.RoomID, "dev_bob")
	if err != nil {
		t.Fatal(err)
	}
	if bob.ReplicaPolicy != "full" {
		t.Fatalf("successor policy = %s, want full", bob.ReplicaPolicy)
	}
	// The promoted owner stays owner while it remains online: a later
	// full-replica report by another member does not depose a live
	// successor mid-meeting.
	if _, err := svc.CheckGracePeriods(ctx, created.RoomID); err != nil {
		t.Fatal(err)
	}
	room, _ = svc.repo.GetRoom(ctx, created.RoomID)
	if room.OwnerDeviceID != "dev_bob" {
		t.Fatalf("live successor deposed: owner = %s want dev_bob", room.OwnerDeviceID)
	}
}

func TestOwnerGracePeriodNobodyOnlineDissolves(t *testing.T) {
	svc, clock := newTestService(t, "")
	created := createRoom(t, svc, "")
	ctx := context.Background()
	joinRoom(t, svc, created, memberDevice("dev_bob"))

	// Owner disconnects, participant is offline too.
	if _, err := svc.MarkOffline(ctx, created.RoomID, "dev_owner"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.MarkOffline(ctx, created.RoomID, "dev_bob"); err != nil {
		t.Fatal(err)
	}
	clock.Advance(6 * time.Minute)
	if _, err := svc.CheckGracePeriods(ctx, created.RoomID); err != nil {
		t.Fatal(err)
	}
	room, _ := svc.repo.GetRoom(ctx, created.RoomID)
	if room.DissolvedAt == nil {
		t.Fatal("room must dissolve when nobody is online after grace")
	}
}

func TestOwnershipTransferThreePhases(t *testing.T) {
	svc, _ := newTestService(t, "")
	created := createRoom(t, svc, "")
	joinRoom(t, svc, created, memberDevice("dev_leo"))
	ctx := context.Background()

	// Candidate must be a member.
	if err := svc.PrepareTransfer(ctx, created.RoomID, "dev_owner", "dev_stranger"); err == nil {
		t.Fatal("non-member candidate accepted")
	}
	prepared, err := svc.PrepareTransferWithSnapshot(ctx, created.RoomID, "dev_owner", "dev_leo", 1)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	// Client-side claims alone cannot commit.
	err = svc.CommitTransfer(ctx, created.RoomID, "dev_owner", "dev_leo", "old", prepared.SnapshotSeq)
	if !errors.Is(err, ErrTransferIncomplete) {
		t.Fatalf("expected ErrTransferIncomplete, got %v", err)
	}
	err = svc.CommitTransfer(ctx, created.RoomID, "dev_owner", "dev_leo", prepared.Generation, prepared.SnapshotSeq)
	if !errors.Is(err, ErrTransferIncomplete) {
		t.Fatalf("expected ErrTransferIncomplete, got %v", err)
	}
	if err := svc.ReportTransferReady(ctx, created.RoomID, "dev_leo", prepared.Generation, prepared.SnapshotSeq); err != nil {
		t.Fatalf("ready: %v", err)
	}
	if err := svc.CommitTransfer(ctx, created.RoomID, "dev_owner", "dev_leo", prepared.Generation, prepared.SnapshotSeq); err != nil {
		t.Fatalf("commit: %v", err)
	}
	room, _ := svc.repo.GetRoom(ctx, created.RoomID)
	if room.OwnerDeviceID != "dev_leo" || room.PendingTransferToDevice != "" {
		t.Fatalf("owner = %s pending = %s", room.OwnerDeviceID, room.PendingTransferToDevice)
	}
	// The old owner is now a plain participant and can leave freely.
	if _, _, err := svc.Leave(ctx, LeaveInput{RoomID: created.RoomID, DeviceID: "dev_owner"}); err != nil {
		t.Fatalf("old owner leave after transfer: %v", err)
	}
	// Non-owners cannot drive transfer endpoints.
	if err := svc.PrepareTransfer(ctx, created.RoomID, "dev_owner", "dev_leo"); err == nil {
		t.Fatal("non-owner prepare accepted")
	}
}

func TestSessionTokenValidation(t *testing.T) {
	svc, _ := newTestService(t, "")
	created := createRoom(t, svc, "")
	token := joinRoom(t, svc, created, memberDevice("dev_bob"))
	ctx := context.Background()

	roomID, deviceID, err := svc.ValidateSessionToken(ctx, token)
	if err != nil || roomID != created.RoomID || deviceID != "dev_bob" {
		t.Fatalf("validate: %v %s %s", err, roomID, deviceID)
	}
	if _, _, err := svc.ValidateSessionToken(ctx, token+".tampered"); err == nil {
		t.Fatal("tampered token accepted")
	}
	if _, _, err := svc.ValidateSessionToken(ctx, "garbage"); err == nil {
		t.Fatal("garbage token accepted")
	}
	// Leaving invalidates the session token.
	if _, _, err := svc.Leave(ctx, LeaveInput{RoomID: created.RoomID, DeviceID: "dev_bob"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := svc.ValidateSessionToken(ctx, token); err == nil {
		t.Fatal("token must die with the membership")
	}
}

func TestRoomPasswordRotation(t *testing.T) {
	svc, _ := newTestService(t, "")
	created := createRoom(t, svc, "")
	ctx := context.Background()

	joinRoom(t, svc, created, memberDevice("dev_bob"))
	// Only the owner may rotate.
	if _, err := svc.RotatePassword(ctx, created.RoomID, "dev_bob"); err == nil {
		t.Fatal("non-owner rotation accepted")
	}
	newPassword, err := svc.RotatePassword(ctx, created.RoomID, "dev_owner")
	if err != nil {
		t.Fatal(err)
	}
	// The old password stops working; the rotated one works.
	if _, _, err := svc.IssueJoinTicket(ctx, created.ShareID, created.Password); err == nil {
		t.Fatal("old password still valid after rotation")
	}
	ticket, _, err := svc.IssueJoinTicket(ctx, created.ShareID, newPassword)
	if err != nil {
		t.Fatalf("rotated password rejected: %v", err)
	}
	if _, _, err := svc.JoinRedeem(ctx, created.RoomID, ticket, memberDevice("dev_carol")); err != nil {
		t.Fatal(err)
	}
}
