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
		SnapshotIntervalOps: 512, PublicURL: "http://server.example", ActiveRoomLimit: 100,
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

func TestCreateRoomActiveLimitAndCancelledArgonWait(t *testing.T) {
	svc, _ := newTestService(t, "")
	svc.cfg.ActiveRoomLimit = 1
	createRoom(t, svc, "")
	if _, err := svc.CreateRoom(context.Background(), CreateRoomInput{Device: ownerDevice("dev_two")}); !errors.Is(err, ErrActiveRoomLimit) {
		t.Fatalf("active room limit error = %v", err)
	}
	svc.cfg.ActiveRoomLimit = 100
	for i := 0; i < cap(svc.argonSem); i++ {
		svc.argonSem <- struct{}{}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := svc.CreateRoom(ctx, CreateRoomInput{Device: ownerDevice("dev_three")}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Argon wait error = %v", err)
	}
	for i := 0; i < cap(svc.argonSem); i++ {
		<-svc.argonSem
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

	// A lazy-only room cannot be promoted from a client policy claim. The
	// fail-closed recovery state remains explicitly recoverable.
	clock.Advance(6 * time.Minute)
	if _, err := svc.CheckGracePeriods(ctx, created.RoomID); err != nil {
		t.Fatal(err)
	}
	room, _ = svc.repo.GetRoom(ctx, created.RoomID)
	if room.DissolvedAt != nil || room.OwnerDeviceID != "dev_owner" {
		t.Fatal("unverified succession must keep the room recoverable")
	}
}

func TestAuthenticatedMemberCanPrepareRecoveryWithoutOwnership(t *testing.T) {
	svc, clock := newTestService(t, "")
	created := createRoom(t, svc, "")
	joinRoom(t, svc, created, memberDevice("dev_bob"))
	clock.Advance(6 * time.Minute)
	ctx := context.Background()
	result, err := svc.PrepareRecoveryTransfer(ctx, created.RoomID, "dev_bob", "dev_bob", 7)
	if err != nil {
		t.Fatalf("recovery prepare: %v", err)
	}
	if result.Generation == "" || result.SnapshotSeq != 7 {
		t.Fatalf("unexpected recovery fence: %+v", result)
	}
	room, err := svc.repo.GetRoom(ctx, created.RoomID)
	if err != nil {
		t.Fatal(err)
	}
	if room.OwnerDeviceID != "dev_owner" || room.PendingTransferToDevice != "dev_bob" {
		t.Fatalf("recovery prepare changed ownership state: owner=%s candidate=%s", room.OwnerDeviceID, room.PendingTransferToDevice)
	}
}

func TestRecoveryCommitRejectsOwnerReconnectAfterPrepare(t *testing.T) {
	svc, clock := newTestService(t, "")
	svc.SetTransferHostReadiness(func(context.Context, string, string) bool { return true })
	svc.SetCurrentSequence(func(string) (uint64, error) { return 0, nil })
	created := createRoom(t, svc, "")
	joinRoom(t, svc, created, memberDevice("dev_bob"))
	clock.Advance(6 * time.Minute)
	ctx := context.Background()
	prepared, err := svc.PrepareRecoveryTransfer(ctx, created.RoomID, "dev_bob", "dev_bob", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.ReportTransferReady(ctx, created.RoomID, "dev_bob", prepared.Generation, 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := svc.MarkOnline(ctx, created.RoomID, "dev_owner"); err != nil {
		t.Fatal(err)
	}
	if err := svc.CommitRecoveryTransfer(ctx, created.RoomID, "dev_bob", prepared.Generation, 0); !errors.Is(err, ErrTransferIncomplete) {
		t.Fatalf("recovery commit after owner reconnect = %v", err)
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

func TestReportedFullPolicyCannotSuccedeLazyMember(t *testing.T) {
	svc, clock := newTestService(t, "")
	created := createRoom(t, svc, "")
	joinRoom(t, svc, created, memberDevice("dev_lazy"))
	ctx := context.Background()
	if err := svc.ReportReplicaPolicy(ctx, created.RoomID, "dev_lazy", "full"); err != nil {
		t.Fatal(err)
	}
	if err := svc.MarkOnline(ctx, created.RoomID, "dev_owner"); err != nil {
		t.Fatal(err)
	}
	if err := svc.MarkOnline(ctx, created.RoomID, "dev_lazy"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.MarkOffline(ctx, created.RoomID, "dev_owner"); err != nil {
		t.Fatal(err)
	}
	clock.Advance(6 * time.Minute)
	if _, err := svc.CheckGracePeriods(ctx, created.RoomID); err != nil {
		t.Fatal(err)
	}
	room, err := svc.repo.GetRoom(ctx, created.RoomID)
	if err != nil {
		t.Fatal(err)
	}
	if room.OwnerDeviceID != "dev_owner" {
		t.Fatalf("forged policy changed owner to %s", room.OwnerDeviceID)
	}
}

func TestOwnershipTransferThreePhases(t *testing.T) {
	svc, _ := newTestService(t, "")
	hostReady := true
	svc.SetTransferHostReadiness(func(context.Context, string, string) bool { return hostReady })
	currentSeq := uint64(1)
	svc.SetCurrentSequence(func(string) (uint64, error) { return currentSeq, nil })
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
	if err := svc.ReportTransferReady(ctx, created.RoomID, "dev_leo", prepared.Generation, prepared.SnapshotSeq, prepared.SnapshotSeq); err != nil {
		t.Fatalf("ready: %v", err)
	}
	currentSeq = 2
	if err := svc.CommitTransfer(ctx, created.RoomID, "dev_owner", "dev_leo", prepared.Generation, prepared.SnapshotSeq); !errors.Is(err, ErrTransferIncomplete) {
		t.Fatalf("stale transfer commit error = %v", err)
	}
	currentSeq = 1
	hostReady = false
	if err := svc.CommitTransfer(ctx, created.RoomID, "dev_owner", "dev_leo", prepared.Generation, prepared.SnapshotSeq); !errors.Is(err, ErrTransferIncomplete) {
		t.Fatalf("unready host commit error = %v", err)
	}
	hostReady = true
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

func TestOwnershipTransferAcceptsEmptyWorkspaceSequences(t *testing.T) {
	svc, _ := newTestService(t, "")
	svc.SetTransferHostReadiness(func(context.Context, string, string) bool { return true })
	svc.SetCurrentSequence(func(string) (uint64, error) { return 0, nil })
	created := createRoom(t, svc, "")
	joinRoom(t, svc, created, memberDevice("dev_empty"))
	ctx := context.Background()
	prepared, err := svc.PrepareTransferWithSnapshot(ctx, created.RoomID, "dev_owner", "dev_empty", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.ReportTransferReady(ctx, created.RoomID, "dev_empty", prepared.Generation, 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := svc.CommitTransfer(ctx, created.RoomID, "dev_owner", "dev_empty", prepared.Generation, 0); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryTransferAcceptsEmptyWorkspaceSequences(t *testing.T) {
	svc, clock := newTestService(t, "")
	svc.SetTransferHostReadiness(func(context.Context, string, string) bool { return true })
	svc.SetCurrentSequence(func(string) (uint64, error) { return 0, nil })
	created := createRoom(t, svc, "")
	joinRoom(t, svc, created, memberDevice("dev_empty"))
	clock.Advance(6 * time.Minute)
	ctx := context.Background()
	prepared, err := svc.PrepareRecoveryTransfer(ctx, created.RoomID, "dev_empty", "dev_empty", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.ReportTransferReady(ctx, created.RoomID, "dev_empty", prepared.Generation, 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := svc.CommitRecoveryTransfer(ctx, created.RoomID, "dev_empty", prepared.Generation, 0); err != nil {
		t.Fatal(err)
	}
}

func TestAutomaticSuccessionAcceptsEmptyWorkspaceSequences(t *testing.T) {
	svc, clock := newTestService(t, "")
	svc.SetTransferHostReadiness(func(context.Context, string, string) bool { return true })
	svc.SetCurrentSequence(func(string) (uint64, error) { return 0, nil })
	created := createRoom(t, svc, "")
	joinRoom(t, svc, created, memberDevice("dev_empty"))
	ctx := context.Background()
	if err := svc.MarkOnline(ctx, created.RoomID, "dev_owner"); err != nil {
		t.Fatal(err)
	}
	if err := svc.MarkOnline(ctx, created.RoomID, "dev_empty"); err != nil {
		t.Fatal(err)
	}
	prepared, err := svc.PrepareTransferWithSnapshot(ctx, created.RoomID, "dev_owner", "dev_empty", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.ReportTransferReady(ctx, created.RoomID, "dev_empty", prepared.Generation, 0, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.MarkOffline(ctx, created.RoomID, "dev_owner"); err != nil {
		t.Fatal(err)
	}
	clock.Advance(6 * time.Minute)
	if _, err := svc.CheckGracePeriods(ctx, created.RoomID); err != nil {
		t.Fatal(err)
	}
	room, err := svc.repo.GetRoom(ctx, created.RoomID)
	if err != nil {
		t.Fatal(err)
	}
	if room.OwnerDeviceID != "dev_empty" {
		t.Fatalf("owner = %s", room.OwnerDeviceID)
	}
}

func TestTransferReadinessExpiresWithPresenceSession(t *testing.T) {
	svc, _ := newTestService(t, "")
	svc.SetTransferHostReadiness(func(context.Context, string, string) bool { return true })
	svc.SetCurrentSequence(func(string) (uint64, error) { return 0, nil })
	created := createRoom(t, svc, "")
	joinRoom(t, svc, created, memberDevice("dev_candidate"))
	ctx := context.Background()
	if err := svc.MarkOnline(ctx, created.RoomID, "dev_owner"); err != nil {
		t.Fatal(err)
	}
	if err := svc.MarkOnline(ctx, created.RoomID, "dev_candidate"); err != nil {
		t.Fatal(err)
	}
	prepared, err := svc.PrepareTransferWithSnapshot(ctx, created.RoomID, "dev_owner", "dev_candidate", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.ReportTransferReady(ctx, created.RoomID, "dev_candidate", prepared.Generation, 0, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.MarkOffline(ctx, created.RoomID, "dev_candidate"); err != nil {
		t.Fatal(err)
	}
	if err := svc.MarkOnline(ctx, created.RoomID, "dev_candidate"); err != nil {
		t.Fatal(err)
	}
	if err := svc.CommitTransfer(ctx, created.RoomID, "dev_owner", "dev_candidate", prepared.Generation, 0); !errors.Is(err, ErrTransferIncomplete) {
		t.Fatalf("stale readiness committed after a new presence session: %v", err)
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
