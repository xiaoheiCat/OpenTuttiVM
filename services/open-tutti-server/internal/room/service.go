// Package room owns the meeting-style room lifecycle of open-tutti-server:
// creation with the server invite code, share/password joins, presence,
// owner grace periods, three-phase ownership transfer, and dissolution.
package room

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base32"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/config"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/store"
)

// Clock is injectable time for deterministic lifecycle tests.
type Clock interface {
	Now() time.Time
}

// RealClock is the wall clock.
type RealClock struct{}

// Now returns the current time.
func (RealClock) Now() time.Time { return time.Now().UTC() }

// Broadcaster fans lifecycle events out to room members. The realtime hub
// implements it.
type Broadcaster interface {
	BroadcastRoom(roomID string, ev vmprotocol.Event)
}

// ErrInviteRequired / ErrInviteWrong report the server invite code checks.
var (
	ErrInviteRequired = errors.New("this server requires an invite code to create rooms")
	ErrInviteWrong    = errors.New("invalid server invite code")
	// ErrOwnerMustApply: the owner cannot leave before applying the final
	// workspace state back to the host workspace.
	ErrOwnerMustApply = errors.New("owner must apply the workspace before leaving")
	// ErrOwnerMustDisbandOrTransfer: leaving requires ending the meeting.
	ErrOwnerMustDisbandOrTransfer = errors.New("owner must disband the room or complete an ownership transfer before leaving")
	// ErrTransferIncomplete: the 3-phase transfer has not finished.
	ErrTransferIncomplete = errors.New("ownership transfer incomplete: candidate needs a full replica and an initialized host workspace")
)

// Service implements room lifecycle.
type Service struct {
	repo   store.Repository
	cfg    config.Config
	clock  Clock
	bcast  Broadcaster
	tokens *tokenMinter

	mu sync.Mutex
	// shareAttempts throttles share-password verification per share id:
	// Argon2id over a six-digit space must not be brute-forceable (or
	// usable as a memory-exhaustion vector) through the public share
	// endpoint. A one-minute sliding window; keyed by share id because
	// that is the resource being guessed.
	shareAttempts map[string][]time.Time
	// argonSem bounds concurrent Argon2id derivations process-wide
	// (each costs ~64 MiB): unbounded parallel verification of wrong
	// passwords is a denial-of-service amplifier on a directly exposed
	// server, independent of the per-share window.
	argonSem chan struct{}
}

// Password attempt policy for the public share endpoint.
const (
	shareAttemptWindow  = time.Minute
	shareAttemptMax     = 10
	argonConcurrencyMax = 4
)

// NewService wires the lifecycle service. The broadcaster can be attached
// later to break the hub/sequencer construction cycle.
func NewService(repo store.Repository, cfg config.Config, clock Clock, bcast Broadcaster) *Service {
	return &Service{
		repo: repo, cfg: cfg, clock: clock, bcast: bcast, tokens: newTokenMinter(cfg.Secret),
		shareAttempts: map[string][]time.Time{},
		argonSem:      make(chan struct{}, argonConcurrencyMax),
	}
}

// SetBroadcaster attaches or replaces the event fanout.
func (s *Service) SetBroadcaster(b Broadcaster) { s.bcast = b }

// broadcast is nil-safe so lifecycle tests can run without a hub.
func (s *Service) broadcast(roomID string, ev vmprotocol.Event) {
	if s.bcast != nil {
		s.bcast.BroadcastRoom(roomID, ev)
	}
}

// CreateRoomInput carries a creation request.
type CreateRoomInput struct {
	InviteCode string
	Device     DeviceInput
}

// DeviceInput describes the enrolling device (a device is a user).
type DeviceInput struct {
	ID          string `json:"id"`
	DisplayName string `json:"display_name"`
	Hostname    string `json:"hostname"`
	PublicKey   string `json:"public_key"`
	// Proof is the base64 Ed25519 signature over "open-tutti-join:"+ticket
	// with the device's private key. Required when the device id is
	// already enrolled: the one-time ticket is the challenge, so token
	// refresh proves key possession instead of just knowing an id.
	Proof string `json:"proof"`
}

// CreatedRoom is returned exactly once to the creator: the share URL and
// the plaintext room password never appear again.
type CreatedRoom struct {
	RoomID       string `json:"room_id"`
	ShareID      string `json:"share_id"`
	ShareURL     string `json:"share_url"`
	Password     string `json:"password"`
	SessionToken string `json:"session_token"`
}

// CreateRoom validates the invite code against the current configuration
// and creates the room with a 6-digit owner-editable password.
func (s *Service) CreateRoom(ctx context.Context, in CreateRoomInput) (CreatedRoom, error) {
	if s.cfg.ServerInviteCode != "" && subtle.ConstantTimeCompare([]byte(in.InviteCode), []byte(s.cfg.ServerInviteCode)) != 1 {
		if in.InviteCode == "" {
			return CreatedRoom{}, ErrInviteRequired
		}
		return CreatedRoom{}, ErrInviteWrong
	}
	// Room creation must not become a key-rewrite oracle: an attacker
	// holding a valid join ticket could otherwise create a throwaway room
	// under the victim's device id with their own key, silently replacing
	// the enrolled key and then passing join proofs. Enrolled keys are
	// immutable, and reusing an existing device id anywhere requires
	// proving possession of that enrolled key.
	if existing, err := s.repo.GetDevice(ctx, in.Device.ID); err == nil {
		if in.Device.Proof == "" || existing.PublicKeyPEM == "" {
			return CreatedRoom{}, errors.New("device identity proof required")
		}
		if !VerifyDeviceProof(existing.PublicKeyPEM, CreateRoomProofMessage(in.Device.ID), in.Device.Proof) {
			return CreatedRoom{}, errors.New("device identity proof failed")
		}
		in.Device.PublicKey = existing.PublicKeyPEM
	}
	if err := s.upsertDevice(ctx, in.Device); err != nil {
		return CreatedRoom{}, err
	}
	roomID := "room_" + randomToken(16)
	shareID := "r_" + randomToken(24)
	password := sixDigitPassword()
	hash, err := HashRoomPassword(password)
	if err != nil {
		return CreatedRoom{}, err
	}
	now := s.clock.Now()
	if err := s.repo.CreateRoom(ctx, store.Room{
		ID: roomID, ShareID: shareID, PasswordHash: hash,
		OwnerDeviceID: in.Device.ID, CreatedAt: now,
	}); err != nil {
		return CreatedRoom{}, fmt.Errorf("create room: %w", err)
	}
	token, tokenHash, err := s.tokens.mint(roomID, in.Device.ID)
	if err != nil {
		return CreatedRoom{}, err
	}
	if err := s.repo.UpsertMembership(ctx, store.Membership{
		RoomID: roomID, DeviceID: in.Device.ID, JoinedAt: now, LastSeenAt: now,
		SessionTokenHash: tokenHash,
	}); err != nil {
		return CreatedRoom{}, err
	}
	return CreatedRoom{
		RoomID: roomID, ShareID: shareID,
		ShareURL:     strings.TrimRight(s.cfg.PublicURL, "/") + "/share/" + shareID,
		Password:     password,
		SessionToken: token,
	}, nil
}

// RotatePassword sets a new 6-digit room password (owner only) and returns
// it once.
func (s *Service) RotatePassword(ctx context.Context, roomID, deviceID string) (string, error) {
	room, _, err := s.authorizeOwnerOf(ctx, roomID, deviceID)
	if err != nil {
		return "", err
	}
	password := sixDigitPassword()
	hash, err := HashRoomPassword(password)
	if err != nil {
		return "", err
	}
	// Field-specific update: the Argon2id hash above took a while, and a
	// full-record write would clobber a transfer/dissolution committed
	// concurrently (stale owner, resurrected room).
	if err := s.repo.UpdateRoomPassword(ctx, room.ID, hash); err != nil {
		return "", err
	}
	return password, nil
}

// GetRoom exposes room state (lifecycle decisions like post-leave engine
// teardown check DissolvedAt here).
func (s *Service) GetRoom(ctx context.Context, roomID string) (store.Room, error) {
	return s.repo.GetRoom(ctx, roomID)
}

// RevokeShareLink invalidates the room's share link: no new joins can
// mint tickets. Existing members keep access until the room ends. Owner
// only.
func (s *Service) RevokeShareLink(ctx context.Context, roomID, deviceID string) error {
	room, _, err := s.authorizeOwnerOf(ctx, roomID, deviceID)
	if err != nil {
		return err
	}
	if room.ShareRevokedAt == nil {
		now := s.clock.Now()
		// Field-specific update: a full-record write would clobber a
		// transfer/dissolution committed since authorizeOwnerOf read
		// the room (stale owner restored, dissolved room resurrected).
		if err := s.repo.UpdateRoomShareRevoked(ctx, room.ID, now); err != nil {
			return err
		}
	}
	return nil
}

// KickMember removes one member from the room: their membership and
// session token die immediately. Owners cannot kick themselves (leave or
// transfer instead). Owner only.
func (s *Service) KickMember(ctx context.Context, roomID, ownerDeviceID, targetDeviceID string) error {
	if _, _, err := s.authorizeOwnerOf(ctx, roomID, ownerDeviceID); err != nil {
		return err
	}
	if targetDeviceID == ownerDeviceID {
		return errors.New("owners leave via leave/transfer, not kick")
	}
	if _, err := s.repo.GetMembership(ctx, roomID, targetDeviceID); err != nil {
		return err
	}
	if err := s.repo.DeleteMembership(ctx, roomID, targetDeviceID); err != nil {
		return err
	}
	if s.bcast != nil {
		s.bcast.BroadcastRoom(roomID, vmprotocol.Event{
			Topic:  vmprotocol.TopicPresence,
			RoomID: roomID,
			Payload: mustJSON(vmprotocol.PresenceDevice{
				DeviceID: targetDeviceID, Online: false,
			}),
		})
	}
	return nil
}

// IssueJoinTicket verifies the room password from the share page and issues
// a one-time, short-lived ticket. The password itself never enters the deep
// link. Verification is throttled per share and bounded process-wide.
func (s *Service) IssueJoinTicket(ctx context.Context, shareID, password string) (ticket string, expiresAt time.Time, err error) {
	room, err := s.repo.GetRoomByShareID(ctx, shareID)
	if err != nil {
		return "", time.Time{}, err
	}
	if room.ShareRevokedAt != nil {
		return "", time.Time{}, errors.New("share link revoked")
	}
	if !s.allowShareAttempt(shareID) {
		return "", time.Time{}, errors.New("too many attempts: wait a minute")
	}
	// Bound concurrent Argon2 work before deriving.
	s.argonSem <- struct{}{}
	ok := VerifyRoomPassword(password, room.PasswordHash)
	<-s.argonSem
	if !ok {
		return "", time.Time{}, errors.New("wrong room password")
	}
	s.clearShareAttempts(shareID)
	ticket = "jt_" + randomToken(32)
	expiresAt = s.clock.Now().Add(s.cfg.JoinTicketTTL)
	// The Argon2 window above is seconds wide: the owner may have
	// revoked the share or rotated the password meanwhile. Re-read the
	// room and re-validate before persisting, or an old-password
	// request receives a redeemable ticket for a revoked share.
	current, err := s.repo.GetRoom(ctx, room.ID)
	if err != nil {
		return "", time.Time{}, err
	}
	if current.ShareRevokedAt != nil {
		return "", time.Time{}, errors.New("share link revoked")
	}
	if !VerifyRoomPassword(password, current.PasswordHash) {
		return "", time.Time{}, errors.New("wrong room password")
	}
	if err := s.repo.CreateJoinTicket(ctx, store.JoinTicket{
		Hash: hashToken(ticket), RoomID: room.ID, ShareID: shareID, ExpiresAt: expiresAt,
	}); err != nil {
		return "", time.Time{}, err
	}
	return ticket, expiresAt, nil
}

// allowShareAttempt records one password attempt against the share's
// sliding window and reports whether it may proceed.
func (s *Service) allowShareAttempt(shareID string) bool {
	now := s.clock.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	attempts := s.shareAttempts[shareID]
	kept := attempts[:0]
	for _, t := range attempts {
		if now.Sub(t) < shareAttemptWindow {
			kept = append(kept, t)
		}
	}
	if len(kept) >= shareAttemptMax {
		s.shareAttempts[shareID] = kept
		return false
	}
	s.shareAttempts[shareID] = append(kept, now)
	return true
}

// clearShareAttempts resets a share's window after a successful
// verification (the correct password holder must not inherit the
// attacker's lockout).
func (s *Service) clearShareAttempts(shareID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.shareAttempts, shareID)
}

// JoinRedeem redeems a one-time ticket, enrolls the device, and returns a
// membership session token. Tickets bind to one redemption and expire.
func (s *Service) JoinRedeem(ctx context.Context, ticket string, device DeviceInput) (roomID, sessionToken string, err error) {
	rec, err := s.repo.GetJoinTicket(ctx, hashToken(ticket))
	if err != nil {
		return "", "", err
	}
	if rec.Redeemed {
		return "", "", errors.New("join ticket already used")
	}
	if s.clock.Now().After(rec.ExpiresAt) {
		return "", "", errors.New("join ticket expired")
	}
	// Serialize with every lifecycle mutation (dissolution and transfer
	// run under s.mu) AND re-run the identity decision inside the lock:
	// two concurrent joins claiming the same previously unseen device
	// both observe GetDevice-missing before the lock, and the later
	// UpsertDevice would otherwise overwrite the first joiner's
	// Ed25519 key without proof, breaking the immutable-identity
	// contract. The lookup/proof decision therefore lives under the
	// same serialization; it also keeps a dissolution committing between
	// ticket consumption and the membership insert from letting this
	// insert recreate a member of a dissolved room whose token still
	// authenticates routes/CAS/tunnels after the room ended.
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, err := s.repo.GetDevice(ctx, device.ID); err == nil {
		if device.Proof == "" || existing.PublicKeyPEM == "" {
			return "", "", errors.New("device identity proof required")
		}
		if !VerifyDeviceProof(existing.PublicKeyPEM, ticket, device.Proof) {
			return "", "", errors.New("device identity proof failed")
		}
		// The claimed key must be the enrolled key; a new key cannot ride
		// an existing device id.
		device.PublicKey = existing.PublicKeyPEM
	}
	room, err := s.repo.GetRoom(ctx, rec.RoomID)
	if err != nil {
		return "", "", err
	}
	if room.DissolvedAt != nil {
		return "", "", errors.New("room already dissolved")
	}
	// Canonical .tutti hosts are "<label>.<slug>.tutti": two members
	// whose hostnames normalize to the same slug would mint identical
	// hosts for the same label+port, and the gateway's listener map
	// would silently route connections to the wrong member. Reject the
	// collision at enrollment — the joining device retries after
	// changing its machine name.
	slug := vmprotocol.SlugifyHostname(device.Hostname)
	members, err := s.repo.ListMemberships(ctx, room.ID)
	if err != nil {
		return "", "", err
	}
	for _, m := range members {
		if m.DeviceID == device.ID {
			continue // rejoin of the same device is fine
		}
		if other, err := s.repo.GetDevice(ctx, m.DeviceID); err == nil &&
			vmprotocol.SlugifyHostname(other.Hostname) == slug {
			return "", "", errors.New("device hostname conflicts with an existing member")
		}
	}
	token, tokenHash, err := s.tokens.mint(room.ID, device.ID)
	if err != nil {
		return "", "", err
	}
	// Every fallible step ran: consume the ticket LAST so a failed
	// enrollment (bad proof, dissolved room, mint failure) leaves the
	// one-time ticket usable by the corrected retry instead of forcing
	// the user back through the password flow. The UPDATE is an atomic
	// compare-and-set, so a concurrent double redemption still loses.
	if err := s.repo.MarkTicketRedeemed(ctx, rec.Hash); err != nil {
		return "", "", err
	}
	if err := s.upsertDevice(ctx, device); err != nil {
		return "", "", err
	}
	now := s.clock.Now()
	if _, err := s.repo.GetMembership(ctx, room.ID, device.ID); errors.Is(err, store.ErrNotFound) {
		if err := s.repo.UpsertMembership(ctx, store.Membership{
			RoomID: room.ID, DeviceID: device.ID, JoinedAt: now, LastSeenAt: now,
			SessionTokenHash: tokenHash,
		}); err != nil {
			return "", "", err
		}
	} else if err == nil {
		// Rejoining device: refresh its session token.
		m, _ := s.repo.GetMembership(ctx, room.ID, device.ID)
		m.SessionTokenHash = tokenHash
		m.LastSeenAt = now
		if err := s.repo.UpsertMembership(ctx, m); err != nil {
			return "", "", err
		}
	} else {
		return "", "", err
	}
	return room.ID, token, nil
}

// LeaveInput controls the exit path. Owners must confirm the automatic
// Apply-to-Workspace ran and must either disband or finish a transfer.
type LeaveInput struct {
	RoomID           string `json:"-"`
	DeviceID         string `json:"-"`
	WorkspaceApplied bool   `json:"workspace_applied"`
	Disband          bool   `json:"disband"`
}

// Leave removes a participant, or ends/transfers ownership for the owner.
func (s *Service) Leave(ctx context.Context, in LeaveInput) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	room, _, err := s.authorizeMember(ctx, in.RoomID, in.DeviceID)
	if err != nil {
		return err
	}

	if room.OwnerDeviceID == in.DeviceID {
		if !in.WorkspaceApplied {
			return ErrOwnerMustApply
		}
		if in.Disband {
			if err := s.repo.DeleteMembership(ctx, in.RoomID, in.DeviceID); err != nil {
				return err
			}
			s.broadcast(in.RoomID, vmprotocol.Event{
				Topic:   vmprotocol.TopicPresence,
				RoomID:  in.RoomID,
				Payload: mustJSON(vmprotocol.PresenceDevice{DeviceID: in.DeviceID, Online: false}),
			})
			return s.dissolveLocked(ctx, in.RoomID)
		}
		// Without disband, the owner may only leave after a completed
		// transfer moved ownership away.
		fresh, err := s.repo.GetRoom(ctx, in.RoomID)
		if err != nil {
			return err
		}
		if fresh.OwnerDeviceID == in.DeviceID {
			return ErrOwnerMustDisbandOrTransfer
		}
	}

	if err := s.repo.DeleteMembership(ctx, in.RoomID, in.DeviceID); err != nil {
		return err
	}
	s.broadcast(in.RoomID, vmprotocol.Event{
		Topic:   vmprotocol.TopicPresence,
		RoomID:  in.RoomID,
		Payload: mustJSON(vmprotocol.PresenceDevice{DeviceID: in.DeviceID, Online: false}),
	})

	// The meeting ends when the last member leaves.
	members, err := s.repo.ListMemberships(ctx, in.RoomID)
	if err != nil {
		return err
	}
	if len(members) == 0 {
		return s.dissolveLocked(ctx, in.RoomID)
	}
	return nil
}

// PrepareTransfer opens phase 1 of the ownership transfer: the room records
// the candidate and a fresh snapshot is taken so the candidate can complete
// a full replica.
func (s *Service) PrepareTransfer(ctx context.Context, roomID, ownerDeviceID, candidateDeviceID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	room, _, err := s.authorizeOwnerOf(ctx, roomID, ownerDeviceID)
	if err != nil {
		return err
	}
	if _, err := s.repo.GetMembership(ctx, roomID, candidateDeviceID); err != nil {
		return errors.New("transfer candidate is not a room member")
	}
	room.PendingTransferToDevice = candidateDeviceID
	return s.repo.UpdateRoom(ctx, room)
}

// CommitTransfer finishes the transfer once the candidate reports a full
// replica and an initialized host workspace (phases 1–2 done client-side).
func (s *Service) CommitTransfer(ctx context.Context, roomID, ownerDeviceID, candidateDeviceID string, replicaFull, workspaceInitialized bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	room, _, err := s.authorizeOwnerOf(ctx, roomID, ownerDeviceID)
	if err != nil {
		return err
	}
	if room.PendingTransferToDevice != candidateDeviceID {
		return ErrTransferIncomplete
	}
	if !replicaFull || !workspaceInitialized {
		return ErrTransferIncomplete
	}
	// Revalidate at commit time: the prepared candidate may have left or
	// been kicked between phases, and assigning ownership to a
	// non-member would leave the room with an absent owner until grace
	// handling intervenes. The caller-supplied readiness booleans say
	// nothing about membership.
	if _, err := s.repo.GetMembership(ctx, roomID, candidateDeviceID); err != nil {
		return errors.New("transfer candidate is no longer a room member")
	}
	room.OwnerDeviceID = candidateDeviceID
	room.PendingTransferToDevice = ""
	if err := s.repo.UpdateRoom(ctx, room); err != nil {
		return err
	}
	s.broadcast(roomID, vmprotocol.Event{
		Topic: vmprotocol.TopicPresence, RoomID: roomID,
		Payload: mustJSON(vmprotocol.PresenceDevice{DeviceID: candidateDeviceID, Online: true, IsOwner: true}),
	})
	return nil
}

// AbortTransfer cancels an in-flight transfer; the original owner keeps the
// role.
func (s *Service) AbortTransfer(ctx context.Context, roomID, ownerDeviceID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	room, _, err := s.authorizeOwnerOf(ctx, roomID, ownerDeviceID)
	if err != nil {
		return err
	}
	room.PendingTransferToDevice = ""
	return s.repo.UpdateRoom(ctx, room)
}

// MarkOnline records a realtime connection. The first connection time of
// the current presence session decides grace-period succession order.
// ReportReplicaPolicy records a member's self-reported replica policy:
// automatic succession promotes only full replicas (owner-survival).
func (s *Service) ReportReplicaPolicy(ctx context.Context, roomID, deviceID, policy string) error {
	switch policy {
	case "full", "lazy":
	default:
		return errors.New("policy must be full or lazy")
	}
	if _, err := s.repo.GetMembership(ctx, roomID, deviceID); err != nil {
		return err
	}
	return s.repo.UpdateMembershipPolicy(ctx, roomID, deviceID, policy)
}

func (s *Service) MarkOnline(ctx context.Context, roomID, deviceID string) error {
	// Presence columns only: this runs on every heartbeat ping, and a
	// full-membership write would (a) clobber a token refresh that
	// committed since the read and (b) reset ConnectedAt, breaking
	// longest-connected succession. The store sets connected_at only on
	// the offline→online transition.
	if _, err := s.repo.GetMembership(ctx, roomID, deviceID); err != nil {
		return err
	}
	return s.repo.UpdatePresence(ctx, roomID, deviceID, true, s.clock.Now())
}

// MarkOffline records a disconnect. unexpected=false means an intentional
// leave handled through Leave.
func (s *Service) MarkOffline(ctx context.Context, roomID, deviceID string) (ownerLost bool, err error) {
	if _, err := s.repo.GetMembership(ctx, roomID, deviceID); err != nil {
		return false, err
	}
	// Presence columns only: a full-membership write from this
	// disconnect path could clobber a session-token refresh that
	// committed after the read (resurrecting the old credential).
	if err := s.repo.UpdatePresence(ctx, roomID, deviceID, false, s.clock.Now()); err != nil {
		return false, err
	}
	room, err := s.repo.GetRoom(ctx, roomID)
	if err != nil {
		return false, err
	}
	return room.OwnerDeviceID == deviceID, nil
}

// CheckGracePeriods runs the owner-disconnect state machine for one room:
// after the grace period an absent owner loses the role to the participant
// with the longest continuous presence; with nobody online the room
// dissolves immediately. Called by a background ticker and by tests. The
// returned dissolved flag tells the caller to run terminal teardown
// (sockets, tunnels, registries) — the same set the HTTP leave path runs.
func (s *Service) CheckGracePeriods(ctx context.Context, roomID string) (dissolved bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	room, err := s.repo.GetRoom(ctx, roomID)
	if err != nil || room.DissolvedAt != nil {
		return false, err
	}
	members, err := s.repo.ListMemberships(ctx, roomID)
	if err != nil {
		return false, err
	}

	var ownerOnline bool
	var online []store.Membership
	for _, m := range members {
		if !m.Online {
			continue
		}
		online = append(online, m)
		if m.DeviceID == room.OwnerDeviceID {
			ownerOnline = true
		}
	}
	if ownerOnline {
		return false, nil
	}

	// Owner gone without a graceful leave: the grace clock starts from the
	// owner's last seen time.
	for _, m := range members {
		if m.DeviceID != room.OwnerDeviceID {
			continue
		}
		if s.clock.Now().Sub(m.LastSeenAt) < s.cfg.OwnerGracePeriod {
			// Still inside the grace window: room continues, disband and
			// transfer stay locked by PrepareTransfer's owner check.
			return false, nil
		}
		break
	}

	if len(online) == 0 {
		// Nobody is online: the meeting ends immediately.
		if err := s.dissolveLocked(ctx, roomID); err != nil {
			return false, err
		}
		return true, nil
	}
	// Automatic succession needs a FULL replica, like explicit transfer:
	// promoting a lazy default would hand ownership to a device whose
	// snapshot-backed blobs exist nowhere after a server failure,
	// breaking the owner-survival contract. Prefer the longest
	// continuously connected FULL replica; with none online the room
	// waits (members can still run an explicit transfer, whose
	// readiness phase materializes the candidate) and re-checks here
	// on the next cycle.
	full := make([]store.Membership, 0, len(online))
	for _, m := range online {
		if m.ReplicaPolicy == "full" {
			full = append(full, m)
		}
	}
	if len(full) == 0 {
		// Members learn the room is leaderless: an explicit transfer
		// (with its readiness phase) is the way out until someone
		// reports a full replica.
		s.broadcast(roomID, vmprotocol.Event{
			Topic: vmprotocol.TopicOwnerLost, RoomID: roomID,
			Payload: []byte(`{"reason":"owner lost; full-replica successor required"}`),
		})
		return false, nil
	}
	online = full
	// Longest continuous presence wins: earliest ConnectedAt of the current
	// presence session, not the earliest join.
	sort.Slice(online, func(i, j int) bool {
		a, b := online[i].ConnectedAt, online[j].ConnectedAt
		if a == nil {
			return false
		}
		if b == nil {
			return true
		}
		return a.Before(*b)
	})
	room.OwnerDeviceID = online[0].DeviceID
	room.PendingTransferToDevice = ""
	if err := s.repo.UpdateRoom(ctx, room); err != nil {
		return false, err
	}
	s.broadcast(roomID, vmprotocol.Event{
		Topic: vmprotocol.TopicPresence, RoomID: roomID,
		Payload: mustJSON(vmprotocol.PresenceDevice{DeviceID: online[0].DeviceID, Online: true, IsOwner: true}),
	})
	return false, nil
}

// Dissolve ends the room: share invalid, memberships and tickets deleted,
// CAS references released for garbage collection.
func (s *Service) Dissolve(ctx context.Context, roomID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dissolveLocked(ctx, roomID)
}

func (s *Service) dissolveLocked(ctx context.Context, roomID string) error {
	room, err := s.repo.GetRoom(ctx, roomID)
	if err != nil {
		return err
	}
	if room.DissolvedAt != nil {
		return nil
	}
	s.broadcast(roomID, vmprotocol.Event{
		Topic: vmprotocol.TopicRoomEnding, RoomID: roomID,
		Payload: mustJSON(map[string]string{"reason": "dissolved"}),
	})
	return s.repo.DissolveRoom(ctx, roomID, s.clock.Now())
}

// DissolveAllActive ends every active room; called at startup because the
// server never restores rooms across restarts.
func (s *Service) DissolveAllActive(ctx context.Context) error {
	rooms, err := s.repo.ListActiveRooms(ctx)
	if err != nil {
		return err
	}
	for _, room := range rooms {
		if err := s.Dissolve(ctx, room.ID); err != nil {
			return err
		}
	}
	return nil
}

// ValidateSessionToken authenticates a session token and returns the room
// and device it authorizes.
func (s *Service) ValidateSessionToken(ctx context.Context, token string) (roomID, deviceID string, err error) {
	return s.tokens.validate(ctx, s.repo, token)
}

// authorizeOwnerOf ensures the device is the active room owner.
func (s *Service) authorizeOwnerOf(ctx context.Context, roomID, deviceID string) (store.Room, store.Membership, error) {
	room, m, err := s.authorizeMember(ctx, roomID, deviceID)
	if err != nil {
		return room, m, err
	}
	if room.OwnerDeviceID != deviceID {
		return room, m, fmt.Errorf("%w: not the room owner", store.ErrNotFound)
	}
	return room, m, nil
}

// authorizeMember ensures the device is an active member of an active room.
func (s *Service) authorizeMember(ctx context.Context, roomID, deviceID string) (store.Room, store.Membership, error) {
	room, err := s.repo.GetRoom(ctx, roomID)
	if err != nil {
		return room, store.Membership{}, err
	}
	if room.DissolvedAt != nil {
		return room, store.Membership{}, errors.New("room already dissolved")
	}
	m, err := s.repo.GetMembership(ctx, roomID, deviceID)
	if err != nil {
		return room, m, err
	}
	return room, m, nil
}

func (s *Service) upsertDevice(ctx context.Context, in DeviceInput) error {
	if in.ID == "" || in.DisplayName == "" || in.PublicKey == "" {
		return errors.New("device id, display name, and public key are required")
	}
	// Session tokens are dot-delimited (room.device.nonce.signature):
	// a dot inside a device id would mint a token neither the server
	// nor Client.AdoptToken can ever parse — reject the id BEFORE any
	// state persists (room creation and join both pass through here).
	if !validDeviceID(in.ID) {
		return errors.New("device id must be 1-64 characters of letters, digits, underscores, or hyphens")
	}
	if in.Hostname == "" {
		in.Hostname = "device"
	}
	return s.repo.UpsertDevice(ctx, store.Device{
		ID: in.ID, DisplayName: in.DisplayName, Hostname: in.Hostname,
		PublicKeyPEM: in.PublicKey, FirstSeenAt: s.clock.Now(),
	})
}

// randomToken returns URL-safe base32 randomness.
func randomToken(nbytes int) string {
	b := make([]byte, nbytes)
	if _, err := rand.Read(b); err != nil {
		panic(err) // crypto/rand failure is unrecoverable
	}
	return strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b))
}

// sixDigitPassword generates the auto-created room password.
func sixDigitPassword() string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	v := uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
	return fmt.Sprintf("%06d", v%1000000)
}

// hashToken is the storage hash for tickets and session tokens.
func hashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func mustJSON(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

// tokenMinter mints and validates HMAC-signed membership session tokens.
type tokenMinter struct {
	key []byte
}

func newTokenMinter(secret string) *tokenMinter {
	sum := sha256.Sum256([]byte("open-tutti-session:" + secret))
	return &tokenMinter{key: sum[:]}
}

// validDeviceID gates enrollment: session tokens are dot-delimited, so
// device ids may not contain dots (or anything else that breaks the
// four-field format).
func validDeviceID(id string) bool {
	if len(id) == 0 || len(id) > 64 {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '_' || c == '-':
		default:
			return false
		}
	}
	return true
}

func (t *tokenMinter) mint(roomID, deviceID string) (token, hash string, err error) {
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return "", "", err
	}
	payload := roomID + "." + deviceID + "." + base64.RawURLEncoding.EncodeToString(nonce)
	sig := hex.EncodeToString(hmacSHA256(t.key, payload))
	token = payload + "." + sig
	return token, hashToken(token), nil
}

func (t *tokenMinter) validate(ctx context.Context, repo store.Repository, token string) (roomID, deviceID string, err error) {
	parts := strings.Split(token, ".")
	if len(parts) != 4 {
		return "", "", errors.New("malformed session token")
	}
	payload := strings.Join(parts[:3], ".")
	want := hmacSHA256(t.key, payload)
	got, err := hex.DecodeString(parts[3])
	if err != nil || subtle.ConstantTimeCompare(want, got) != 1 {
		return "", "", errors.New("invalid session token signature")
	}
	roomID, deviceID = parts[0], parts[1]
	m, err := repo.GetMembership(ctx, roomID, deviceID)
	if err != nil {
		return "", "", err
	}
	if subtle.ConstantTimeCompare([]byte(hashToken(token)), []byte(m.SessionTokenHash)) != 1 {
		return "", "", errors.New("stale session token")
	}
	return roomID, deviceID, nil
}
