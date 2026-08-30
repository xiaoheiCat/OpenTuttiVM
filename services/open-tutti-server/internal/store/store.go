// Package store defines the repository contracts of open-tutti-server.
// Business logic depends on these interfaces only; the v1 implementation is
// SQLite in internal/store/sqlite, and a future PostgreSQL adapter can be
// swapped in without touching lifecycle semantics.
package store

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is returned for missing rows.
var ErrNotFound = errors.New("not found")

// Room is one collaboration room. Rooms are meetings: they die when everyone
// leaves; the server never restores a room after restart.
type Room struct {
	ID           string
	ShareID      string
	PasswordHash string
	// PasswordArgon2 params travel with the hash (encoded).
	OwnerDeviceID string
	CreatedAt     time.Time
	DissolvedAt   *time.Time
	// ShareRevokedAt marks a revoked share link: no new joins, existing
	// members stay until the room ends.
	ShareRevokedAt *time.Time
	// PendingTransferToDevice marks a 3-phase ownership transfer in flight.
	PendingTransferToDevice    string
	PendingTransferGeneration  string
	PendingTransferSnapshotSeq uint64
}

// Device is one enrolled device; a device is a user.
type Device struct {
	ID          string
	DisplayName string
	Hostname    string
	// PublicKeyPEM is the device's Ed25519 public key.
	PublicKeyPEM string
	FirstSeenAt  time.Time
}

// Membership is a device's participation in a room.
type Membership struct {
	RoomID      string
	DeviceID    string
	JoinedAt    time.Time
	ConnectedAt *time.Time
	LastSeenAt  time.Time
	Online      bool
	// SessionTokenHash authorizes this membership's API and WS calls.
	SessionTokenHash string
	// ReplicaPolicy is the member's self-reported replica policy
	// ("full" or "lazy"): automatic succession only promotes members
	// keeping a full replica (owner-survival contract).
	ReplicaPolicy       string
	TransferGeneration  string
	TransferSnapshotSeq uint64
	TransferAppliedSeq  uint64
	TransferReady       bool
}

// JoinTicket is a one-time, short-lived ticket issued by the share page
// after the room password verifies. The ticket — never the password — goes
// into the deep link.
type JoinTicket struct {
	Hash      string
	RoomID    string
	ShareID   string
	ExpiresAt time.Time
	Redeemed  bool
}

// SnapshotRecord is a persisted workspace checkpoint.
type SnapshotRecord struct {
	RoomID       string
	ServerSeq    uint64
	RootTreeHash string
	EntriesJSON  []byte
	Reason       string
	CreatedAt    time.Time
}

// CASRef tracks which rooms reference which CAS objects so dissolution can
// garbage-collect unreferenced objects.
type CASRef struct {
	RoomID string
	Hash   string
}

type CASObject struct {
	Hash string
	Size int64
}

// Repository is the durable metadata store.
// ErrTicketUsed reports a lost redemption race.
var ErrTicketUsed = errors.New("join ticket already used")

type Repository interface {
	// Rooms
	CreateRoom(ctx context.Context, room Room) error
	GetRoom(ctx context.Context, id string) (Room, error)
	GetRoomByShareID(ctx context.Context, shareID string) (Room, error)
	UpdateRoom(ctx context.Context, room Room) error
	// UpdateRoomPassword changes only the password hash (rotation races
	// lifecycle transitions; a full-record update would clobber them).
	UpdateRoomPassword(ctx context.Context, roomID, passwordHash string) error
	// UpdateRoomShareRevoked stamps only the share-revocation timestamp
	// (full-record writes race lifecycle transitions).
	UpdateRoomShareRevoked(ctx context.Context, roomID string, revokedAt time.Time) error
	// UpdatePresence touches only presence columns; connected_at changes
	// solely on an offline→online transition so heartbeat pings cannot
	// reset succession order and disconnects cannot clobber token
	// refreshes.
	UpdatePresence(ctx context.Context, roomID, deviceID string, online bool, now time.Time) error
	// UpdateMembershipPolicy records a member replica policy report.
	UpdateMembershipPolicy(ctx context.Context, roomID, deviceID, policy string) error
	UpdateTransferReadiness(ctx context.Context, roomID, deviceID, generation string, snapshotSeq, appliedSeq uint64) error
	ListActiveRooms(ctx context.Context) ([]Room, error)

	// Devices
	UpsertDevice(ctx context.Context, d Device) error
	GetDevice(ctx context.Context, id string) (Device, error)

	// Memberships
	UpsertMembership(ctx context.Context, m Membership) error
	GetMembership(ctx context.Context, roomID, deviceID string) (Membership, error)
	ListMemberships(ctx context.Context, roomID string) ([]Membership, error)
	DeleteMembership(ctx context.Context, roomID, deviceID string) error
	DeleteRoomMemberships(ctx context.Context, roomID string) error

	// Join tickets
	CreateJoinTicket(ctx context.Context, t JoinTicket) error
	GetJoinTicket(ctx context.Context, hash string) (JoinTicket, error)
	MarkTicketRedeemed(ctx context.Context, hash string) error

	// Snapshots
	SaveSnapshot(ctx context.Context, s SnapshotRecord) error
	LatestSnapshot(ctx context.Context, roomID string) (SnapshotRecord, error)

	// CAS references
	AddCASRefs(ctx context.Context, roomID string, hashes []string) error
	AddCASRefsSized(ctx context.Context, roomID string, objects []CASObject, quotaBytes int64) error
	ListCASRefCounts(ctx context.Context) (map[string]int, error)
	// RoomCASRefs lists one room's referenced object hashes.
	RoomCASRefs(ctx context.Context, roomID string) ([]string, error)
	// CollectUnreferencedCAS checks global refs in a short transaction and runs
	// the deletion callback outside that transaction. The repository's CAS
	// publication fence remains held across both steps.
	CollectUnreferencedCAS(ctx context.Context, hashes []string, del func(hash string) error) error
	// CASPublication serializes a CAS object's publication (filesystem
	// write plus reference insertion) with collection.
	CASPublication(fn func() error) error
	// CreateRoomWithOwner commits device enrollment, room creation,
	// and the owner membership atomically.
	CreateRoomWithOwner(ctx context.Context, d Device, room Room, m Membership) error
	// EnrollWithTicket commits ticket consumption, device enrollment,
	// and membership upsert atomically; ErrTicketUsed marks a lost
	// redemption race.
	EnrollWithTicket(ctx context.Context, hash string, now time.Time, d Device, m Membership) error
	DeleteRoomCASRefs(ctx context.Context, roomID string) error
	// HasCASRef reports whether the room references the object hash;
	// CAS reads are authorized per room, not by global object existence.
	HasCASRef(ctx context.Context, roomID, hash string) (bool, error)
	RecordCASOrphan(ctx context.Context, hash string) error
	ListCASOrphans(ctx context.Context) ([]string, error)
	DeleteCASOrphan(ctx context.Context, hash string) error

	// Dissolution cleanup
	// DissolveRoomFenced dissolves under the CAS publication lock and
	// returns the room's refs for post-dissolution collection.
	DissolveRoomFenced(ctx context.Context, roomID string, at time.Time) ([]string, error)
	DissolveRoom(ctx context.Context, roomID string, at time.Time) error
}
