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
	PendingTransferToDevice string
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

// Repository is the durable metadata store.
type Repository interface {
	// Rooms
	CreateRoom(ctx context.Context, room Room) error
	GetRoom(ctx context.Context, id string) (Room, error)
	GetRoomByShareID(ctx context.Context, shareID string) (Room, error)
	UpdateRoom(ctx context.Context, room Room) error
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
	ListCASRefCounts(ctx context.Context) (map[string]int, error)
	DeleteRoomCASRefs(ctx context.Context, roomID string) error
	// HasCASRef reports whether the room references the object hash;
	// CAS reads are authorized per room, not by global object existence.
	HasCASRef(ctx context.Context, roomID, hash string) (bool, error)

	// Dissolution cleanup
	DissolveRoom(ctx context.Context, roomID string, at time.Time) error
}
