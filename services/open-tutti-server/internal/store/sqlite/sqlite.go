// Package sqlite is the v1 Repository implementation over SQLite
// (modernc.org/sqlite, pure Go, platform-neutral).
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite" // registers the "sqlite" driver

	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/store"
)

// Open opens (creating if needed) the repository database and applies the
// schema.
func Open(path string) (*Repo, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)")
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1) // modernc sqlite is happiest single-writer
	if err := applySchema(db); err != nil {
		db.Close()
		return nil, err
	}
	return &Repo{db: db}, nil
}

// Repo implements store.Repository.
type Repo struct{ db *sql.DB }

func applySchema(db *sql.DB) error {
	const schema = `
CREATE TABLE IF NOT EXISTS rooms (
	id TEXT PRIMARY KEY,
	share_id TEXT NOT NULL UNIQUE,
	password_hash TEXT NOT NULL,
	owner_device_id TEXT NOT NULL,
	created_at INTEGER NOT NULL,
	dissolved_at INTEGER,
	pending_transfer_to_device TEXT NOT NULL DEFAULT '',
	share_revoked_at INTEGER
);
CREATE TABLE IF NOT EXISTS devices (
	id TEXT PRIMARY KEY,
	display_name TEXT NOT NULL,
	hostname TEXT NOT NULL,
	public_key_pem TEXT NOT NULL,
	first_seen_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS memberships (
	room_id TEXT NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
	device_id TEXT NOT NULL REFERENCES devices(id),
	joined_at INTEGER NOT NULL,
	connected_at INTEGER,
	last_seen_at INTEGER NOT NULL,
	online INTEGER NOT NULL DEFAULT 0,
	session_token_hash TEXT NOT NULL DEFAULT '',
	replica_policy TEXT NOT NULL DEFAULT 'lazy',
	PRIMARY KEY (room_id, device_id)
);
CREATE TABLE IF NOT EXISTS join_tickets (
	hash TEXT PRIMARY KEY,
	room_id TEXT NOT NULL,
	share_id TEXT NOT NULL,
	expires_at INTEGER NOT NULL,
	redeemed INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS snapshots (
	room_id TEXT NOT NULL,
	server_seq INTEGER NOT NULL,
	root_tree_hash TEXT NOT NULL,
	entries_json BLOB NOT NULL,
	reason TEXT NOT NULL,
	created_at INTEGER NOT NULL,
	PRIMARY KEY (room_id, server_seq)
);
CREATE TABLE IF NOT EXISTS cas_refs (
	room_id TEXT NOT NULL,
	hash TEXT NOT NULL,
	PRIMARY KEY (room_id, hash)
);
CREATE INDEX IF NOT EXISTS idx_memberships_room ON memberships(room_id);
CREATE INDEX IF NOT EXISTS idx_cas_refs_hash ON cas_refs(hash);
`
	_, err := db.Exec(schema)
	if err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	// Databases created before share revocation existed gain the column;
	// fresh ones already have it.
	if _, err := db.Exec(`ALTER TABLE rooms ADD COLUMN share_revoked_at INTEGER`); err != nil {
		if !strings.Contains(err.Error(), "duplicate column") {
			return fmt.Errorf("migrate share_revoked_at: %w", err)
		}
	}
	// Databases created before replica-policy reporting existed: every
	// member reads as lazy until it says otherwise.
	if _, err := db.Exec(`ALTER TABLE memberships ADD COLUMN replica_policy TEXT NOT NULL DEFAULT 'lazy'`); err != nil {
		if !strings.Contains(err.Error(), "duplicate column") {
			return fmt.Errorf("migrate replica_policy: %w", err)
		}
	}
	return nil
}

// UpdateMembershipPolicy records one member's replica policy ("full" or
// "lazy"): automatic succession must only hand ownership to a device
// that actually keeps the full replica, per the owner-survival contract.
func (r *Repo) UpdateMembershipPolicy(ctx context.Context, roomID, deviceID, policy string) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE memberships SET replica_policy=? WHERE room_id=? AND device_id=?`,
		policy, roomID, deviceID)
	return err
}

// Close closes the database.
func (r *Repo) Close() error { return r.db.Close() }

const roomCols = "id, share_id, password_hash, owner_device_id, created_at, dissolved_at, pending_transfer_to_device, share_revoked_at"

func scanRoom(row interface{ Scan(...any) error }) (store.Room, error) {
	var room store.Room
	var created int64
	var dissolved sql.NullInt64
	var shareRevoked sql.NullInt64
	err := row.Scan(&room.ID, &room.ShareID, &room.PasswordHash, &room.OwnerDeviceID,
		&created, &dissolved, &room.PendingTransferToDevice, &shareRevoked)
	if errors.Is(err, sql.ErrNoRows) {
		return store.Room{}, store.ErrNotFound
	}
	if err != nil {
		return store.Room{}, err
	}
	room.CreatedAt = time.Unix(created, 0).UTC()
	if dissolved.Valid {
		t := time.Unix(dissolved.Int64, 0).UTC()
		room.DissolvedAt = &t
	}
	if shareRevoked.Valid {
		t := time.Unix(shareRevoked.Int64, 0).UTC()
		room.ShareRevokedAt = &t
	}
	return room, nil
}

func (r *Repo) CreateRoom(ctx context.Context, room store.Room) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO rooms (`+roomCols+`) VALUES (?,?,?,?,?,?,?,?)`,
		room.ID, room.ShareID, room.PasswordHash, room.OwnerDeviceID,
		room.CreatedAt.Unix(), nilTime(room.DissolvedAt), room.PendingTransferToDevice,
		nilTime(room.ShareRevokedAt))
	return err
}

func (r *Repo) GetRoom(ctx context.Context, id string) (store.Room, error) {
	return scanRoom(r.db.QueryRowContext(ctx, `SELECT `+roomCols+` FROM rooms WHERE id = ?`, id))
}

func (r *Repo) GetRoomByShareID(ctx context.Context, shareID string) (store.Room, error) {
	return scanRoom(r.db.QueryRowContext(ctx, `SELECT `+roomCols+` FROM rooms WHERE share_id = ? AND dissolved_at IS NULL`, shareID))
}

// UpdateRoomShareRevoked stamps ONLY the share-revocation timestamp:
// a full-record room update from a revocation racing a transfer or
// dissolution would write the stale owner and lifecycle fields back.
func (r *Repo) UpdateRoomShareRevoked(ctx context.Context, roomID string, revokedAt time.Time) error {
	// Unix seconds like every other INTEGER timestamp: passing the
	// time.Time stores a string, which then fails the INTEGER scan on
	// every later GetRoom.
	_, err := r.db.ExecContext(ctx,
		`UPDATE rooms SET share_revoked_at=? WHERE id=?`, revokedAt.Unix(), roomID)
	return err
}

// UpdatePresence touches ONLY presence columns: full-membership writes
// from disconnect paths raced token refreshes (resurrecting the old
// credential) and heartbeat pings reset ConnectedAt (breaking
// longest-connected succession). connected_at is set only on an
// offline→online transition.
func (r *Repo) UpdatePresence(ctx context.Context, roomID, deviceID string, online bool, now time.Time) error {
	onlineInt := 0
	if online {
		onlineInt = 1
	}
	// Timestamps are unix seconds, like every other membership write —
	// binding time.Time directly stores a string the INTEGER columns'
	// readers cannot scan.
	_, err := r.db.ExecContext(ctx,
		`UPDATE memberships SET
			connected_at = CASE WHEN ? AND online=0 THEN ? ELSE connected_at END,
			online = ?,
			last_seen_at = ?
		WHERE room_id=? AND device_id=?`,
		onlineInt, now.Unix(), onlineInt, now.Unix(), roomID, deviceID)
	return err
}

// UpdateRoomPassword changes ONLY the password hash: a full-record
// room update from a rotation racing a transfer/dissolution would write
// the stale owner and lifecycle fields back over the newer state.
func (r *Repo) UpdateRoomPassword(ctx context.Context, roomID, passwordHash string) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE rooms SET password_hash=? WHERE id=?`, passwordHash, roomID)
	return err
}

func (r *Repo) UpdateRoom(ctx context.Context, room store.Room) error {
	res, err := r.db.ExecContext(ctx,
		`UPDATE rooms SET password_hash=?, owner_device_id=?, dissolved_at=?, pending_transfer_to_device=?, share_revoked_at=? WHERE id=?`,
		room.PasswordHash, room.OwnerDeviceID, nilTime(room.DissolvedAt), room.PendingTransferToDevice,
		nilTime(room.ShareRevokedAt), room.ID)
	if err != nil {
		return err
	}
	return requireUpdated(res)
}

func (r *Repo) ListActiveRooms(ctx context.Context) ([]store.Room, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT `+roomCols+` FROM rooms WHERE dissolved_at IS NULL`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.Room
	for rows.Next() {
		room, err := scanRoom(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, room)
	}
	return out, rows.Err()
}

func nilTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.Unix()
}

func requireUpdated(res sql.Result) error {
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return store.ErrNotFound
	}
	return nil
}

func (r *Repo) UpsertDevice(ctx context.Context, d store.Device) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO devices (id, display_name, hostname, public_key_pem, first_seen_at)
		 VALUES (?,?,?,?,?)
		 ON CONFLICT(id) DO UPDATE SET display_name=excluded.display_name, hostname=excluded.hostname, public_key_pem=excluded.public_key_pem`,
		d.ID, d.DisplayName, d.Hostname, d.PublicKeyPEM, d.FirstSeenAt.Unix())
	return err
}

func (r *Repo) GetDevice(ctx context.Context, id string) (store.Device, error) {
	var d store.Device
	var firstSeen int64
	err := r.db.QueryRowContext(ctx,
		`SELECT id, display_name, hostname, public_key_pem, first_seen_at FROM devices WHERE id=?`, id).
		Scan(&d.ID, &d.DisplayName, &d.Hostname, &d.PublicKeyPEM, &firstSeen)
	if errors.Is(err, sql.ErrNoRows) {
		return store.Device{}, store.ErrNotFound
	}
	if err != nil {
		return store.Device{}, err
	}
	d.FirstSeenAt = time.Unix(firstSeen, 0).UTC()
	return d, nil
}

const membershipCols = "room_id, device_id, joined_at, connected_at, last_seen_at, online, session_token_hash, replica_policy"

func scanMembership(row interface{ Scan(...any) error }) (store.Membership, error) {
	var m store.Membership
	var connected sql.NullInt64
	var joined, lastSeen int64
	var online int
	err := row.Scan(&m.RoomID, &m.DeviceID, &joined, &connected, &lastSeen, &online, &m.SessionTokenHash, &m.ReplicaPolicy)
	if errors.Is(err, sql.ErrNoRows) {
		return store.Membership{}, store.ErrNotFound
	}
	if err != nil {
		return store.Membership{}, err
	}
	m.Online = online != 0
	if connected.Valid {
		t := time.Unix(connected.Int64, 0).UTC()
		m.ConnectedAt = &t
	}
	m.JoinedAt = time.Unix(joined, 0).UTC()
	m.LastSeenAt = time.Unix(lastSeen, 0).UTC()
	if m.ReplicaPolicy == "" {
		m.ReplicaPolicy = "lazy"
	}
	return m, nil
}

func (r *Repo) UpsertMembership(ctx context.Context, m store.Membership) error {
	var connected any
	if m.ConnectedAt != nil {
		connected = m.ConnectedAt.Unix()
	}
	online := 0
	if m.Online {
		online = 1
	}
	policy := m.ReplicaPolicy
	if policy == "" {
		policy = "lazy"
	}
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO memberships (`+membershipCols+`) VALUES (?,?,?,?,?,?,?,?)
		 ON CONFLICT(room_id, device_id) DO UPDATE SET
		   connected_at=excluded.connected_at,
		   last_seen_at=excluded.last_seen_at,
		   online=excluded.online,
		   session_token_hash=excluded.session_token_hash,
		   replica_policy=excluded.replica_policy`,
		m.RoomID, m.DeviceID, m.JoinedAt.Unix(), connected, m.LastSeenAt.Unix(), online, m.SessionTokenHash, policy)
	return err
}

func (r *Repo) GetMembership(ctx context.Context, roomID, deviceID string) (store.Membership, error) {
	return scanMembership(r.db.QueryRowContext(ctx,
		`SELECT `+membershipCols+` FROM memberships WHERE room_id=? AND device_id=?`, roomID, deviceID))
}

func (r *Repo) ListMemberships(ctx context.Context, roomID string) ([]store.Membership, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT `+membershipCols+` FROM memberships WHERE room_id=? ORDER BY joined_at`, roomID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.Membership
	for rows.Next() {
		m, err := scanMembership(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func (r *Repo) DeleteMembership(ctx context.Context, roomID, deviceID string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM memberships WHERE room_id=? AND device_id=?`, roomID, deviceID)
	return err
}

func (r *Repo) DeleteRoomMemberships(ctx context.Context, roomID string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM memberships WHERE room_id=?`, roomID)
	return err
}

func (r *Repo) CreateJoinTicket(ctx context.Context, t store.JoinTicket) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO join_tickets (hash, room_id, share_id, expires_at, redeemed) VALUES (?,?,?,?,0)`,
		t.Hash, t.RoomID, t.ShareID, t.ExpiresAt.Unix())
	return err
}

func (r *Repo) GetJoinTicket(ctx context.Context, hash string) (store.JoinTicket, error) {
	var t store.JoinTicket
	var redeemed int
	var expires int64
	err := r.db.QueryRowContext(ctx,
		`SELECT hash, room_id, share_id, expires_at, redeemed FROM join_tickets WHERE hash=?`, hash).
		Scan(&t.Hash, &t.RoomID, &t.ShareID, &expires, &redeemed)
	if errors.Is(err, sql.ErrNoRows) {
		return store.JoinTicket{}, store.ErrNotFound
	}
	if err != nil {
		return store.JoinTicket{}, err
	}
	t.ExpiresAt = time.Unix(expires, 0).UTC()
	t.Redeemed = redeemed != 0
	return t, nil
}

func (r *Repo) MarkTicketRedeemed(ctx context.Context, hash string) error {
	res, err := r.db.ExecContext(ctx, `UPDATE join_tickets SET redeemed=1 WHERE hash=? AND redeemed=0`, hash)
	if err != nil {
		return err
	}
	return requireUpdated(res)
}

func (r *Repo) SaveSnapshot(ctx context.Context, s store.SnapshotRecord) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO snapshots (room_id, server_seq, root_tree_hash, entries_json, reason, created_at)
		 VALUES (?,?,?,?,?,?)`,
		s.RoomID, s.ServerSeq, s.RootTreeHash, s.EntriesJSON, s.Reason, s.CreatedAt.Unix())
	return err
}

func (r *Repo) LatestSnapshot(ctx context.Context, roomID string) (store.SnapshotRecord, error) {
	var s store.SnapshotRecord
	var created int64
	err := r.db.QueryRowContext(ctx,
		`SELECT room_id, server_seq, root_tree_hash, entries_json, reason, created_at
		 FROM snapshots WHERE room_id=? ORDER BY server_seq DESC LIMIT 1`, roomID).
		Scan(&s.RoomID, &s.ServerSeq, &s.RootTreeHash, &s.EntriesJSON, &s.Reason, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return store.SnapshotRecord{}, store.ErrNotFound
	}
	if err != nil {
		return store.SnapshotRecord{}, err
	}
	s.CreatedAt = time.Unix(created, 0).UTC()
	return s, nil
}

func (r *Repo) AddCASRefs(ctx context.Context, roomID string, hashes []string) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `INSERT OR IGNORE INTO cas_refs (room_id, hash) VALUES (?,?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, h := range hashes {
		if _, err := stmt.ExecContext(ctx, roomID, h); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// HasCASRef reports whether the room references the object hash.
func (r *Repo) HasCASRef(ctx context.Context, roomID, hash string) (bool, error) {
	var one int
	err := r.db.QueryRowContext(ctx,
		`SELECT 1 FROM cas_refs WHERE room_id=? AND hash=? LIMIT 1`, roomID, hash).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// CollectUnreferencedCAS deletes the caller-provided objects whose last
// reference died, running the refcount check AND the deletion callback
// inside ONE write transaction: a concurrent AddCASRefs waits on the
// write lock and only commits afterwards, so it re-validates against
// the post-collection refs and can never end up referencing a deleted
// object. A failed deletion callback merely leaks the object (safe).
func (r *Repo) CollectUnreferencedCAS(ctx context.Context, hashes []string, del func(hash string) error) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, h := range hashes {
		var n int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM cas_refs WHERE hash=?`, h).Scan(&n); err != nil {
			return err
		}
		if n == 0 && del != nil {
			// Leak on failure is safe: a present-but-unreferenced object
			// only costs disk; a deleted-but-referenced one corrupts.
			_ = del(h)
		}
	}
	return tx.Commit()
}

// RoomCASRefs lists the object hashes one room references (collection
// input at dissolution).
func (r *Repo) RoomCASRefs(ctx context.Context, roomID string) ([]string, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT hash FROM cas_refs WHERE room_id=?`, roomID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var h string
		if err := rows.Scan(&h); err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

func (r *Repo) ListCASRefCounts(ctx context.Context) (map[string]int, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT hash, COUNT(*) FROM cas_refs GROUP BY hash`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var h string
		var n int
		if err := rows.Scan(&h, &n); err != nil {
			return nil, err
		}
		out[h] = n
	}
	return out, rows.Err()
}

func (r *Repo) DeleteRoomCASRefs(ctx context.Context, roomID string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM cas_refs WHERE room_id=?`, roomID)
	return err
}

func (r *Repo) DissolveRoom(ctx context.Context, roomID string, at time.Time) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE rooms SET dissolved_at=?, pending_transfer_to_device='' WHERE id=? AND dissolved_at IS NULL`, at.Unix(), roomID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM memberships WHERE room_id=?`, roomID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM join_tickets WHERE room_id=?`, roomID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM cas_refs WHERE room_id=?`, roomID); err != nil {
		return err
	}
	return tx.Commit()
}
