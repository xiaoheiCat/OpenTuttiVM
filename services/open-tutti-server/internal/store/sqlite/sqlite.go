// Package sqlite is the v1 Repository implementation over SQLite
// (modernc.org/sqlite, pure Go, platform-neutral).
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
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
type Repo struct {
	db *sql.DB
	// casMu serializes CAS object publication (filesystem write +
	// reference insertion) with reference collection; see
	// CASPublication and CollectUnreferencedCAS.
	casMu sync.Mutex
}

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
	pending_transfer_generation TEXT NOT NULL DEFAULT '',
	pending_transfer_snapshot_seq INTEGER NOT NULL DEFAULT 0,
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
	transfer_generation TEXT NOT NULL DEFAULT '',
	transfer_snapshot_seq INTEGER NOT NULL DEFAULT 0,
	transfer_applied_seq INTEGER NOT NULL DEFAULT 0,
	transfer_ready INTEGER NOT NULL DEFAULT 0,
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
CREATE TABLE IF NOT EXISTS cas_objects (
	 hash TEXT PRIMARY KEY,
	 size INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS cas_orphans (hash TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS cas_pending_refs (
 room_id TEXT NOT NULL,
 device_id TEXT NOT NULL,
 hash TEXT NOT NULL,
 size INTEGER NOT NULL,
 expires_at INTEGER NOT NULL,
 PRIMARY KEY (room_id, device_id, hash)
);
CREATE INDEX IF NOT EXISTS idx_memberships_room ON memberships(room_id);
CREATE INDEX IF NOT EXISTS idx_cas_refs_hash ON cas_refs(hash);
CREATE INDEX IF NOT EXISTS idx_cas_pending_expiry ON cas_pending_refs(expires_at);
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
	if _, err := db.Exec(`ALTER TABLE rooms ADD COLUMN pending_transfer_generation TEXT NOT NULL DEFAULT ''`); err != nil {
		if !strings.Contains(err.Error(), "duplicate column") {
			return fmt.Errorf("migrate pending_transfer_generation: %w", err)
		}
	}
	if _, err := db.Exec(`ALTER TABLE rooms ADD COLUMN pending_transfer_snapshot_seq INTEGER NOT NULL DEFAULT 0`); err != nil {
		if !strings.Contains(err.Error(), "duplicate column") {
			return fmt.Errorf("migrate pending_transfer_snapshot_seq: %w", err)
		}
	}
	// Databases created before replica-policy reporting existed: every
	// member reads as lazy until it says otherwise.
	if _, err := db.Exec(`ALTER TABLE memberships ADD COLUMN replica_policy TEXT NOT NULL DEFAULT 'lazy'`); err != nil {
		if !strings.Contains(err.Error(), "duplicate column") {
			return fmt.Errorf("migrate replica_policy: %w", err)
		}
	}
	for _, stmt := range []string{
		`ALTER TABLE memberships ADD COLUMN transfer_generation TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE memberships ADD COLUMN transfer_snapshot_seq INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE memberships ADD COLUMN transfer_applied_seq INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE memberships ADD COLUMN transfer_ready INTEGER NOT NULL DEFAULT 0`,
	} {
		if _, err := db.Exec(stmt); err != nil && !strings.Contains(err.Error(), "duplicate column") {
			return fmt.Errorf("migrate transfer readiness: %w", err)
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

func (r *Repo) UpdateTransferReadiness(ctx context.Context, roomID, deviceID, generation string, snapshotSeq, appliedSeq uint64) error {
	res, err := r.db.ExecContext(ctx, `UPDATE memberships SET transfer_generation=?, transfer_snapshot_seq=?, transfer_applied_seq=?, transfer_ready=1 WHERE room_id=? AND device_id=?`, generation, snapshotSeq, appliedSeq, roomID, deviceID)
	if err != nil {
		return err
	}
	return requireUpdated(res)
}

// Close closes the database.
func (r *Repo) Close() error { return r.db.Close() }

const roomCols = "id, share_id, password_hash, owner_device_id, created_at, dissolved_at, pending_transfer_to_device, pending_transfer_generation, pending_transfer_snapshot_seq, share_revoked_at"

func scanRoom(row interface{ Scan(...any) error }) (store.Room, error) {
	var room store.Room
	var created int64
	var dissolved sql.NullInt64
	var snapshotSeq uint64
	var shareRevoked sql.NullInt64
	err := row.Scan(&room.ID, &room.ShareID, &room.PasswordHash, &room.OwnerDeviceID,
		&created, &dissolved, &room.PendingTransferToDevice, &room.PendingTransferGeneration, &snapshotSeq, &shareRevoked)
	if errors.Is(err, sql.ErrNoRows) {
		return store.Room{}, store.ErrNotFound
	}
	if err != nil {
		return store.Room{}, err
	}
	room.CreatedAt = time.Unix(created, 0).UTC()
	room.PendingTransferSnapshotSeq = snapshotSeq
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
		`INSERT INTO rooms (`+roomCols+`) VALUES (?,?,?,?,?,?,?,?,?,?)`,
		room.ID, room.ShareID, room.PasswordHash, room.OwnerDeviceID,
		room.CreatedAt.Unix(), nilTime(room.DissolvedAt), room.PendingTransferToDevice, room.PendingTransferGeneration, room.PendingTransferSnapshotSeq,
		nilTime(room.ShareRevokedAt))
	return err
}

// CreateRoomWithOwner commits device enrollment, room creation, and the
// owner membership in ONE transaction: a membership write failing after
// CreateRoom committed left an active room whose OwnerDeviceID pointed
// at no membership — the caller never got credentials and nobody could
// administer or dissolve the room until a server restart.
func (r *Repo) CreateRoomWithOwner(ctx context.Context, d store.Device, room store.Room, m store.Membership) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO devices (id, display_name, hostname, public_key_pem, first_seen_at)
		 VALUES (?,?,?,?,?)
		 ON CONFLICT(id) DO UPDATE SET display_name=excluded.display_name, public_key_pem=excluded.public_key_pem`,
		d.ID, d.DisplayName, d.Hostname, d.PublicKeyPEM, d.FirstSeenAt.Unix()); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO rooms (`+roomCols+`) VALUES (?,?,?,?,?,?,?,?,?,?)`,
		room.ID, room.ShareID, room.PasswordHash, room.OwnerDeviceID,
		room.CreatedAt.Unix(), nilTime(room.DissolvedAt), room.PendingTransferToDevice, room.PendingTransferGeneration, room.PendingTransferSnapshotSeq,
		nilTime(room.ShareRevokedAt)); err != nil {
		return err
	}
	var connected any
	if m.ConnectedAt != nil {
		connected = m.ConnectedAt.Unix()
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO memberships (`+membershipCols+`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
		m.RoomID, m.DeviceID, m.JoinedAt.Unix(), connected, m.LastSeenAt.Unix(), 0, m.SessionTokenHash, "lazy", "", 0, 0, 0); err != nil {
		return err
	}
	return tx.Commit()
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
		`UPDATE rooms SET password_hash=?, owner_device_id=?, dissolved_at=?, pending_transfer_to_device=?, pending_transfer_generation=?, pending_transfer_snapshot_seq=?, share_revoked_at=? WHERE id=?`,
		room.PasswordHash, room.OwnerDeviceID, nilTime(room.DissolvedAt), room.PendingTransferToDevice,
		room.PendingTransferGeneration, room.PendingTransferSnapshotSeq, nilTime(room.ShareRevokedAt), room.ID)
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

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
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
		 ON CONFLICT(id) DO UPDATE SET display_name=excluded.display_name, public_key_pem=excluded.public_key_pem`,
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

const membershipCols = "room_id, device_id, joined_at, connected_at, last_seen_at, online, session_token_hash, replica_policy, transfer_generation, transfer_snapshot_seq, transfer_applied_seq, transfer_ready"

func scanMembership(row interface{ Scan(...any) error }) (store.Membership, error) {
	var m store.Membership
	var connected sql.NullInt64
	var joined, lastSeen int64
	var online int
	var ready int
	err := row.Scan(&m.RoomID, &m.DeviceID, &joined, &connected, &lastSeen, &online, &m.SessionTokenHash, &m.ReplicaPolicy, &m.TransferGeneration, &m.TransferSnapshotSeq, &m.TransferAppliedSeq, &ready)
	if errors.Is(err, sql.ErrNoRows) {
		return store.Membership{}, store.ErrNotFound
	}
	if err != nil {
		return store.Membership{}, err
	}
	m.Online = online != 0
	m.TransferReady = ready != 0
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
		`INSERT INTO memberships (`+membershipCols+`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
		 ON CONFLICT(room_id, device_id) DO UPDATE SET
		   connected_at=excluded.connected_at,
		   last_seen_at=excluded.last_seen_at,
		   online=excluded.online,
		   session_token_hash=excluded.session_token_hash,
			   replica_policy=excluded.replica_policy`,
		m.RoomID, m.DeviceID, m.JoinedAt.Unix(), connected, m.LastSeenAt.Unix(), online, m.SessionTokenHash, policy, m.TransferGeneration, m.TransferSnapshotSeq, m.TransferAppliedSeq, boolInt(m.TransferReady))
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

// EnrollWithTicket commits ticket consumption, device enrollment, and
// membership upsert in ONE transaction: split writes let a concurrent
// redemption's loser clobber the winner's token (same device) or leave
// a ghost membership (different device) after its compare-and-set
// fails at the end.
func (r *Repo) EnrollWithTicket(ctx context.Context, hash string, now time.Time, d store.Device, m store.Membership) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Expiry is enforced at COMMIT time, not at the earlier request
	// check: a redemption that waited on the lifecycle mutex behind a
	// long operation must not turn elapsed credentials into a member.
	// `now` is the caller's clock at redemption time — the ticket stays
	// valid through its final second (>=).
	res, err := tx.ExecContext(ctx, `UPDATE join_tickets SET redeemed=1 WHERE hash=? AND redeemed=0 AND expires_at>=?`, hash, now.Unix())
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return store.ErrTicketUsed
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO devices (id, display_name, hostname, public_key_pem, first_seen_at)
		 VALUES (?,?,?,?,?)
		 ON CONFLICT(id) DO UPDATE SET display_name=excluded.display_name, public_key_pem=excluded.public_key_pem`,
		d.ID, d.DisplayName, d.Hostname, d.PublicKeyPEM, d.FirstSeenAt.Unix()); err != nil {
		return err
	}
	policy := string(m.ReplicaPolicy)
	if policy == "" {
		policy = "lazy"
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO memberships (`+membershipCols+`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
		 ON CONFLICT(room_id, device_id) DO UPDATE SET
		   connected_at=excluded.connected_at,
		   last_seen_at=excluded.last_seen_at,
		   online=excluded.online,
		   session_token_hash=excluded.session_token_hash,
			   replica_policy=excluded.replica_policy`,
		m.RoomID, m.DeviceID, m.JoinedAt.Unix(), nil, m.LastSeenAt.Unix(), 0, m.SessionTokenHash, policy, "", 0, 0, 0); err != nil {
		return err
	}
	return tx.Commit()
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
	objects := make([]store.CASObject, 0, len(hashes))
	for _, hash := range hashes {
		objects = append(objects, store.CASObject{Hash: hash})
	}
	return r.AddCASRefsSized(ctx, roomID, objects, 0)
}

func (r *Repo) AddCASRefsSized(ctx context.Context, roomID string, objects []store.CASObject, quotaBytes int64) error {
	// Publication callers hold CASPublication across filesystem writes and
	// this metadata transaction. Locking again here would deadlock because the
	// publication fence is intentionally non-reentrant.
	return r.addCASRefsSized(ctx, roomID, objects, quotaBytes)
}

func (r *Repo) addCASRefsSized(ctx context.Context, roomID string, objects []store.CASObject, quotaBytes int64) error {
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
	for _, object := range objects {
		if object.Hash == "" || object.Size < 0 {
			return errors.New("invalid CAS object accounting")
		}
		if object.Size > 0 {
			if _, err := tx.ExecContext(ctx, `INSERT INTO cas_objects(hash, size) VALUES(?, ?) ON CONFLICT(hash) DO UPDATE SET size=excluded.size`, object.Hash, object.Size); err != nil {
				return err
			}
		}
		if _, err := stmt.ExecContext(ctx, roomID, object.Hash); err != nil {
			return err
		}
	}
	var used int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(o.size), 0) FROM cas_refs r JOIN cas_objects o ON o.hash=r.hash WHERE r.room_id=?`, roomID).Scan(&used); err != nil {
		return err
	}
	// The quota is stored in the repository-independent room policy table by
	// the API's configured limit; callers pass the limit through the context.
	if quotaBytes > 0 && used > quotaBytes {
		return fmt.Errorf("CAS room quota exceeded: %d > %d bytes", used, quotaBytes)
	}
	return tx.Commit()
}

// ReserveCASPending records an upload without making it a durable room ref.
// The per-device quota prevents any member from filling a room's durable
// quota with abandoned chunk PUTs.
func (r *Repo) ReserveCASPending(ctx context.Context, ref store.CASPendingRef, quotaBytes int64) error {
	if ref.RoomID == "" || ref.DeviceID == "" || ref.Hash == "" || ref.Size < 0 {
		return errors.New("invalid pending CAS reference")
	}
	r.casMu.Lock()
	defer r.casMu.Unlock()
	// The PUT handler reserves before entering the filesystem publication
	// section, so this transaction never spans file IO.
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO cas_pending_refs(room_id,device_id,hash,size,expires_at) VALUES(?,?,?,?,?) ON CONFLICT(room_id,device_id,hash) DO UPDATE SET size=excluded.size,expires_at=excluded.expires_at`, ref.RoomID, ref.DeviceID, ref.Hash, ref.Size, ref.ExpiresAt.Unix()); err != nil {
		return err
	}
	var existingSize int64
	if err := tx.QueryRowContext(ctx, `SELECT size FROM cas_objects WHERE hash=?`, ref.Hash).Scan(&existingSize); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	} else if err == nil && existingSize != ref.Size {
		return fmt.Errorf("CAS object %s size mismatch: have %d, got %d", ref.Hash, existingSize, ref.Size)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO cas_objects(hash,size) VALUES(?,?) ON CONFLICT(hash) DO NOTHING`, ref.Hash, ref.Size); err != nil {
		return err
	}
	var used int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(size),0) FROM cas_pending_refs WHERE room_id=? AND device_id=? AND expires_at>=?`, ref.RoomID, ref.DeviceID, time.Now().Unix()).Scan(&used); err != nil {
		return err
	}
	if quotaBytes > 0 && used > quotaBytes {
		return fmt.Errorf("CAS pending device quota exceeded: %d > %d bytes", used, quotaBytes)
	}
	return tx.Commit()
}

// PromoteCASPending atomically turns reservations into live references after
// the sequencer accepted the operation. Missing reservations are allowed for
// objects already live in the room or snapshot-created objects.
func (r *Repo) PromoteCASPending(ctx context.Context, roomID, deviceID string, hashes []string, quotaBytes int64) error {
	r.casMu.Lock()
	defer r.casMu.Unlock()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := r.promoteCASPendingTx(ctx, tx, roomID, deviceID, hashes, quotaBytes); err != nil {
		return err
	}
	return tx.Commit()
}

// PublishCASPending makes pending promotion and the caller's prepare step one
// atomic publication. The callback must only mutate a private prepare copy:
// its result becomes authoritative only after the transaction commits.
func (r *Repo) PublishCASPending(ctx context.Context, roomID, deviceID string, hashes []string, quotaBytes int64, fn func() error) error {
	r.casMu.Lock()
	defer r.casMu.Unlock()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := r.promoteCASPendingTx(ctx, tx, roomID, deviceID, hashes, quotaBytes); err != nil {
		return err
	}
	if fn != nil {
		if err := fn(); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (r *Repo) promoteCASPendingTx(ctx context.Context, tx *sql.Tx, roomID, deviceID string, hashes []string, quotaBytes int64) error {
	for _, hash := range hashes {
		if hash == "" {
			continue
		}
		var live int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM cas_refs WHERE room_id=? AND hash=?`, roomID, hash).Scan(&live); err != nil {
			return err
		}
		if live != 0 {
			if _, err := tx.ExecContext(ctx, `DELETE FROM cas_pending_refs WHERE room_id=? AND device_id=? AND hash=?`, roomID, deviceID, hash); err != nil {
				return err
			}
			continue
		}
		var pending int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM cas_pending_refs WHERE room_id=? AND device_id=? AND hash=? AND expires_at > ?`, roomID, deviceID, hash, time.Now().Unix()).Scan(&pending); err != nil {
			return err
		}
		if pending == 0 {
			return fmt.Errorf("CAS object %s has no live or pending reference", hash)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO cas_refs(room_id,hash) SELECT room_id,hash FROM cas_pending_refs WHERE room_id=? AND device_id=? AND hash=?`, roomID, deviceID, hash); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM cas_pending_refs WHERE room_id=? AND device_id=? AND hash=?`, roomID, deviceID, hash); err != nil {
			return err
		}
	}
	var used int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(o.size),0) FROM cas_refs r JOIN cas_objects o ON o.hash=r.hash WHERE r.room_id=?`, roomID).Scan(&used); err != nil {
		return err
	}
	if quotaBytes > 0 && used > quotaBytes {
		return fmt.Errorf("CAS room quota exceeded: %d > %d bytes", used, quotaBytes)
	}
	return nil
}

func (r *Repo) CanPromoteCASPending(ctx context.Context, roomID, deviceID string, hashes []string, quotaBytes int64) error {
	r.casMu.Lock()
	defer r.casMu.Unlock()
	return r.canPromoteCASPending(ctx, roomID, deviceID, hashes, quotaBytes)
}

func (r *Repo) canPromoteCASPending(ctx context.Context, roomID, deviceID string, hashes []string, quotaBytes int64) error {
	if quotaBytes <= 0 {
		return nil
	}
	var used int64
	if err := r.db.QueryRowContext(ctx, `SELECT COALESCE(SUM(o.size),0) FROM cas_refs r JOIN cas_objects o ON o.hash=r.hash WHERE r.room_id=?`, roomID).Scan(&used); err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, hash := range hashes {
		if hash == "" || seen[hash] {
			continue
		}
		seen[hash] = true
		var live int
		if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM cas_refs WHERE room_id=? AND hash=?`, roomID, hash).Scan(&live); err != nil {
			return err
		}
		if live != 0 {
			continue
		}
		var size int64
		if err := r.db.QueryRowContext(ctx, `SELECT size FROM cas_pending_refs WHERE room_id=? AND device_id=? AND hash=? AND expires_at > ?`, roomID, deviceID, hash, time.Now().Unix()).Scan(&size); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("CAS object %s has no pending reference", hash)
			}
			return err
		}
		used += size
	}
	if used > quotaBytes {
		return fmt.Errorf("CAS room quota exceeded: %d > %d bytes", used, quotaBytes)
	}
	return nil
}

func (r *Repo) SweepCASPending(ctx context.Context, now time.Time) error {
	r.casMu.Lock()
	defer r.casMu.Unlock()
	_, err := r.db.ExecContext(ctx, `DELETE FROM cas_pending_refs WHERE expires_at <= ?`, now.Unix())
	return err
}

func (r *Repo) DeleteCASPending(ctx context.Context, roomID, deviceID string, hashes []string) error {
	r.casMu.Lock()
	defer r.casMu.Unlock()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, hash := range hashes {
		if _, err := tx.ExecContext(ctx, `DELETE FROM cas_pending_refs WHERE room_id=? AND device_id=? AND hash=?`, roomID, deviceID, hash); err != nil {
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

func (r *Repo) RecordCASOrphan(ctx context.Context, hash string) error {
	_, err := r.db.ExecContext(ctx, `INSERT OR IGNORE INTO cas_orphans(hash) VALUES(?)`, hash)
	return err
}

func (r *Repo) ListCASOrphans(ctx context.Context) ([]string, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT hash FROM cas_orphans`)
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

func (r *Repo) DeleteCASOrphan(ctx context.Context, hash string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM cas_orphans WHERE hash=?`, hash)
	return err
}

// CASPublication serializes object publication (filesystem write +
// reference insertion) with collection: both hold casMu, so a
// collection can never observe a zero count in the gap between a
// freshly written object and its reference row.
func (r *Repo) CASPublication(fn func() error) error {
	r.casMu.Lock()
	defer r.casMu.Unlock()
	return fn()
}

// CollectUnreferencedCAS checks refs in a short transaction, then runs the
// deletion callback outside that transaction while retaining casMu. All CAS
// publication paths use the same fence, so a reference cannot be committed
// between the global refcount check and filesystem deletion.
func (r *Repo) CollectUnreferencedCAS(ctx context.Context, hashes []string, del func(hash string) error) error {
	r.casMu.Lock()
	defer r.casMu.Unlock()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	seen := make(map[string]struct{}, len(hashes))
	var candidates []string
	for _, h := range hashes {
		if h == "" {
			continue
		}
		if _, ok := seen[h]; ok {
			continue
		}
		seen[h] = struct{}{}
		var n int
		if err := tx.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM cas_refs WHERE hash=?) + (SELECT COUNT(*) FROM cas_pending_refs WHERE hash=?)`, h, h).Scan(&n); err != nil {
			return err
		}
		if n == 0 {
			candidates = append(candidates, h)
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	for _, h := range candidates {
		if del != nil {
			// Failed deletion deliberately leaks and remains eligible for a
			// later sweep; deleting a referenced object would corrupt a room.
			_ = del(h)
		}
	}
	return nil
}

// CASObjectBatch returns committed object metadata in bounded pages. Callers
// reconcile this list with the filesystem while casMu prevents publication
// from racing the mark-and-sweep decision.
func (r *Repo) ListCASObjects(ctx context.Context, after string, limit int) ([]store.CASObject, error) {
	if limit <= 0 {
		return nil, nil
	}
	rows, err := r.db.QueryContext(ctx, `SELECT hash,size FROM cas_objects WHERE hash>? ORDER BY hash LIMIT ?`, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.CASObject
	for rows.Next() {
		var o store.CASObject
		if err := rows.Scan(&o.Hash, &o.Size); err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// CASObjectLive checks refs and pending reservations under casMu.
func (r *Repo) CASObjectLive(ctx context.Context, hash string) (bool, error) {
	r.casMu.Lock()
	defer r.casMu.Unlock()
	var count int
	err := r.db.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM cas_refs WHERE hash=?) + (SELECT COUNT(*) FROM cas_pending_refs WHERE hash=?)`, hash, hash).Scan(&count)
	return count > 0, err
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

// DissolveRoomFenced reads the room's CAS refs and dissolves the room
// (deleting those refs) inside the SAME casMu critical section that
// serializes CAS publication: an upload inserting a reference can no
// longer land between the ref read and the delete, which leaked the
// object behind a terminal room's orphan cas_refs row.
func (r *Repo) DissolveRoomFenced(ctx context.Context, roomID string, at time.Time) ([]string, error) {
	r.casMu.Lock()
	defer r.casMu.Unlock()
	refs, err := r.RoomCASRefs(ctx, roomID)
	if err != nil {
		return nil, err
	}
	if err := r.DissolveRoom(ctx, roomID, at); err != nil {
		return nil, err
	}
	return refs, nil
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
