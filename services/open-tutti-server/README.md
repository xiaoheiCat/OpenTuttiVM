# open-tutti-server

The bare collaboration server for OpenTuttiVM rooms: room lifecycle,
share-link auth, the authoritative OT sequencer, content-addressed
storage, and the cross-device relay. It never runs agent code — agents
always run in each user's own Docker Desktop Linux VM.

See `docs/architecture/open-tutti-vm.md` for the full model.

## Run

```bash
cp .env.example .env      # set OPEN_TUTTI_SECRET at minimum
docker compose up -d      # from the repository root
```

Or from source:

```bash
go run . # reads .env and OPEN_TUTTI_* environment variables
```

Remote deployment requires TLS termination in a reverse proxy. The server
fails closed when a plain `http://` public URL is paired with a non-loopback
listen address; loopback HTTP remains available for local development.

## Configuration

Environment > `.env` > defaults. Every variable is documented in
[`.env.example`](./.env.example).

`OPEN_TUTTI_CAS_ROOM_QUOTA_BYTES` limits each room's distinct referenced CAS
bytes. Repeated references to the same hash count once per room; the same hash
is counted independently for different rooms.

## API surface (summary)

- `GET  /api/info` — server info
- `POST /api/rooms` — create a room (invite code when configured)
- `GET  /share/{shareID}` — H5 join page (password → one-time ticket)
- `POST /api/share/{shareID}/join-ticket` — mint a 60 s join ticket
- `POST /api/rooms/{roomID}/join` — redeem ticket, returns bootstrap snapshot.
  Re-enrolling an existing device id requires `proof`: a base64 Ed25519
  signature over `open-tutti-join:` + ticket with the enrolled device key
  (the one-time ticket is the challenge)
- `GET  /api/rooms/{roomID}/bootstrap` — snapshot + replay window (resync)
- `GET  /api/rooms/{roomID}/ws` — business WebSocket (ops, events, ports)
- `GET  /api/rooms/{roomID}/cas/{hash}` / `PUT` — chunk store (hash-verified)
- `GET  /api/rooms/{roomID}/routes` — `.tutti` route resolution (`?list=1`
  enumerates every live route for gateway sync)
- `GET  /api/tunnel` — yamux multiplexed relay for cross-device streams;
  routes are bound to the authenticated room
- `POST /api/rooms/{roomID}/leave|password/rotate|transfer/*` — lifecycle
- `POST /api/rooms/{roomID}/share/revoke` — owner: invalidate the share
  link (new joins get 410; members keep access)
- `POST /api/rooms/{roomID}/members/{deviceID}/kick` — owner: remove a
  member; membership row, live business socket, and tunnel session all
  die immediately

All room-scoped routes require `Authorization: Bearer <session-token>`; query
parameters are never accepted for session tokens.

## Business WebSocket messages (summary)

Client → server: `op` (workspace operation), `ports` (session port
announcements), `conflict_resolved` (only the barrier's assigned resolver
lifts the fence — connection identity is checked), `agent_share` (owner
enables/disables borrowing), `borrow_command`, `approval_request`
(owner-side agent runtime), `approval_decision` (session operator), `ping`.

Server → client: workspace events (`operation`, `operation_rejected`,
`conflict_*`, `snapshot`, `environment.changed`, `ports_changed`,
`presence`, `ending`) plus agent-borrowing events (`agent.shared`,
`agent.borrow_command`, `agent.borrow_revoked`, `agent.approval_request`,
`agent.approval_decision`). Identities (author, borrower, decider) are
stamped server-side from the authenticated connection.
