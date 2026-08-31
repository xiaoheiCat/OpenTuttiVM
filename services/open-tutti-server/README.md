# open-tutti-server

The bare collaboration server for OpenTuttiVM rooms: room lifecycle,
share-link auth, the authoritative OT sequencer, content-addressed
storage, and the cross-device relay. It never runs agent code — agents
always run in each user's own Docker Desktop Linux VM.

See `docs/architecture/open-tutti-vm.md` for the full model.

## Run

```bash
cp services/open-tutti-server/.env.example .env # from the repository root; set OPEN_TUTTI_SECRET to a unique value
docker compose up -d      # from the repository root
```

Or from source:

```bash
go run . # reads .env and OPEN_TUTTI_* environment variables
```

The default Compose deployment uses an explicit bridge network. The server
listens on container loopback at `127.0.0.1:8081`; an internal `socat`
forwarder exposes port 8080 to Docker, while Docker publishes it only on the host loopback
at `127.0.0.1:8080`. This is consistent on Docker Desktop macOS/Windows and
 Linux without host networking. The server always rejects plain HTTP on a
non-loopback listener; no environment marker is a security boundary.
Remote deployment must use a separate Compose override with an HTTPS reverse proxy and an `https://`
`OPEN_TUTTI_PUBLIC_URL`; plain HTTP on a non-loopback listener is rejected.
Do not publish the server port or the `/data` volume to an untrusted network.

## Configuration

Environment > `.env` > defaults. `OPEN_TUTTI_SECRET` is required, must be at
least 32 characters, and placeholder values such as `change-me`, `replace-me`,
and `default` are rejected. Every variable is documented in
[`.env.example`](./.env.example).

The checked-in Compose file builds the server from the repository Dockerfile;
it does not pull a mutable `latest` image. Compose also fails before startup
unless `OPEN_TUTTI_SECRET` is explicitly supplied in `.env` or the environment.
This is the checked-in v1 deployment contract: local Dockerfile build, explicit
secret, named `open-tutti-data` volume, and loopback-only host publication. The
historical `../../docs/OpenTuttiVM_PRD对话.pdf` is an immutable record of earlier
discussion; its old `latest` and bind-mount examples are not executable
deployment guidance and the PDF text has not been modified.

`OPEN_TUTTI_CAS_ROOM_QUOTA_BYTES` limits each room's distinct referenced CAS
bytes. Repeated references to the same hash count once per room; the same hash
is counted independently for different rooms.

`OPEN_TUTTI_ACTIVE_ROOM_LIMIT` bounds active rooms, including room creations in
progress. Creation returns `429` when the limit is reached.

Workspace admission also enforces cumulative live path-key bytes and retained
deduplication identity-key bytes. Path bytes are released by remove and adjusted
by rename; deduplication keys remain retained through checkpoint so accepted
retries stay idempotent. Operation IDs and agent-session IDs have explicit byte
length limits. The corresponding overrides are listed in `.env.example`.

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
