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

## Configuration

Environment > `.env` > defaults. Every variable is documented in
[`.env.example`](./.env.example).

## API surface (summary)

- `GET  /api/info` — server info
- `POST /api/rooms` — create a room (invite code when configured)
- `GET  /share/{shareID}` — H5 join page (password → one-time ticket)
- `POST /api/share/{shareID}/join-ticket` — mint a 60 s join ticket
- `POST /api/rooms/{roomID}/join` — redeem ticket, returns bootstrap snapshot
- `GET  /api/rooms/{roomID}/bootstrap` — snapshot + replay window (resync)
- `GET  /api/rooms/{roomID}/ws` — business WebSocket (ops, events, ports)
- `GET  /api/rooms/{roomID}/cas/{hash}` / `PUT` — chunk store (hash-verified)
- `GET  /api/rooms/{roomID}/routes` — `.tutti` route resolution (`?list=1`
  enumerates every live route for gateway sync)
- `GET  /api/tunnel` — yamux multiplexed relay for cross-device streams
- `POST /api/rooms/{roomID}/leave|password/rotate|transfer/*` — lifecycle
- `POST /api/rooms/{roomID}/share/revoke` — owner: invalidate the share
  link (new joins get 410; members keep access)
- `POST /api/rooms/{roomID}/members/{deviceID}/kick` — owner: remove a
  member (their session token dies immediately)

All room-scoped routes require the session token from join.

## Business WebSocket messages (summary)

Client → server: `op` (workspace operation), `ports` (session port
announcements), `conflict_resolved`, `agent_share` (owner enables/disables
borrowing), `borrow_command`, `approval_request` (owner-side agent
runtime), `approval_decision` (session operator), `ping`.

Server → client: workspace events (`operation`, `operation_rejected`,
`conflict_*`, `snapshot`, `environment.changed`, `ports_changed`,
`presence`, `ending`) plus agent-borrowing events (`agent.shared`,
`agent.borrow_command`, `agent.borrow_revoked`, `agent.approval_request`,
`agent.approval_decision`). Identities (author, borrower, decider) are
stamped server-side from the authenticated connection.
