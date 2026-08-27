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
- `GET  /api/rooms/{roomID}/routes` — `.tutti` route resolution
- `GET  /api/tunnel` — yamux multiplexed relay for cross-device streams
- `POST /api/rooms/{roomID}/leave|password/rotate|transfer/*` — lifecycle

All room-scoped routes require the session token from join.
