# OpenTuttiVM Architecture

OpenTuttiVM is a real-time collaborative extension of the Tutti desktop:
multiple users share one agent workspace — files, terminals, and browser
previews — while every agent keeps running in its owner's own machine,
never on a shared server.

The complete design conversation is preserved at
`docs/OpenTuttiVM_PRD对话.pdf`.

## Topology

```
┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│  Device A    │   │  Device B    │   │  Device C    │
│ Docker       │   │ Docker       │   │ Docker       │
│ Desktop VM   │   │ Desktop VM   │   │ Desktop VM   │
│              │   │              │   │              │
│ agents       │   │ agents       │   │ agents       │
│ room-sync    │   │ room-sync    │   │ room-sync    │
│ open-tutti-fs│   │ open-tutti-fs│   │ open-tutti-fs│
└──────┬───────┘   └──────┬───────┘   └──────┬───────┘
       │  WS + yamux     │                  │
       └─────────────────┼──────────────────┘
                         ▼
               ┌──────────────────┐
               │ open-tutti-server│  (bare server: rooms, OT sequencer,
               │ SQLite + CAS     │   CAS, relay — NO agent code)
               └──────────────────┘
```

Two hard rules shape everything:

1. **Agent Host Boundary.** Agents always run on the user's own device
   (their Docker Desktop Linux VM). The server never executes agent code.
   This change does not alter session/turn/goal lifecycle semantics —
   `packages/agent/host` is untouched; room-sync/fs are new collaboration
   runtimes that mount shared state underneath agents.
2. **Server is the single source of truth** for room membership, the
   operation sequence, and content-addressed storage; every device holds a
   replica it can rebuild from the server at any time.

## Modules

| Module | Responsibility |
| --- | --- |
| `packages/workspace/vm-protocol` | Wire contracts: `FileOperation`/`Envelope`, snapshots, event topics, `.tutti` hostname parsing, tunnel headers |
| `packages/workspace/vm-cas` | Content-addressed storage: 4 MiB chunks, SHA-256, self-verifying manifests, local + memory stores |
| `packages/workspace/vm-sync` | Server-authoritative OT engine: text patch transform/apply, optimistic blob replace, conflict barrier, replica bootstrap/resync, `ConvertChange` (POSIX write → operation) |
| `services/open-tutti-server` | The bare server: room lifecycle, share/ticket auth, HTTP API, WS hub, sequencer, CAS endpoints, yamux relay |
| `services/open-tutti-room-sync` | Per-device engine: Full/Lazy replica, WS client with gap-resync, `.tutti` gateway + VIP allocation, local CA, Room FS Protocol host |
| `services/open-tutti-fs` | FUSE mount (Linux containers) bridging POSIX into the room via the Room FS Protocol |

## Collaboration model

Hybrid, by content class:

- **Text (UTF-8, ≤8 MiB)** — real-time File Operation Stream: server-side
  OT transforms concurrent patches (byte-range splices) into one total
  order; authors see rejections only when their base hash no longer
  matches and re-diff automatically.
- **Binary / large** — CAS blob replace with optimistic version check
  (`BaseHash`); losers re-upload against the current manifest.
- **Semantic conflicts** (same-point edits, racing replaces) — server
  opens a **conflict barrier**: the path is fenced, the last author
  becomes resolver, everyone else is notified; the barrier resolves
  explicitly. Fencing is enforced by the server, not by UI promises.
- **Persistence** — periodic snapshots (text→CAS manifests) referenced by
  sequence number; bootstrap = snapshot + replay window; a sequence gap
  triggers snapshot resync, never partial guessing.

## Room lifecycle (meeting semantics)

- A room exists while people are in it; when everyone leaves it dissolves.
- Owner must Apply-to-Workspace (mirror the final state to their host
  workspace) before leaving; then disband or transfer.
- Owner disconnect: 5-minute grace → ownership transfers to the longest
  continuously-connected participant, or the room dissolves.
- Transfer is three-phase: prepare (candidate must be a member) → the
  candidate builds a full replica and initializes the host workspace →
  commit. Partial transfers never land.
- Server restart ends all rooms (meeting over; durable artifacts remain in
  CAS and participants' applied workspaces).

## Auth model

No accounts. Device = user (Ed25519 identity). Optional server invite code
(`OPEN_TUTTI_SERVER_INVITE_CODE`, checked per room creation). Sharing is a
URL `/share/<share-id>` + a 6-digit room password (Argon2id). The share
page mints a one-time join ticket (60 s TTL) that the desktop redeems for
a session token; the `open-tutti://join` deep link carries the ticket —
never the password.

## `.tutti` virtual network

`session.device.tutti:port` is always unique. Device short names route
when unambiguous: ambiguous HTTP(S) renders an H5 session selector
(synthetic CA-signed TLS, trusted only inside Tutti runtimes); ambiguous
raw TCP refuses with guidance instead of random routing. Synthetic IPs
come from 100.96.0.0/12 and never leave the room network. All cross-device
traffic flows device → server → device over WebSocket + yamux.

## Compatibility

Go modules build and test on Linux, macOS, and Windows; the FUSE layer is
Linux-only by design (it runs inside session containers on the Docker
Desktop VM) and is build-tagged out elsewhere. Deployment ships only the
bare server image (`ghcr.io/xiaoheicat/open-tutti-server`); clients build
per-platform from this repository.

Upstream Tutti services (`services/tuttid`, `apps/desktop`) are untouched
by this change set; OpenTuttiVM adds new value beside them.
