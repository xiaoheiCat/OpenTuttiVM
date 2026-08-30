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
| `services/open-tutti-server` | The bare server: room lifecycle, share/ticket auth, HTTP API, WS hub, sequencer, CAS endpoints, yamux relay, agent-borrowing registry (lease fencing + approval routing) |
| `services/open-tutti-room-sync` | Per-device engine: Full/Lazy replica, WS client with gap-resync, `.tutti` gateway + VIP allocation, local CA, Room FS Protocol host |
| `services/open-tutti-fs` | FUSE mount (Linux containers) bridging POSIX into the room via the Room FS Protocol |
| `packages/workspace/vm-roomfs` | The Room FS Protocol itself — a workspace-domain contract both services adapt to, so neither executable owns the seam |

## Collaboration model

Hybrid, by content class:

- **Text (UTF-8, ≤8 MiB)** — real-time File Operation Stream: server-side
  OT transforms concurrent patches (byte-range splices) into one total
  order; authors see rejections only when their base hash no longer
  matches and re-diff automatically.
- **Binary / large** — CAS blob replace with optimistic version check
  (`BaseHash`); losers re-upload against the current manifest. Before a
  replacement becomes authoritative, the server verifies the manifest's
  object graph (manifest decodes, self-hash matches, every chunk is in
  CAS), so replicas never chase dangling references.
- **Semantic conflicts** (same-point edits, racing replaces) — server
  opens a **conflict barrier**: the path is fenced, the last author
  becomes resolver, everyone else is notified; the barrier resolves
  explicitly. Fencing is enforced by the server, not by UI promises,
  and `conflict_resolved` is only honored from the assigned resolver's
  own connection.
- **Persistence** — periodic snapshots (text→CAS manifests) referenced by
  sequence number; bootstrap = snapshot + replay window; a sequence gap
  triggers snapshot resync, never partial guessing.

## Room lifecycle (meeting semantics)

- A room exists while people are in it; when everyone leaves it dissolves.
- Owner must Apply-to-Workspace (mirror the final state to their host
  workspace) before leaving; then disband or transfer.
- Owner disconnect: 5-minute grace → ownership transfers to the longest
  continuously-connected participant, or the room dissolves.
- Transfer is three-phase: prepare issues a server generation and snapshot
  sequence → the candidate builds a full replica and initializes the host
  workspace, then reports readiness on its authenticated room connection →
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

Remote server deployment requires TLS termination in a reverse proxy before
exposing the HTTP listener. Plain HTTP is fail-closed on non-loopback binds;
loopback HTTP is for local development only.

## `.tutti` virtual network

`session.device.tutti:port` is always unique. Device short names route
when unambiguous: ambiguous HTTP(S) renders an H5 session selector
(synthetic CA-signed TLS, trusted only inside Tutti runtimes); ambiguous
raw TCP refuses with guidance instead of random routing. Synthetic IPs
come from 100.96.0.0/12 and never leave the room network. All cross-device
traffic flows device → server → device over WebSocket + yamux.

VIP binding is runtime-probed. Inside the room VM image the /12 block is
configured on an adapter and per-host VIPs bind directly; on Linux
without configuration the gateway sets `IP_FREEBIND` so the same VIPs
still bind; on runtimes where neither works (plain containers without
`NET_ADMIN`, stock macOS/Windows) the allocator falls back to shared
loopback mode — DNS answers 127.0.0.1 for every `.tutti` host, one
listener serves each port, and connections demultiplex by TLS SNI or
the HTTP Host header (raw TCP without either works only on a
single-host port).

room-sync composes the whole surface in one process: a UDP DNS responder
answers `*.tutti` with the allocator's VIPs, per-route VIP listeners
terminate TLS with the room CA (exported under `OPEN_TUTTI_CA_DIR` for
injection into the Tutti Browser and session containers — never the host
OS store), device-level addresses re-resolve at connect time with the
selector on ambiguity, and the yamux tunnel serves **both** legs —
outbound dials for locally originated connections and inbound forwards
that dial the owning session container on the room network
(`OPEN_TUTTI_SESSION_DIAL`, default `agent-<session>`). The room compose
advertises what each session serves through `OPEN_TUTTI_SESSION_PORTS`
(under `OPEN_TUTTI_SESSION_LABEL`, default `main`): without an
announcement the preview registry stays empty and the relay refuses every
target by design. Share pages hand off through the desktop-registered
`tutti://join` deep link carrying server, room id, and one-time ticket —
the password never appears in any URL.

## Agent Borrowing (v1, first-class)

Borrowing is a room capability, locked in the design record as a v1
feature, not a later plugin:

- **Model** — an owner shares one agent instance (`agent.shared`) with its
  capabilities (skills, MCP servers, tools). File-only skills inject
  read-only into sessions; MCP servers and tools execute on the **owner's
  device** through its Capability Broker. Configs and credentials never
  travel: borrowers can use "Alice's GitHub MCP to look at an issue" but
  can never read her MCP config, OAuth tokens, or `~/.ssh`.
- **Flow** — borrower command (`agent.borrow_command`) → server validates
  the lease → routes to the owner's device → the agent executes there, in
  the room VM, with results/terminal/file changes streaming to everyone.
- **Revocation is fencing, not UI** — every share start bumps a lease
  generation; revoke bumps it again. Commands carrying a stale generation
  are rejected instantly (`agent.borrow_revoked`), and the current
  borrowing session learns its generation is dead.
- **Approvals route to the borrower** — the current borrower is the
  session operator. Permission prompts (`agent.approval_request`) go to
  them, never to the owner (a capability provider, not the operator); the
  decision (`agent.approval_decision`) routes back to the owner's runtime,
  which resumes or interrupts. Agents continue approval-free work while
  paused on a prompt.
- **BorrowSafe gating** — adapters that cannot guarantee the isolation
  contract (workspace-only filesystem, no host fs, no docker socket, no
  credential files, no privilege escalation, policy-controlled network)
  stay self-usable but the server refuses to share them.

Server-side state lives in `services/open-tutti-server/internal/borrow`
(registry + fencing + approval routing); identities (owner, borrower,
decider) are always stamped from the authenticated connection, never
trusted from the wire.

## Why room events do not use the `packages/events` catalog

`vm-protocol`'s room event bus is a deliberate second seam, not an
oversight. `packages/events` is the daemon's schema-first DOMAIN event
catalog: versioned, generated-validation records persisted and replayed
across tuttid's HTTP/query surfaces. The room bus is a live, ordered,
in-memory TRANSPORT stream — envelope + `json.RawMessage` payload over
one WebSocket per device, where the sequencer's ordering (and its
rejection/barrier protocol) IS the contract. Routing it through the
generated catalog would add version ceremony to a stream whose
consumers all deploy in lockstep with the server (same repo, same
release), while making a workspace package depend on daemon domain
schema. Room topics therefore stay in `vm-protocol` (workspace-owned
wire contract); if a room event ever needs to PERSIST into tuttid's
domain streams, the adapter translating it belongs in
`services/tuttid`, not in the room bus.

## Compatibility

Go modules build and test on Linux, macOS, and Windows; the FUSE layer is
Linux-only by design (it runs inside session containers on the Docker
Desktop VM) and is build-tagged out elsewhere. Deployment ships only the
bare server image (`ghcr.io/xiaoheicat/open-tutti-server`); clients build
per-platform from this repository.

Upstream Tutti services (`services/tuttid`, `apps/desktop`) are untouched
by this change set; OpenTuttiVM adds new value beside them.
