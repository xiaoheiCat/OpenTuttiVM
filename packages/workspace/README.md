# workspace protocol packages

Shared Go contracts for OpenTuttiVM room collaboration, consumed by
`services/open-tutti-server`, `services/open-tutti-room-sync`, and
`services/open-tutti-fs`.

- **vm-protocol** — wire types: `FileOperation`/`Envelope`, workspace
  snapshots, event topics and payloads, `.tutti` hostname parsing and
  device-route resolution, tunnel headers
- **vm-cas** — content-addressed storage: 4 MiB chunks, SHA-256 hashes,
  self-verifying manifests (canonical JSON body without the hash field),
  local + memory stores, materialization
- **vm-sync** — the collaboration engine: server-authoritative state with
  byte-range text OT (transform + apply), optimistic blob replace,
  server-enforced conflict barriers, replica bootstrap/apply/resync
  (`ErrSeqGap` → snapshot resync), whole-write → operation conversion
  (`ConvertChange`)

All three are dependency-light (stdlib + `golang.org/x/crypto`-free
protocols) and test-covered on Linux, macOS, and Windows.
