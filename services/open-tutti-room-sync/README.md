# open-tutti-room-sync

The per-device room engine. One instance runs in each user's Docker
Desktop Linux VM for each joined room (`open-tutti-vm-<roomId>` Compose
project):

- keeps the workspace replica in the `open-tutti-vm-<roomId>-workspace`
  named volume — **Full** for room owners and transfer candidates,
  **Lazy** for participants (tree always local, content fetched on demand)
- talks to open-tutti-server: operations over the room WebSocket with
  sequence-gap resync, CAS chunks over HTTP
- hosts the Room FS Protocol (unix socket) that `open-tutti-fs` mounts
- terminates the `.tutti` virtual network: route resolution, synthetic
  VIPs (100.96.0.0/12), the local CA whose trust exists only inside Tutti
  runtimes, and the H5 session selector for ambiguous device addresses

## Environment

| Variable | Default | Meaning |
| --- | --- | --- |
| `OPEN_TUTTI_SERVER` | — (required) | Server base URL |
| `OPEN_TUTTI_TOKEN` | — (required) | Room session token |
| `OPEN_TUTTI_DEVICE_ID` | — (required) | This device's id |
| `OPEN_TUTTI_POLICY` | `lazy` | `lazy` or `full` |
| `OPEN_TUTTI_CACHE_DIR` | `/data/cache` | CAS cache |
| `OPEN_TUTTI_CA_DIR` | `/data/ca` | Device-private room CA pair; certificate and key are atomically published as one synced file |
| `OPEN_TUTTI_FS_LISTEN` | `/run/open-tutti/roomfs.sock` | Room FS socket |
| `OPEN_TUTTI_ROOMFS_CAPABILITY` | random per process | Capability for open-tutti-fs |
| `OPEN_TUTTI_ROOMFS_CAPABILITY_FILE` | sibling `roomfs.cap` for Unix sockets | Private capability handoff |
| `OPEN_TUTTI_ROOMFS_CAPABILITY` | random per process | Capability for open-tutti-fs |
| `OPEN_TUTTI_ROOMFS_CAPABILITY_FILE` | sibling `roomfs.cap` for Unix sockets | Private capability handoff |

Architecture: `docs/architecture/open-tutti-vm.md`.

Room-sync has no Agent Host adapter in this deployment, so it does not publish
`agent_share shared=true`; the server rejects such declarations as well.

Room FS delivery is standalone and fail-closed: room-sync writes a Unix
capability file with mode `0600`, while Windows uses loopback TCP plus the same
first-frame capability. This does not claim Desktop Docker orchestration or
Full Replica persistence, which remain outside the current standalone boundary.

Room FS delivery is standalone and fail-closed: room-sync writes a Unix
capability file with mode `0600`, while Windows uses loopback TCP plus the same
first-frame capability. This does not claim Desktop Docker orchestration or
Full Replica persistence, which remain outside the current standalone boundary.
