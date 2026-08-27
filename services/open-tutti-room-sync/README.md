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
| `OPEN_TUTTI_FS_LISTEN` | `/run/open-tutti/roomfs.sock` | Room FS socket |

Architecture: `docs/architecture/open-tutti-vm.md`.
