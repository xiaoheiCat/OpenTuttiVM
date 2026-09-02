# open-tutti-fs

Mounts the room workspace as a FUSE filesystem inside agent-session
containers (Linux only — it runs on the Docker Desktop VM, never on the
host).

- reads come from the local replica via the Room FS Protocol
  (`roomfs` package: length-framed JSON + raw binary bodies, with
  server-pushed invalidations)
- writes buffer in the open file and flush as whole-file protocol writes;
  room-sync converts them into Room File Operations (text patch or CAS
  blob replace) against the server-authoritative sequence
- room-level rejections (base mismatch, barrier fencing) surface as
  `EAGAIN`, so editors and CLIs retry against the fresh revision
- timeout, connection, transport, and CAS/fetch failures surface as `EIO`;
  dirty buffers remain pending until authoritative acknowledgement
- timeout, connection, transport, and CAS/fetch failures surface as `EIO`;
  dirty buffers remain pending until authoritative acknowledgement

## Environment

| Variable | Default | Meaning |
| --- | --- | --- |
| `OPEN_TUTTI_ROOMFS_ADDR` | `/run/open-tutti/roomfs.sock` | room-sync socket (or `host:port`) |
| `OPEN_TUTTI_MOUNT` | `/workspace` | Mount point |
| `OPEN_TUTTI_DEVICE_ID` | — (required) | Device id for submissions |
| `OPEN_TUTTI_ROOMFS_CAPABILITY` | — | Per-process capability supplied by room-sync |
| `OPEN_TUTTI_ROOMFS_CAPABILITY_FILE` | `/run/open-tutti/roomfs.cap` | Private capability file |

Capability-file reads fail closed for symlinks/reparse points, non-regular files,
and unsafe POSIX permissions. room-sync publishes the file through a private
temporary file, fsync, and an atomic platform-specific rename.
| `OPEN_TUTTI_ROOMFS_CAPABILITY` | — | Per-process capability supplied by room-sync |
| `OPEN_TUTTI_ROOMFS_CAPABILITY_FILE` | `/run/open-tutti/roomfs.cap` | Private capability file |

The `roomfs` package also defines the server side; room-sync hosts it over
its replica. Both halves share the framing code, so the contract is tested
against itself. The first frame is a capability handshake; unauthenticated
connections are closed before any filesystem request is dispatched. The
standalone mount is fail-closed when no capability is supplied.
The first frame is a capability handshake; unauthenticated connections are
closed before any filesystem request is dispatched. The standalone mount is
fail-closed when no capability is supplied.
