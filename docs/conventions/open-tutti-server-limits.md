# Open Tutti Server Limits

The server enforces both per-device and per-room pending CAS upload budgets.
`OPEN_TUTTI_CAS_PENDING_QUOTA_BYTES` limits one device;
`OPEN_TUTTI_CAS_PENDING_ROOM_QUOTA_BYTES` limits the sum of unexpired pending
reservations for the room. Pending reservations are removed on expiry, device
leave, and room dissolution.

Workspace engines enforce `OPEN_TUTTI_WORKSPACE_MAX_CONTENT_BYTES` before an
operation is accepted. It counts live text content and blob logical sizes, not
deduplicated CAS storage, and is independent of the CAS room quota.
