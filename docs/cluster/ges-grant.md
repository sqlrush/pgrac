# GES Cross-Node Grant — User Manual

> Status: introduced in linkdb v0.24.0-stage2.16.

## Overview

GES (Global Enqueue Service) coordinates non-block locks across cluster
nodes.  spec-2.16 ships the MVP grant/convert infrastructure — payload
wire format, ring buffers, work queue, pending table, and observability.
Caller-side `LockAcquireExtended` integration that drives end-to-end
cross-node grants will land in spec-2.17 (BAST + deadlock + 4-node).

## Configuration

| GUC | Default | Range | Context | Description |
|---|---|---|---|---|
| `cluster.ges_request_timeout_ms` | `60000` | `[1, 600000]` | `USERSET` | Timeout (ms) for cross-node GES grant request.  Backend rolls back via GES_RELEASE on expiry.  PG `lock_timeout=0` (disabled) does NOT short-circuit — falls back to this GUC. |
| `cluster.grd_max_entries` | `0` | `[0, 1048576]` | `POSTMASTER` | Size of GRD entry HTAB (per spec-2.15).  `0` = skeleton mode. |
| `cluster.grd_entry_reclaim` | `on` | `bool` | `SIGHUP` | Enable safe cold reclaim of holderless GRD entries after the lookup pin drops to zero. |
| `cluster.grd_entry_reclaim_max_per_sweep` | `256` | `[0, 65536]` | `SIGHUP` | Maximum cold entries LMON attempts to reclaim per sweep. |

### Effective timeout

```
effective = lock_timeout == 0      ? ges_request_timeout_ms
          : NOWAIT (lock_timeout < 0) ? 0
          : min(lock_timeout, ges_request_timeout_ms)
```

## Wait Events

| Name | Class | When |
|---|---|---|
| `GesGrantWait` | Cluster:GES | Backend waiting on cross-node grant reply |
| `GesConvertWait` | Cluster:GES | Backend waiting on convert ack |
| `GesDrain` | Cluster:GES | LMON draining dirty-list / work queue |
| `GesEnqueueAcquire`, `GesEnqueueConvert`, `GesEnqueueReleaseAck`, `GesMasterQuery`, `GesLocalFastPath` | Cluster:GES | (spec-2.13 base set) |

## SQLSTATE Error Codes

| Code | Name | Meaning |
|---|---|---|
| `53R70` | `cluster_ges_timeout` | Grant request exceeded effective timeout |
| `53R71` | `cluster_ges_pending_full` | Pending request table capacity reached |
| `53R02` | `cluster_grd_full` | GRD entry / cap surface reached (per spec-2.14) |

## Observability

Five `pg_cluster_state` rows in the `ges` / `grd` category surface the
nofail counters introduced by spec-2.16:

| Key | Meaning |
|---|---|
| `ges_work_queue_full_count` | Handler enqueued REJECT_BUSY reply when work queue saturated |
| `ges_inbound_validation_fail_count` | Inbound payload dropped (5-item validation failed) |
| `ges_cleanup_deferred_count` | `LockReleaseAll` GES_RELEASE deferred to cleanup dirty-list |
| `ges_reply_deferred_count` | LMON reply deferred to reply dirty-list (reserved pool exhausted) |
| `ges_reply_dropped_count` | Reply dirty-list saturated;  oldest reply dropped (backend retry converges via timeout) |

Four cap counters (per spec-2.16 D1) surface entry-level saturation:

- `holders_full_count`
- `waiters_full_count`
- `converts_full_count`
- `ngranted_promoted_count`

spec-6.3a adds GRD entry-lifecycle counters in the `grd` category:

| Key | Meaning |
|---|---|
| `grd_entries_reclaimed_count` | Cold holderless entries removed from the GRD HTAB |
| `grd_reclaim_skipped_pinned_count` | Reclaim attempts skipped because a lookup pin was still held |
| `grd_pin_high_water` | Highest observed lookup-pin count on a single entry |
| `grd_sweep_runs` | LMON cold-reclaim sweep invocations |

Implementation details for the pin/release discipline, shard-local scan
model, and ERROR cleanup classification are in
`docs/cluster/grd-entry-lifecycle.md`.

## Wire Format

Payload bytes follow the 36-byte `ClusterICEnvelope`:

```
[ Envelope 36B ][ GesRequestPayload 48B  or  GesReplyPayload 48B ]
```

### `GesRequestPayload` (`msg_type=4`)

```
0..3    opcode           uint32 LE  (1=REQUEST, 2=CONVERT, 3=RELEASE)
4..7    lockmode         uint32 LE  (PG LOCKMODE 1..8)
8..31   holder_id        24 bytes   (node_id, procno, cluster_epoch, request_id)
32..47  resid            16 bytes   (ClusterResId byte image)
```

### `GesReplyPayload` (`msg_type=5`)

```
0..3    opcode           uint32 LE  (1=GRANT, 2=REJECT)
4..7    reject_reason    uint32 LE  (0=NONE/GRANT, 1=WORK_QUEUE_FULL,
                                     2=LOCK_CONFLICT, 3=EPOCH_MISMATCH,
                                     4=TIMEOUT)
8..31   holder_id        24 bytes   (echoes request)
32..47  resid            16 bytes   (echoes request)
```

## Forward Compatibility

- `cluster.ges_request_timeout_ms` range will extend to include `-1`
  (perpetual wait) once cross-node deadlock detection ships in
  spec-2.17.
- Holder identity 4-tuple `(node_id, cluster_epoch, procno, request_id)`
  is forward-compat with spec-2.17 BAST + 4-node Cluster.
- LMS worker pool migration (spec-2.18+) will route Phase 2 grant
  decisions through dedicated LMS workers without changing the
  `GesGrantWait` wait event surface.
