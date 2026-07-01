# Configuration reference

linkdb uses two configuration mechanisms layered on top of standard
PostgreSQL configuration:

1. **`postgresql.conf`** — standard PG config plus `cluster.*`
   GUCs added by linkdb's cluster subsystem.
2. **`pgrac.conf`** — INI-style file describing the cluster
   topology (the list of nodes that participate in the cluster).

## cluster.* GUCs

Most bootstrap and storage-routing GUCs require server restart to
change (PGC_POSTMASTER). Runtime maintenance knobs are marked with
their own context below.

### `cluster.node_id`

| | |
|---|---|
| Type | integer |
| Default | -1 (unconfigured) |
| Range | -1 .. 127 |
| Context | postmaster |
| Boot setting | `postgresql.conf` |

Numeric identifier of this node within the cluster.  Set per
node — every node in the same cluster has a distinct
`cluster.node_id`.  The default of -1 means "not part of a
cluster" and is the value used by single-node development /
test deployments.

```text
# postgresql.conf
cluster.node_id = 0
```

Verify:

```sql
SHOW "cluster.node_id";
-- 0
```

### `cluster.wal_threads_dir`

| | |
|---|---|
| Type | string |
| Default | `''` (flat `pg_wal` layout) |
| Context | postmaster |
| Boot setting | `postgresql.conf` (written by `pgrac-init --wal-threads-dir`) |

Shared-storage root of the per-thread WAL layout.  When set (must be
an absolute path), `$PGDATA/pg_wal` must resolve to
`<dir>/thread_<cluster.node_id + 1>`; startup is refused otherwise
(SQLSTATE `53RA0` / `53RA1`).  See
[Per-thread WAL layout](wal-threads.md).

```text
# postgresql.conf
cluster.wal_threads_dir = '/shared/walroot'
```

### Cluster backup / PITR GUCs

These settings support the cluster-aware physical backup / restore /
PITR surface.

| GUC | Type | Default | Context | Notes |
|---|---|---|---|---|
| `cluster.recovery_target_scn` | string | `''` | postmaster | Target SCN used by `pg_cluster_pitr_status`. Empty means latest unless a name or cluster-time target is set. |
| `cluster.recovery_target_cluster_time` | string | `''` | postmaster | Timestamp target reported by `pg_cluster_pitr_status`; current 6.5 recovery action remains fail-closed for this target type. |
| `cluster.recovery_target_name` | string | `''` | postmaster | Named restore-point target resolved by `pg_cluster_pitr_status`. |
| `cluster.recovery_target_action` | enum | `pause` | postmaster | Accepted values: `pause`, `promote`, `shutdown`; exposed in PITR status. |
| `cluster.enable_pitr_restore_points` | bool | `off` | sighup | Enables future automatic restore-point scheduling. Manual `pg_cluster_create_restore_point()` is independent. |
| `cluster.pitr_restore_point_interval_ms` | integer | `0` | sighup | Zero disables automatic scheduling. |
| `cluster.backup_wal_retention` | integer | `0` MB | sighup | Retention hint for the future backup-set writer. |
| `cluster.backup_parallel_channels` | integer | `1` | sighup | Reserved copy-channel capacity for the future backup-set writer. |
| `cluster.backup_manifest_checksums` | enum | `crc32c` | sighup | Manifest checksums are mandatory; unchecked manifests are not supported. |

The current implementation is intentionally conservative.  These GUCs
expose the 6.5 catalog and state surface, but mutating cluster physical
backup and restore-point entry points fail closed with
`feature_not_supported` until the physical capture, durable WAL pin,
commit-drain restore-point barrier, restore, and PITR replay paths are
implemented.  The server refuses to publish a manifest or restore point
when those proofs are absent.

### `cluster.interconnect_tier`

| | |
|---|---|
| Type | enum |
| Allowed values | `stub` (default), `tier1`, `tier2`, `tier3` |
| Context | postmaster |
| Boot setting | `postgresql.conf` |

Selects the cluster interconnect transport.

| Value | Behavior |
|---|---|
| `stub` (default) | Same-node operation is a no-op success.  Cross-node send raises `ERRCODE_FEATURE_NOT_SUPPORTED`.  No real wire traffic.  Suitable for single-node deployments and CI. |
| `tier1` | TCP transport for the LMON heartbeat path between cluster nodes.  Requires every peer (including self) to be listed in `pgrac.conf` with an `interconnect_addr`.  See `pg_cluster_ic_peers` for runtime peer state. |
| `tier2` | Currently not supported.  Setting this causes the postmaster to refuse to start with `ERRCODE_FEATURE_NOT_SUPPORTED`. |
| `tier3` | Same as `tier2`: not supported. |

```text
# postgresql.conf
cluster.interconnect_tier = stub
```

Verify:

```sql
SELECT name, vartype, context, setting
  FROM pg_settings
 WHERE name = 'cluster.interconnect_tier';
--          name           | vartype |  context   | setting
-- ------------------------+---------+------------+---------
--  cluster.interconnect_tier | enum    | postmaster | stub
```

### `cluster.interconnect_heartbeat_interval_ms`

| | |
|---|---|
| Type | integer (milliseconds) |
| Default | `1000` (1 s) |
| Range | `100` – `60000` |
| Context | postmaster |

How often the LMON aux process emits a heartbeat to each connected peer in `tier1` mode, and (because the LMON main loop uses one timer for both jobs) how often it retries a connection to a peer that is currently down.  Has no effect when `cluster.interconnect_tier = stub`.

### `cluster.interconnect_connect_timeout_ms`

| | |
|---|---|
| Type | integer (milliseconds) |
| Default | `5000` (5 s) |
| Range | `1000` – `60000` |
| Context | postmaster |

Reserved for future timeout enforcement on outbound `tier1` TCP `connect(2)` calls.  Currently informational; LMON re-attempts on each heartbeat tick.

### `cluster.interconnect_recv_timeout_ms`

| | |
|---|---|
| Type | integer (milliseconds) |
| Default | `30000` (30 s) |
| Range | `1000` – `600000` |
| Context | postmaster |

Reserved for future timeout enforcement on `tier1` peer `recv(2)`.  Currently informational; the connection is held open until the peer closes or the postmaster shuts down.

### `cluster.interconnect_payload_max_bytes`

| | |
|---|---|
| Type | integer (bytes) |
| Default | `67108864` (64 MB) |
| Range | `16777216` (16 MB) – `268435456` (256 MB) |
| Context | postmaster |

Upper bound on the payload size accepted by the chunked-send API.  A caller asking to send a larger payload is rejected outright with `ERRCODE_PROGRAM_LIMIT_EXCEEDED` rather than silently truncating.  Increase this when the workload expects larger cross-node messages; the hard cap is 256 MB.

### `cluster.grd_max_entries`

| | |
|---|---|
| Type | integer |
| Default | `0` |
| Range | `0` – `1048576` |
| Context | postmaster |

Capacity of the GRD resource-entry hash table. `0` keeps the entry
table disabled for skeleton-mode deployments. Values above zero enable
GES resource tracking; empty entries are eligible for lifecycle reclaim
once they have no holders, waiters, converts, reservations, or lookup
pins.

### `cluster.grd_entry_reclaim`

| | |
|---|---|
| Type | bool |
| Default | `on` |
| Context | sighup |

Enables safe cold reclaim of holderless GRD entries. The lookup/release
pin discipline remains active even when this is off; disabling reclaim
only prevents `HASH_REMOVE` so operators can preserve entries for
diagnostics during investigation.

### `cluster.grd_entry_reclaim_max_per_sweep`

| | |
|---|---|
| Type | integer |
| Default | `256` |
| Range | `0` – `65536` |
| Context | sighup |

Maximum number of cold entries LMON attempts to reclaim during one
sweep. `0` disables periodic sweeps, while last-pin release can still
reclaim a cold entry when `cluster.grd_entry_reclaim = on`.

### `cluster.interconnect_chunk_reassembly_timeout_ms`

| | |
|---|---|
| Type | integer (milliseconds) |
| Default | `10000` (10 s) |
| Range | `1000` – `60000` |
| Context | postmaster |

How long the receiver waits for the remaining chunks of a chunked payload before declaring the reassembly stuck and dropping the peer.  The peer is then reconnected on the next tick.

### `cluster.interconnect_tcp_keepidle_sec`

| | |
|---|---|
| Type | integer (seconds) |
| Default | `60` |
| Range | `30` – `600` |
| Context | postmaster |

Idle seconds before the kernel begins probing a quiet TCP connection (`TCP_KEEPIDLE` on Linux, `TCP_KEEPALIVE` on macOS).  The application-level heartbeat (~3 s deadline) is the primary liveness path; this kernel-level keepalive is a fallback for the rare case where the application stalls but the socket remains open.

### `cluster.interconnect_tcp_keepintvl_sec`

| | |
|---|---|
| Type | integer (seconds) |
| Default | `10` |
| Range | `10` – `60` |
| Context | postmaster |

Interval between successive kernel keepalive probes once probing has begun (`TCP_KEEPINTVL`).

### `cluster.interconnect_tcp_keepcnt`

| | |
|---|---|
| Type | integer |
| Default | `6` |
| Range | `3` – `20` |
| Context | postmaster |

Number of unacked keepalive probes before the kernel declares the connection dead (`TCP_KEEPCNT`).  With the defaults the kernel-level worst-case half-open detection is `60 + 6 × 10 = 120 s`, well within the application-level path.

### `cluster.shared_storage_backend`

| | |
|---|---|
| Type | enum |
| Allowed values | `stub` (default), `local`, `cluster_fs` |
| Context | postmaster |
| Boot setting | `postgresql.conf` |

Selects the backend that the cluster-aware storage manager
(`cluster_smgr`) dispatches into when `cluster.smgr_user_relations`
is enabled.

| Value | Behavior |
|---|---|
| `stub` (default) | The cluster_smgr vtable is wired but no I/O paths route through it.  Setting `cluster.smgr_user_relations = on` while this is `stub` causes the postmaster to refuse to start. |
| `local` | Routes cluster-aware relation I/O through the local filesystem under PGDATA.  Useful for single-host smoke testing and for the two-instance concurrent-open path that exercises every relation through the cluster_smgr stack. |
| `cluster_fs` | Routes cluster-aware relation I/O into a shared directory tree (`cluster.shared_data_dir`) that every cluster node mounts.  Relation files live at `<shared_data_dir>/base/<dboid>/<relfilenode>`, so all participating nodes read and write the same physical files.  Requires `cluster.shared_data_dir`. |

```text
# postgresql.conf
cluster.shared_storage_backend = cluster_fs
cluster.shared_data_dir = '/mnt/pgrac-shared/data'
```

### `cluster.shared_data_dir`

| | |
|---|---|
| Type | string |
| Default | `''` |
| Context | postmaster |
| Boot setting | `postgresql.conf` |

Absolute path of the shared data root used by the `cluster_fs`
backend.  Must be the same filesystem location (the same shared
mount) on every node of the cluster.  Required (non-empty) when
`cluster.shared_storage_backend = cluster_fs`; a relative path is
rejected at startup.

On first attach a node creates a control file
(`pgrac_shared.control`) at the root that records the storage
identity and the set of node ids that have attached.  Each node
adds itself to that participant set when it starts.

### `cluster.shared_storage_uuid`

| | |
|---|---|
| Type | string |
| Default | `''` (auto-generated) |
| Context | postmaster |
| Boot setting | `postgresql.conf` |

Optional expected identity of the shared data root.  When empty,
the first node to attach generates an identity and records it in
`pgrac_shared.control`.  When set, the node verifies that the
control file's identity matches before attaching, and refuses to
start otherwise — use this to guard against pointing a node at the
wrong (or a stale) shared directory.

> **Production warning.**  Sharing one data root between nodes
> requires that the same relation maps to the same file name on
> every node.  The current release does not yet coordinate catalog
> naming across nodes: identical file naming holds only when every
> node runs the identical DDL in the identical order on a freshly
> initialized cluster.  Cross-node DDL coordination is currently
> not supported; deviating DDL across nodes leaves the shared tree
> inconsistent.  Treat `cluster_fs` deployments as experimental.

### `cluster.smgr_user_relations`

| | |
|---|---|
| Type | bool |
| Default | `off` |
| Context | postmaster |
| Boot setting | `postgresql.conf` |

When `on`, permanent (non-temporary) user relations are routed
through `cluster_smgr` instead of PostgreSQL's stock `md.c`
storage manager.  Two cluster instances may have a backend each
open the same relation at the same time; per-instance
`SMgrRelation` state and per-instance file handles are tracked
independently.

This GUC is **experimental**.  Two-instance concurrent open is
supported, but cross-instance cache invalidation across the
cluster and `md.c`-equivalent fsync registration are not yet
activated.  Enabling it triggers a postmaster startup `WARNING`
and is unsuitable for production workloads — stale-cache behavior
across cluster peers and crash-recovery durability are not
guaranteed.

```text
# postgresql.conf
cluster.shared_storage_backend = local
cluster.smgr_user_relations = on
```

Verify the GUC and the live SMgrRelation count via
`pg_cluster_state`:

```sql
SHOW "cluster.smgr_user_relations";
SELECT key, value
  FROM cluster_dump_state()
 WHERE category = 'shared_fs'
   AND key IN ('smgr_user_relations', 'smgr_active_relations');
```

The cross-instance invalidation hook firing rate is observable
through `pg_stat_cluster_counters`:

```sql
SELECT name, value
  FROM pg_stat_cluster_counters
 WHERE name = 'cluster.smgr.remote_invalidation_stub_call_count';
```

The counter advances on every relation extend, truncate, or
unlink while `cluster.enabled = on` and the GUC is `on`; it stays
at zero in `cluster.enabled = off` mode regardless of GUC.

### `cluster.config_file`

| | |
|---|---|
| Type | string |
| Default | `pgrac.conf` (resolved relative to PGDATA) |
| Context | postmaster |
| Boot setting | `postgresql.conf` |

Path to the cluster topology file (`pgrac.conf` by default).
Use an absolute path to point at shared storage in multi-node
deployments.

```text
# postgresql.conf
cluster.config_file = '/srv/shared/pgrac.conf'
```

### `cluster.voting_disks`

> **EXPERIMENTAL.** The voting-disk + quorum-lite feature is not
> production-ready in this release.  The catalog surface (GUCs / views /
> wait events / counters / SQLSTATEs) is wired and visible, but the
> background coordinator process is not yet driven by postmaster
> startup; views report the fail-closed default state.  Use only for
> evaluation against the documented surface.

| | |
|---|---|
| Type | string (comma-separated path list) |
| Default | empty (disabled) |
| Context | postmaster |
| Boot setting | `postgresql.conf` |

Comma-separated list of voting-disk file paths.  Each disk is a
pre-allocated file on shared storage that holds one slot per cluster
instance; the cluster coordinator polls all disks every
`cluster.quorum_poll_interval_ms` and computes a majority view.

Empty disables the voting-disk path entirely.  In multi-node mode,
combining empty with `cluster.allow_single_node = off` triggers a
postmaster startup error to prevent silent fail-open.

```text
# postgresql.conf
cluster.voting_disks = '/srv/voting/disk1,/srv/voting/disk2,/srv/voting/disk3'
```

Three disks across distinct failure domains is the recommended
production configuration; one, five, and seven are also valid odd-only
sizes.  A startup heuristic emits a warning when the configured paths
share a common parent directory (likely co-located on one volume).

### `cluster.quorum_poll_interval_ms`

| | |
|---|---|
| Type | integer (milliseconds) |
| Default | `2000` (2 s) |
| Range | `500` – `30000` |
| Context | postmaster |

Coordinator main-loop tick interval.  The lease window backends use to
gate writes is `2 ×` this value: a coordinator stalled for more than
two ticks is treated as failed and the cluster fails closed without
waiting for an explicit signal.

### `cluster.voting_disk_io_timeout_ms`

| | |
|---|---|
| Type | integer (milliseconds) |
| Default | `5000` (5 s) |
| Range | `500` – `60000` |
| Context | postmaster |

Per-I/O deadline for voting-disk read / write / fsync.  An I/O that
exceeds this deadline counts as a disk failure and decrements the
healthy-disk count surfaced by `pg_cluster_quorum_state.disks_ok`.

### `cluster.voting_disk_size_bytes`

| | |
|---|---|
| Type | integer (bytes; multiple of 512) |
| Default | `262144` |
| Range | `4096` – `1048576` |
| Context | postmaster |

Size of each pre-allocated voting-disk file.  Each cluster instance
owns four 512-byte slots: voting, clean-leave marker, join-commit marker,
and ADG apply-master lease.  The default holds all four regions for 128
instances.

## ADG Physical Standby

The ADG standby path is opt-in.  Existing primary deployments keep their
current behavior with the defaults: `cluster.dg_role = primary` and
`cluster.enable_adg = off`.

To start ADG standby apply, configure the standby instance with
`cluster.dg_role = standby` and `cluster.enable_adg = on`.  With the
default `cluster.apply_master_election = on`, `cluster.voting_disks`
must name a reachable voting-disk majority because the MRP validates its
Apply Master term lease in the ADG lease region before replay is allowed.
For a single-node test standby only, set `cluster.apply_master_election =
off` to bypass durable election and force the local node to act as Apply
Master.

The standby starts an MRP auxiliary process during recovery states so WAL
replay is gated before read-only service opens.  If the Apply Master
lease cannot be validated, replay waits fail-closed and
`standby_consistent_scn` is cleared.  ADG thread-barrier WAL records
advance the read-consistent SCN only after every active WAL thread has
published a safe SCN.

`pg_stat_cluster_adg` and `pg_stat_gcluster_adg` expose the current MRP
state, Apply Master owner/term, receive/apply LSNs, lag bytes, and
`standby_consistent_scn`.

### ADG GUC Summary

| GUC | Default | Context | Purpose |
|---|---|---|---|
| `cluster.dg_role` | `primary` | postmaster | Selects primary or ADG standby behavior. |
| `cluster.dg_mode` | `async` | sighup | Shipping acknowledgement mode: `async`, `sync`, or `max_availability`. |
| `cluster.enable_adg` | `off` | postmaster | Starts ADG MRP/read-only standby paths when role is `standby`. |
| `cluster.apply_master_election` | `on` | sighup | Uses the durable voting-disk Apply Master lease. |
| `cluster.adg_lag_threshold_sec` | `10` | sighup | Read-only service lag threshold. |
| `cluster.apply_master_max_lag_ms` | `5000` | sighup | Lag threshold surfaced by ADG status views. |
| `cluster.max_standby_delay` | `30` | sighup | Maximum standby read delay before apply conflict handling. |
| `cluster.apply_master_switch_drain_ms` | `5000` | sighup | Drain window during Apply Master switch. |
| `cluster.adg_barrier_interval_ms` | `0` | sighup | Requested periodic thread-safe SCN barrier interval; `0` uses commit-driven barriers. |
| `cluster.wal_sender_timeout_sec` | `60` | sighup | Primary-side LNS shipping timeout. |
| `cluster.wal_receiver_timeout_sec` | `60` | sighup | Standby-side RFS receive timeout. |

### `cluster.self_fence_enabled`

> **EXPERIMENTAL.** Fence-lite path requires the voting-disk + quorum
> coordinator;treat together with the EXPERIMENTAL surface above.

| | |
|---|---|
| Type | bool |
| Default | `on` (production fail-safe) |
| Context | postmaster |

Whether the postmaster initiates an orderly fast-shutdown when the
cluster has lost quorum for longer than `cluster.self_fence_grace_ms`.
When `on` (default), the postmaster self-signals SIGINT after the
grace window, driving PG's standard fast-shutdown path; when `off`,
operators must stop the instance manually.

In-flight transaction abort on quorum loss is controlled separately
by `cluster.freeze_writes_enabled` — this GUC only affects the
postmaster shutdown decision, not the per-backend abort behaviour.

The dev / test escape is `cluster.allow_single_node = on`:  in single-
node mode the coordinator never enters the LOST quorum state, so
self-fence cannot trigger regardless of this GUC's value.

### `cluster.self_fence_grace_ms`

| | |
|---|---|
| Type | integer (milliseconds) |
| Default | `30000` (30 s) |
| Range | `1000` – `300000` |
| Context | postmaster |

Delay between the cluster's quorum-loss broadcast and the postmaster's
self-fence shutdown.  This delay applies **only** to the postmaster
shutdown decision — the in-flight transaction freeze broadcast happens
immediately when quorum is lost.

The default (30 s ≈ 7.5× the lease window) absorbs short network
flaps without triggering a self-shutdown.  Lower it for more
aggressive shutdown on flap;raise it to give operators more time to
intervene.

### `cluster.freeze_writes_enabled`

| | |
|---|---|
| Type | bool |
| Default | `on` (production fail-safe) |
| Context | postmaster |

Whether backends abort in-flight transactions on receipt of the cluster
quorum-loss broadcast (`PROCSIG_CLUSTER_FREEZE_WRITES`).  When `on`
(default), the next `CHECK_FOR_INTERRUPTS` after the signal raises
`ERRCODE_CLUSTER_QUORUM_LOST_BACKEND` (SQLSTATE `53R50`) and rolls
back the transaction.  When `off`, the signal is silently absorbed
and only the commit-boundary fail-closed gate (controlled by
`cluster.voting_disks` / quorum lease) prevents writes.

Off is for diagnosing fence-induced abort behaviour without losing
in-flight work — production should keep on.  This setting does **not**
bypass the commit-boundary gate either way.

### `cluster.fence_audit_log`

| | |
|---|---|
| Type | enum (`off` / `log` / `debug`) |
| Default | `log` |
| Context | postmaster |

Verbosity of fence-related entries in the postmaster log.

* `off` — silent;rely on `pg_cluster_fence_state` view + counters.
* `log` — one LOG line per broadcast (freeze / thaw / self-fence
  initiated).  Default;recommended for production.
* `debug` — adds DEBUG2 entries for each backend that received the
  freeze signal.  Verbose, dev / test only.

## Reconfig coordinator observability

### `pg_cluster_reconfig_state` view

Single-row view (always exactly one row when `cluster.enabled = on`;
zero rows when `cluster.enabled = off`) exposing the last reconfig
event applied locally.

| Column | Type | Meaning |
|---|---|---|
| `event_id` | `bigint` | Deduplication hash of `(dead_bitmap, cssd_dead_generation)`.  `0` = never applied (sentinel). |
| `coordinator_node_id` | `integer` | `min(survivor_set)` deterministic coordinator for the event.  `0` when never applied (sentinel — distinguish via `event_id`). |
| `old_epoch` | `bigint` | Membership epoch immediately before the bump. |
| `new_epoch` | `bigint` | Membership epoch after the bump.  Equals `old_epoch + 1` when this node was the coordinator;equals `old_epoch` when this node was a survivor observer (the new epoch arrives via IC envelope piggyback). |
| `dead_bitmap` | `text` | 16-byte bitmap of declared peers in `DEAD` state, formatted as `0x` + 32 hex digits.  Bit `i` set means `node_id = i` was DEAD at apply time. |
| `applied_at` | `timestamptz` | Server-local timestamp of the apply.  `NULL` when never applied. |
| `observer_role` | `text` | One of `coordinator` / `survivor` / `none`.  `none` only when `event_id = 0`. |
| `event_seq` | `bigint` | Per-process monotonic apply counter.  Increments on every published event. |
| `cssd_dead_generation` | `bigint` | Snapshot of the `cssd` peer-state transition counter at apply time.  Used to distinguish a rejoin-then-redeath from a single sustained outage. |

Example query:

```sql
SELECT * FROM pg_cluster_reconfig_state;
-- event_id            | 123456789012345678
-- coordinator_node_id | 0
-- old_epoch           | 5
-- new_epoch           | 6
-- dead_bitmap         | 0x00000000000000000000000000000002
-- applied_at          | 2026-05-11 12:34:56+00
-- observer_role       | coordinator
-- event_seq           | 1
-- cssd_dead_generation | 7
```

### Reconfig error code

| SQLSTATE | Name | Cause | Retry semantics |
|---|---|---|---|
| `53R60` | `ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS` | A writable transaction was aborted because cluster membership changed mid-flight.  Only fires on transactions that have already allocated a top-level transaction id (i.e. have performed at least one write). | **Immediate retry safe.**  The transaction was aborted before its commit record was written;the next attempt will run under the new membership epoch. |

Compare with the related codes:

| SQLSTATE | Trigger | Source spec |
|---|---|---|
| `53R40` `ERRCODE_CLUSTER_QUORUM_LOST` | Commit-boundary fail-closed gate fired before commit record was written. | Voting-disk quorum (spec-2.6). |
| `53R50` `ERRCODE_CLUSTER_QUORUM_LOST_BACKEND` | In-flight backend aborted by quorum-loss freeze broadcast. | Fence-lite (spec-2.28). |
| `53R60` `ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS` | In-flight writable backend aborted by reconfig coordinator. | Reconfig coordinator (spec-2.29). |

Clients should be prepared to retry on any of these — they share the
"transaction aborted, cluster control-plane changed" semantics.  In
all three cases the abort happens before the commit record is durable,
so client retry is safe.

## pgrac.conf format

INI-style: section headers in `[brackets]` and `key = value`
lines.  Comments start with `#` or `;`.

### Example

```ini
# pgrac.conf
[cluster]
name = pgrac-prod-01

[node.0]
interconnect_addr = 10.0.0.1:6432
hostname          = db-1.internal
public_addr       = 192.168.1.1:5432
role              = primary
region            = us-east-1a

[node.1]
interconnect_addr = 10.0.0.2:6432
hostname          = db-2.internal
role              = standby
```

### Section types

| Section | Purpose |
|---|---|
| `[cluster]` | Cluster-wide diagnostic metadata.  Currently only `name` is recognised; unknown keys produce a `WARNING` and are ignored. |
| `[node.<N>]` | Per-node entry.  `<N>` is a decimal integer in `[0, 127]` (matches the `cluster.node_id` range).  Each node id appears at most once. |

### Recognised keys inside `[node.<N>]`

| Key | Required | Format | Notes |
|---|---|---|---|
| `interconnect_addr` | yes | `host:port` | Where peers contact this node.  `host` is non-empty (no DNS validation at parse time); `port` in `[1, 65535]`. |
| `hostname` | no | string | Free-form short hostname; appears in `pg_cluster_nodes.hostname`. |
| `public_addr` | no | `host:port` | Client-facing address (for load balancer use). |
| `role` | no | `primary` / `standby` / `arbiter` | Defaults to `primary` if absent. |
| `region` | no | string | Free-form region tag for diagnostic display. |

### Comment forms

```ini
[node.0]
# A full-line comment.
interconnect_addr = 10.0.0.1:6432  ; trailing semicolon comment also works
hostname = db-1.internal           # or '#' style
```

Anything from the first `#` or `;` on a line is treated as comment.

### Validation rules at startup

The postmaster parses `pgrac.conf` and FATALs (refuses to start)
on any of:

- malformed section header / `key = value` line (errcontext includes
  the offending line number)
- duplicate `[node.<N>]` for the same `<N>`
- node id outside `[0, 127]`
- `[node.<N>]` missing the required `interconnect_addr` key
- `interconnect_addr` not in `host:port` form
- `role` not one of `primary` / `standby` / `arbiter`
- `cluster.node_id` (the GUC) is not present in any `[node.<N>]` section

If `pgrac.conf` is missing entirely the server falls back to a
single-node topology — see [Bootstrap](bootstrap.md) for details.

### Maximum cluster size

`pgrac.conf` may declare up to **128 nodes** (`node_id` range
`[0, 127]`).  Larger clusters are not currently supported.

### Reload semantics

`cluster.config_file` is a `PGC_POSTMASTER` GUC; topology changes
require a server restart.  There is no online "reload pgrac.conf"
path in this version.

## Reporting issues

File issues at <https://github.com/sqlrush/linkdb/issues>.
